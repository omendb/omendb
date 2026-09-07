//! PostgreSQL cancel-request routing into cooperative operation tokens.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use pgwire::api::ClientInfo;
use pgwire::api::cancel::CancelHandler;
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::startup::SecretKey;
use pgwire::messages::{DecodeContext, Message, ProtocolVersion};
use tokio::io::AsyncReadExt;

use super::CancellationToken;

pub(super) type CancelKey = (i32, Vec<u8>);

/// Owns the bridge between PostgreSQL cancel requests and one cooperative
/// operation token per authenticated connection. The server's cancel handler
/// routes protocol identities here; this registry owns both the identity and
/// the database operation's cancellation state.
pub(super) struct CancellationRegistry {
    entries: Mutex<HashMap<CancelKey, CancellationEntry>>,
}

struct CancellationEntry {
    address: std::net::SocketAddr,
    active: Option<Arc<WireQueryCancellation>>,
}

pub(super) struct WireQueryCancellation {
    token: CancellationToken,
}

pub(super) struct QueryCancellationLease {
    registry: Arc<CancellationRegistry>,
    key: CancelKey,
    operation: Arc<WireQueryCancellation>,
}

impl CancellationRegistry {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn register<C: ClientInfo>(&self, client: &C) {
        let (pid, secret_key) = client.pid_and_secret_key();
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(
                (pid, secret_key.to_bytes().to_vec()),
                CancellationEntry {
                    address: client.socket_addr(),
                    active: None,
                },
            );
        }
    }

    pub(super) fn begin<C: ClientInfo>(
        self: &Arc<Self>,
        client: &C,
    ) -> Option<(CancellationToken, QueryCancellationLease)> {
        let (pid, secret_key) = client.pid_and_secret_key();
        let key = (pid, secret_key.to_bytes().to_vec());
        let operation = Arc::new(WireQueryCancellation {
            token: CancellationToken::new(),
        });
        let mut entries = self.entries.lock().ok()?;
        let entry = entries.get_mut(&key)?;
        entry.active = Some(Arc::clone(&operation));
        Some((
            operation.token.clone(),
            QueryCancellationLease {
                registry: Arc::clone(self),
                key,
                operation,
            },
        ))
    }

    pub(super) fn cancel(&self, pid: i32, secret_key: &SecretKey) -> bool {
        let key = (pid, secret_key.to_bytes().to_vec());
        let Ok(entries) = self.entries.lock() else {
            return false;
        };
        let Some(entry) = entries.get(&key) else {
            return false;
        };
        if let Some(operation) = &entry.active {
            operation.token.cancel();
            true
        } else {
            false
        }
    }

    pub(super) fn finish(&self, key: &CancelKey, operation: &Arc<WireQueryCancellation>) {
        if let Ok(mut entries) = self.entries.lock()
            && let Some(entry) = entries.get_mut(key)
            && entry
                .active
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, operation))
        {
            entry.active = None;
        }
    }

    pub(crate) fn cleanup_connection(&self, address: std::net::SocketAddr) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|_, entry| {
                if entry.address == address {
                    if let Some(operation) = &entry.active {
                        operation.token.cancel();
                    }
                    false
                } else {
                    true
                }
            });
        }
    }

    pub(super) fn cancel_all(&self) {
        if let Ok(entries) = self.entries.lock() {
            for entry in entries.values() {
                if let Some(operation) = &entry.active {
                    operation.token.cancel();
                }
            }
        }
    }
}

impl Drop for QueryCancellationLease {
    fn drop(&mut self) {
        self.operation.token.cancel();
        self.registry.finish(&self.key, &self.operation);
    }
}

pub(super) struct WireCancelHandler {
    pub(super) cancellations: Arc<CancellationRegistry>,
}

#[async_trait]
impl CancelHandler for WireCancelHandler {
    async fn on_cancel_request(&self, request: CancelRequest) {
        self.cancellations.cancel(request.pid, &request.secret_key);
    }
}

/// Read only a cancel packet when ordinary session admission is exhausted.
/// pgwire issues 4-byte (3.0) or 32-byte (3.2) secrets; no arbitrary startup
/// payload or client-specified allocation is admitted through this path.
pub(super) async fn receive_capacity_cancel(
    socket: &mut tokio::net::TcpStream,
    registry: &CancellationRegistry,
) -> bool {
    let read = async {
        let mut packet = [0u8; 44];
        socket.read_exact(&mut packet[..8]).await.ok()?;
        let length = u32::from_be_bytes(packet[..4].try_into().ok()?) as usize;
        if !matches!(length, 16 | 44) || !CancelRequest::is_cancel_request_packet(&packet[..8]) {
            return None;
        }
        socket.read_exact(&mut packet[8..length]).await.ok()?;
        let mut packet = bytes::BytesMut::from(&packet[..length]);
        // Decode bytes unchanged for either key width; the registry keys
        // both protocol versions by their normalized byte representation.
        let context = DecodeContext::new(ProtocolVersion::PROTOCOL3_2);
        let request = CancelRequest::decode(&mut packet, &context).ok()??;
        registry.cancel(request.pid, &request.secret_key);
        Some(())
    };
    matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), read).await,
        Ok(Some(()))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn capacity_cancel_routes_both_protocol_secret_widths() {
        for secret in [SecretKey::I32(123), SecretKey::Bytes(vec![7; 32].into())] {
            let registry = CancellationRegistry::new();
            let operation = Arc::new(WireQueryCancellation {
                token: CancellationToken::new(),
            });
            registry.entries.lock().unwrap().insert(
                (42, secret.to_bytes().to_vec()),
                CancellationEntry {
                    address: "127.0.0.1:1".parse().unwrap(),
                    active: Some(Arc::clone(&operation)),
                },
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut packet = bytes::BytesMut::new();
            CancelRequest::new(42, secret).encode(&mut packet).unwrap();
            client.write_all(&packet).await.unwrap();
            assert!(receive_capacity_cancel(&mut socket, &registry).await);
            assert!(operation.token.is_cancelled());
        }
    }
}
