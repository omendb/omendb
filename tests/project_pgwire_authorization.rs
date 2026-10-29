//! Authorization and startup-failure checks at the real wire boundary.
#![cfg(feature = "pgwire")]

use std::io;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use omendb::{RelationalBackendConfig, RelationalDatabase, pgwire_server};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn read_frame(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let tag = stream.read_u8().await?;
    let length = stream.read_u32().await?;
    assert!(
        (4..=16_384).contains(&length),
        "unexpected server frame size"
    );
    let mut payload = vec![0; usize::try_from(length - 4).unwrap()];
    stream.read_exact(&mut payload).await?;
    Ok((tag, payload))
}

async fn write_frame(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> io::Result<()> {
    stream.write_u8(tag).await?;
    stream
        .write_u32(u32::try_from(payload.len() + 4).unwrap())
        .await?;
    stream.write_all(payload).await
}

fn error_field(error: &[u8], field: &[u8]) -> bool {
    error.split(|byte| *byte == 0).any(|value| value == field)
}

async fn start_scram(address: std::net::SocketAddr, user: &str, first: &[u8]) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut startup = 196_608_u32.to_be_bytes().to_vec();
    startup.extend_from_slice(format!("user\0{user}\0database\0omendb\0\0").as_bytes());
    stream
        .write_u32(u32::try_from(startup.len() + 4).unwrap())
        .await
        .unwrap();
    stream.write_all(&startup).await.unwrap();
    let (tag, mechanisms) = read_frame(&mut stream).await.unwrap();
    assert_eq!(tag, b'R');
    assert_eq!(&mechanisms[..4], &10_u32.to_be_bytes());
    let mut initial = b"SCRAM-SHA-256\0".to_vec();
    initial.extend_from_slice(&u32::try_from(first.len()).unwrap().to_be_bytes());
    initial.extend_from_slice(first);
    write_frame(&mut stream, b'p', &initial).await.unwrap();
    stream
}

async fn until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), read_frame(stream))
            .await
            .unwrap()
            .unwrap();
        let ready = frame.0 == b'Z';
        frames.push(frame);
        if ready {
            return frames;
        }
    }
}

async fn until_closed_without_rows(stream: &mut TcpStream) -> Vec<Vec<u8>> {
    let mut errors = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), read_frame(stream)).await {
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                ) =>
            {
                return errors;
            }
            Ok(Ok((tag, payload))) => {
                assert_ne!(tag, b'D', "unauthenticated peer received protected rows");
                if tag == b'E' {
                    errors.push(payload);
                }
            }
            outcome => panic!("failed startup did not close the connection: {outcome:?}"),
        }
    }
}

fn protected_database(directory: &std::path::Path) -> RelationalDatabase {
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.join("db"))).unwrap();
    database
        .execute_sql("CREATE TABLE protected (id BIGINT PRIMARY KEY)")
        .unwrap();
    database
        .execute_sql("INSERT INTO protected VALUES (7)")
        .unwrap();
    database
}

#[tokio::test]
async fn failed_startup_cannot_become_a_trusted_query_connection() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = protected_database(directory.path());
    pgwire_server::provision_wire_user(&mut database, "known", "password").unwrap();
    let database = Arc::new(RwLock::new(database));
    let (address, _server) = pgwire_server::spawn(database, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut stream = start_scram(address, "missing", b"n,,n=missing,r=clientnonce0123456789").await;
    let (tag, error) = read_frame(&mut stream).await.unwrap();
    assert_eq!(tag, b'E');
    assert!(error_field(&error, b"C28P01"));
    // A peer need not disconnect when authentication fails. Query must never
    // turn the missing identity into trust authority.
    let _ = write_frame(&mut stream, b'Q', b"SELECT id FROM protected\0").await;
    until_closed_without_rows(&mut stream).await;
    assert!(error_field(&error, b"SFATAL"));
}

#[tokio::test]
async fn scram_proof_cannot_authenticate_a_recreated_principal() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = protected_database(directory.path());
    pgwire_server::provision_wire_user(&mut database, "alice", "old-password").unwrap();
    let database = Arc::new(RwLock::new(database));
    let (address, _server) = pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut scram = ScramSha256::new(b"old-password", ChannelBinding::unsupported());
    let mut stream = start_scram(address, "alice", scram.message()).await;
    let (tag, continuation) = read_frame(&mut stream).await.unwrap();
    assert_eq!(tag, b'R');
    assert_eq!(&continuation[..4], &11_u32.to_be_bytes());
    scram.update(&continuation[4..]).unwrap();
    {
        let mut db = database.write().unwrap();
        let old = db.principal("alice").unwrap().unwrap().id;
        db.transaction(|db, tx| {
            tx.drop_principal(db, old)?;
            let new = tx.create_principal(db, "alice")?;
            tx.set_database_administrator(db, new, true)
        })
        .unwrap();
        pgwire_server::provision_wire_user(&mut db, "alice", "new-password").unwrap();
    }
    // The original credential still proves against SCRAM's cached password,
    // but it cannot acquire the replacement principal's administrator rights.
    write_frame(&mut stream, b'p', scram.message())
        .await
        .unwrap();
    let _ = write_frame(&mut stream, b'Q', b"SELECT id FROM protected\0").await;
    let errors = until_closed_without_rows(&mut stream).await;
    assert!(errors.iter().any(|error| error_field(error, b"SFATAL")));
}

#[tokio::test]
async fn retained_describe_checks_revocation_and_aborts_staged_writes() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = protected_database(directory.path());
    pgwire_server::provision_wire_user(&mut database, "reader", "password").unwrap();
    pgwire_server::provision_wire_grant(&mut database, "reader", "protected", true, true).unwrap();
    let database = Arc::new(RwLock::new(database));
    let (address, _server) = pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut scram = ScramSha256::new(b"password", ChannelBinding::unsupported());
    let mut stream = start_scram(address, "reader", scram.message()).await;
    let (_, continuation) = read_frame(&mut stream).await.unwrap();
    assert_eq!(&continuation[..4], &11_u32.to_be_bytes());
    scram.update(&continuation[4..]).unwrap();
    write_frame(&mut stream, b'p', scram.message())
        .await
        .unwrap();
    let startup = until_ready(&mut stream).await;
    assert!(!startup.iter().any(|(tag, _)| *tag == b'E'));
    let final_message = startup
        .iter()
        .find(|(tag, payload)| *tag == b'R' && payload.starts_with(&12_u32.to_be_bytes()))
        .unwrap();
    scram.finish(&final_message.1[4..]).unwrap();

    let mut parse = b"retained\0SELECT id FROM protected\0".to_vec();
    parse.extend_from_slice(&0_u16.to_be_bytes());
    write_frame(&mut stream, b'P', &parse).await.unwrap();
    write_frame(&mut stream, b'S', &[]).await.unwrap();
    let prepared = until_ready(&mut stream).await;
    assert!(prepared.iter().any(|(tag, _)| *tag == b'1'));
    assert!(!prepared.iter().any(|(tag, _)| *tag == b'E'));
    write_frame(
        &mut stream,
        b'Q',
        b"BEGIN; INSERT INTO protected VALUES (8)\0",
    )
    .await
    .unwrap();
    assert!(
        !until_ready(&mut stream)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'E')
    );
    pgwire_server::provision_wire_grant(
        &mut database.write().unwrap(),
        "reader",
        "protected",
        false,
        false,
    )
    .unwrap();

    // This is Describe itself, not another Parse that might fail earlier.
    write_frame(&mut stream, b'D', b"Sretained\0")
        .await
        .unwrap();
    write_frame(&mut stream, b'S', &[]).await.unwrap();
    let described = until_ready(&mut stream).await;
    assert!(
        described
            .iter()
            .any(|(tag, error)| *tag == b'E' && error_field(error, b"C42501"))
    );
    assert!(!described.iter().any(|(tag, _)| *tag == b'T'));
    assert_eq!(described.last().unwrap().1, b"E");
    write_frame(&mut stream, b'Q', b"SELECT 1\0").await.unwrap();
    assert!(
        until_ready(&mut stream)
            .await
            .iter()
            .any(|(tag, error)| *tag == b'E' && error_field(error, b"C25P02"))
    );
    write_frame(&mut stream, b'Q', b"ROLLBACK\0").await.unwrap();
    assert_eq!(until_ready(&mut stream).await.last().unwrap().1, b"I");
    assert_eq!(
        database
            .write()
            .unwrap()
            .execute_sql("SELECT id FROM protected")
            .unwrap()
            .rows
            .len(),
        1
    );
}
