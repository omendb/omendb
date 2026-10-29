use std::collections::{BTreeMap, BTreeSet};

use super::{
    AuthorizationCatalog, Principal, PrincipalId, PrincipalRecord, TableGrant, TableIncarnation,
    corrupt, validate_name,
};
use crate::{DbError, Result, TableId};

const MAGIC: &[u8; 4] = b"OAC1";
const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CREDENTIAL_BYTES: usize = 65_536;

pub(crate) fn encode(catalog: &AuthorizationCatalog) -> Result<Vec<u8>> {
    let mut bytes = MAGIC.to_vec();
    bytes.push(u8::from(catalog.bootstrap));
    put_length(&mut bytes, catalog.principals.len())?;
    for (id, record) in &catalog.principals {
        bytes.extend_from_slice(&id.0);
        put_bytes(&mut bytes, record.principal.name.as_bytes())?;
        bytes.push(u8::from(record.administrator));
        bytes.push(u8::from(record.credential.is_some()));
        if let Some(credential) = &record.credential {
            if credential.len() > MAX_CREDENTIAL_BYTES {
                return Err(DbError::ValueTooLarge(credential.len()));
            }
            put_bytes(&mut bytes, credential)?;
        }
        put_length(&mut bytes, record.grants.len())?;
        for (table, grant) in &record.grants {
            bytes.extend_from_slice(&table.table.0.to_le_bytes());
            bytes.extend_from_slice(&table.tree.to_le_bytes());
            bytes.push(u8::from(grant.read) | (u8::from(grant.write) << 1));
        }
        if bytes.len() > MAX_BYTES {
            return Err(DbError::ValueTooLarge(bytes.len()));
        }
    }
    Ok(bytes)
}

fn put_length(bytes: &mut Vec<u8>, length: usize) -> Result<()> {
    bytes.extend_from_slice(
        &u32::try_from(length)
            .map_err(|_| DbError::ValueTooLarge(length))?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_length(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

pub(crate) fn decode(bytes: &[u8]) -> Result<AuthorizationCatalog> {
    if bytes.len() > MAX_BYTES {
        return Err(corrupt("catalog exceeds its byte limit"));
    }
    let mut input = Input(bytes);
    if input.take(4)? != MAGIC {
        return Err(corrupt("invalid format or version"));
    }
    let bootstrap = input.boolean()?;
    let count = input.length()?;
    // A record needs at least an identity, a nonempty name, flags and counts.
    if count > input.0.len() / 27 {
        return Err(corrupt("invalid principal count"));
    }
    let mut principals = BTreeMap::new();
    let mut names = BTreeSet::new();
    for _ in 0..count {
        let id = PrincipalId(input.take(16)?.try_into().expect("fixed-width identity"));
        if id.0 == [0; 16] {
            return Err(corrupt("zero principal identity"));
        }
        let name = std::str::from_utf8(input.bytes()?)
            .map_err(|_| corrupt("invalid principal name encoding"))?
            .to_owned();
        validate_name(&name).map_err(|_| corrupt("invalid principal name"))?;
        if !names.insert(name.clone()) {
            return Err(corrupt("duplicate principal name"));
        }
        let administrator = input.boolean()?;
        let credential = if input.boolean()? {
            let credential = input.bytes()?;
            if credential.len() > MAX_CREDENTIAL_BYTES {
                return Err(corrupt("credential exceeds its byte limit"));
            }
            Some(credential.to_vec())
        } else {
            None
        };
        let count = input.length()?;
        if count > input.0.len() / 17 {
            return Err(corrupt("invalid grant count"));
        }
        let mut grants = BTreeMap::new();
        for _ in 0..count {
            let object = TableIncarnation {
                table: TableId(input.u64()?),
                tree: input.u64()?,
            };
            let rights = input.take(1)?[0];
            if !(1..=3).contains(&rights) || object.tree == 0 {
                return Err(corrupt("invalid table grant"));
            }
            let grant = TableGrant {
                read: rights & 1 != 0,
                write: rights & 2 != 0,
            };
            if grants.insert(object, grant).is_some() {
                return Err(corrupt("duplicate table grant"));
            }
        }
        let record = PrincipalRecord {
            principal: Principal { id, name },
            administrator,
            credential,
            grants,
        };
        if principals.insert(id, record).is_some() {
            return Err(corrupt("duplicate principal identity"));
        }
    }
    if !input.0.is_empty() || (bootstrap && !principals.is_empty()) {
        return Err(corrupt("trailing bytes or invalid bootstrap state"));
    }
    Ok(AuthorizationCatalog {
        bootstrap,
        principals,
    })
}

struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        if count > self.0.len() {
            return Err(corrupt("truncated catalog"));
        }
        let (value, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(value)
    }
    fn boolean(&mut self) -> Result<bool> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt("invalid boolean")),
        }
    }
    fn length(&mut self) -> Result<usize> {
        let length = u32::from_le_bytes(self.take(4)?.try_into().expect("fixed-width length"));
        usize::try_from(length).map_err(|_| corrupt("length exceeds address space"))
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.length()?;
        self.take(length)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("fixed-width integer"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_codec_rejects_truncation_duplicates_and_invalid_flags() {
        let mut catalog = AuthorizationCatalog::bootstrap();
        let id = catalog.create_principal("alice").unwrap();
        let record = catalog.record_mut(id).unwrap();
        record.administrator = true;
        record.credential = Some(vec![42; 64]);
        record.grants.insert(
            TableIncarnation {
                table: TableId(8),
                tree: 9,
            },
            TableGrant {
                read: false,
                write: true,
            },
        );
        let encoded = encode(&catalog).unwrap();
        let decoded = decode(&encoded).unwrap();
        let alice = decoded.find("alice").unwrap();
        assert_eq!(alice.principal.id, id);
        assert!(alice.administrator);
        assert_eq!(alice.credential.as_deref(), Some(&[42; 64][..]));
        assert!(alice.grants.values().next().unwrap().write);
        for end in 0..encoded.len() {
            assert!(
                decode(&encoded[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
        let mut invalid = encoded.clone();
        invalid[4] = 2;
        assert!(decode(&invalid).is_err());
        invalid[4] = 1;
        assert!(decode(&invalid).is_err());
        let mut duplicate = encoded.clone();
        duplicate[5..9].copy_from_slice(&2_u32.to_le_bytes());
        duplicate.extend_from_slice(&encoded[9..]);
        assert!(decode(&duplicate).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }
}
