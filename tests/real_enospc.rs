//! Real-filesystem capacity evidence.
//!
//! The test is ignored in normal local runs because it requires a dedicated
//! size-limited Linux filesystem. CI mounts that filesystem and runs it
//! explicitly so the result cannot be confused with the deterministic fault
//! seam.

#[cfg(target_os = "linux")]
mod linux {
    use seerdb::{DB, Error, Options};
    use std::cmp::min;
    use std::env;
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Write};
    use std::path::PathBuf;

    fn fill_until_nearly_full(root: &PathBuf) -> io::Result<File> {
        let filler_path = root.join("seerdb-enospc.filler");
        let mut filler = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(filler_path)?;
        let chunk = vec![0u8; 64 * 1024];
        loop {
            let available = fs2::available_space(root)?;
            if available <= 1024 {
                break;
            }
            let length = min(chunk.len(), (available - 1024) as usize);
            if length == 0 {
                break;
            }
            match filler.write_all(&chunk[..length]) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::StorageFull => break,
                Err(error) => return Err(error),
            }
        }
        filler.sync_data()?;
        Ok(filler)
    }

    #[test]
    #[ignore = "requires SEERDB_ENOSPC_ROOT on a dedicated size-limited Linux filesystem"]
    fn real_filesystem_enospc_preflight_is_retryable() {
        let root = PathBuf::from(
            env::var_os("SEERDB_ENOSPC_ROOT")
                .expect("CI must provide SEERDB_ENOSPC_ROOT for this ignored test"),
        );
        let path = root.join("db");
        fs::create_dir_all(&path).unwrap();

        let mut options = Options::for_test();
        options.max_wal_bytes = 1024 * 1024;
        let mut db = DB::open(&path, options).unwrap();
        db.put(b"base", b"value-1").unwrap();
        db.flush().unwrap();

        // Journal the pending mutation first. Its fixed WAL extent is already
        // admitted, so the filler targets the later data-generation reserve
        // rather than making the test accidentally exercise WAL admission.
        db.put(b"pending", b"value-2").unwrap();
        let filler = fill_until_nearly_full(&root).unwrap();

        assert!(matches!(db.flush(), Err(Error::CapacityPreflight)));
        assert!(!db.durability_status().write_fenced);
        drop(filler);
        fs::remove_file(root.join("seerdb-enospc.filler")).unwrap();

        db.flush().unwrap();
        assert_eq!(db.get(b"pending").unwrap(), Some(b"value-2".to_vec()));
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(b"value-1".to_vec()));
        assert_eq!(reopened.get(b"pending").unwrap(), Some(b"value-2".to_vec()));
    }
}
