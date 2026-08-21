//! Real-filesystem capacity evidence.
//!
//! The test is ignored in normal local runs because it requires a dedicated
//! size-limited Linux filesystem. CI mounts that filesystem and runs it
//! explicitly so the result cannot be confused with the deterministic fault
//! seam.

#![allow(clippy::disallowed_methods)]

#[cfg(target_os = "linux")]
mod linux {
    use seerdb::{BlobStorageMode, DB, Error, Options};
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

    fn release_filler(root: &PathBuf, filler: File) -> io::Result<()> {
        drop(filler);
        fs::remove_file(root.join("seerdb-enospc.filler"))?;
        // XFS can defer free-space accounting for an unlink until the
        // directory inode is synchronized; the retry must observe released
        // capacity on every supported filesystem.
        File::open(root)?.sync_all()
    }

    #[test]
    #[ignore = "requires SEERDB_ENOSPC_ROOT on a dedicated size-limited Linux filesystem"]
    fn real_filesystem_enospc_preflight_is_retryable() {
        let root = PathBuf::from(
            env::var_os("SEERDB_ENOSPC_ROOT")
                .expect("CI must provide SEERDB_ENOSPC_ROOT for this ignored test"),
        );
        let path = root.join("db");
        // A previous run leaves its databases behind; the gate must be
        // re-runnable against the same mount without manual cleanup.
        let _ = fs::remove_dir_all(&path);
        let _ = fs::remove_dir_all(root.join("segmented-db"));

        let mut options = Options::for_test();
        options.max_wal_bytes = 1024 * 1024;
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", b"value-1").unwrap();
        db.flush().unwrap();

        // Journal the pending mutation first. Its fixed WAL extent is already
        // admitted, so the filler targets the later data-generation reserve
        // rather than making the test accidentally exercise WAL admission.
        db.put(b"pending", b"value-2").unwrap();
        let filler = fill_until_nearly_full(&root).unwrap();

        let before_flush = db.metrics().unwrap().storage;
        let flush_result = db.flush();
        assert!(
            matches!(&flush_result, Err(Error::CapacityPreflight)),
            "expected retryable capacity preflight, got {flush_result:?}; metrics={:?}",
            db.metrics()
        );
        let after_flush = db.metrics().unwrap().storage;
        assert_eq!(
            after_flush.physical_page_writes, before_flush.physical_page_writes,
            "capacity preflight must not issue page writes"
        );
        assert!(!db.durability_status().write_fenced);
        release_filler(&root, filler).unwrap();

        db.flush().unwrap();
        assert_eq!(db.get(b"pending").unwrap(), Some(b"value-2".to_vec()));

        db.delete(b"pending").unwrap();
        db.flush().unwrap();
        let filler = fill_until_nearly_full(&root).unwrap();
        let before_vacuum = db.metrics().unwrap().storage;
        let vacuum_result = db.vacuum();
        assert!(
            matches!(&vacuum_result, Err(Error::CapacityPreflight)),
            "expected maintenance capacity preflight, got {vacuum_result:?}; metrics={:?}",
            db.metrics()
        );
        let after_vacuum = db.metrics().unwrap().storage;
        assert_eq!(
            after_vacuum.physical_page_writes, before_vacuum.physical_page_writes,
            "maintenance capacity preflight must not issue page writes"
        );
        assert!(!db.durability_status().write_fenced);
        release_filler(&root, filler).unwrap();

        db.vacuum().unwrap();
        assert_eq!(db.get(b"pending").unwrap(), None);

        let large = vec![0xAB; 2_000];
        for key in [b"gc-live".as_slice(), b"gc-dead-1", b"gc-dead-2"] {
            db.put(key, &large).unwrap();
        }
        db.flush().unwrap();
        db.delete(b"gc-dead-1").unwrap();
        db.delete(b"gc-dead-2").unwrap();
        db.flush().unwrap();
        let filler = fill_until_nearly_full(&root).unwrap();
        let before_gc = db.metrics().unwrap().storage;
        let gc_result = db.gc();
        assert!(
            matches!(&gc_result, Err(Error::CapacityPreflight)),
            "expected maintenance capacity preflight for mixed blob GC, got {gc_result:?}; metrics={:?}",
            db.metrics()
        );
        let after_gc = db.metrics().unwrap().storage;
        assert_eq!(
            after_gc.physical_page_writes, before_gc.physical_page_writes,
            "mixed blob GC capacity preflight must not issue page writes"
        );
        assert!(!db.durability_status().write_fenced);
        release_filler(&root, filler).unwrap();

        assert!(db.gc().unwrap() > 0);
        assert_eq!(db.get(b"gc-live").unwrap(), Some(large.clone()));
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(b"value-1".to_vec()));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        assert_eq!(reopened.get(b"gc-live").unwrap(), Some(vec![0xAB; 2_000]));

        let segmented_path = root.join("segmented-db");
        let segmented_options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            max_wal_bytes: 1024 * 1024,
            ..Options::for_test()
        };
        let mut segmented = DB::create(&segmented_path, segmented_options).unwrap();
        segmented.put(b"base", &large).unwrap();
        segmented.flush().unwrap();
        segmented.put(b"pending", &large).unwrap();
        let filler = fill_until_nearly_full(&root).unwrap();
        let before_segmented_flush = segmented.metrics().unwrap().storage;
        let segmented_flush = segmented.flush();
        assert!(
            matches!(segmented_flush, Err(Error::CapacityPreflight)),
            "expected segmented append capacity preflight, got {segmented_flush:?}; metrics={:?}",
            segmented.metrics()
        );
        let after_segmented_flush = segmented.metrics().unwrap().storage;
        assert_eq!(
            after_segmented_flush.physical_page_writes, before_segmented_flush.physical_page_writes,
            "segmented append refusal must not issue page writes"
        );
        assert!(!segmented.durability_status().write_fenced);
        release_filler(&root, filler).unwrap();

        segmented.flush().unwrap();
        assert_eq!(segmented.get(b"pending").unwrap(), Some(large.clone()));
        drop(segmented);
        let segmented_reopened = DB::open(&segmented_path, Options::for_test()).unwrap();
        assert_eq!(segmented_reopened.get(b"pending").unwrap(), Some(large));
    }
}
