//! Power Failure Tests for seerdb
//!
//! Tests crash consistency under simulated power failures using dm-flakey.
//! These tests verify that seerdb correctly recovers after unexpected crashes.
//!
//! # Requirements
//!
//! - Linux with device mapper support
//! - Root/sudo access for dm-flakey setup
//! - Run with: `sudo -E cargo test --test power_failure_tests`
//!
//! # How dm-flakey works
//!
//! dm-flakey creates a virtual block device that can:
//! 1. Drop writes (simulating power loss during write)
//! 2. Corrupt specific bytes (simulating partial writes)
//! 3. Return errors for a period (simulating device failure)
//!
//! Test flow:
//! 1. Create loopback device from file
//! 2. Create dm-flakey device on top
//! 3. Write data to seerdb on dm-flakey device
//! 4. Trigger "crash" by switching dm-flakey to drop_writes mode
//! 5. Destroy dm-flakey, recreate in normal mode
//! 6. Verify seerdb recovers correctly

#![cfg(target_os = "linux")]

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Check if we have root privileges
fn is_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

/// Size of the test loopback device (64MB)
const LOOP_SIZE_MB: usize = 64;

/// Helper to run shell commands
fn run_cmd(cmd: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| format!("Failed to run {}: {}", cmd, e))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(format!(
            "{} failed: {}",
            cmd,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// dm-flakey test harness
struct DmFlakeyHarness {
    /// Path to the backing file
    backing_file: PathBuf,
    /// Loop device (e.g., /dev/loop0)
    loop_device: Option<String>,
    /// dm-flakey device name
    dm_name: String,
    /// Mount point for the filesystem
    mount_point: PathBuf,
    /// Whether the device is in "crash" mode
    in_crash_mode: bool,
}

impl DmFlakeyHarness {
    /// Create a new dm-flakey test harness
    fn new(test_name: &str) -> Result<Self, String> {
        if !is_root() {
            return Err("Root privileges required for dm-flakey tests".to_string());
        }

        let base_dir = PathBuf::from("/tmp/seerdb_power_test");
        fs::create_dir_all(&base_dir).map_err(|e| format!("Failed to create test dir: {}", e))?;

        let backing_file = base_dir.join(format!("{}.img", test_name));
        let mount_point = base_dir.join(format!("{}_mount", test_name));
        let dm_name = format!("seerdb_test_{}", test_name);

        Ok(Self {
            backing_file,
            loop_device: None,
            dm_name,
            mount_point,
            in_crash_mode: false,
        })
    }

    /// Set up the loopback device and dm-flakey
    fn setup(&mut self) -> Result<(), String> {
        // Create backing file
        let file = File::create(&self.backing_file)
            .map_err(|e| format!("Failed to create backing file: {}", e))?;
        file.set_len((LOOP_SIZE_MB * 1024 * 1024) as u64)
            .map_err(|e| format!("Failed to set file size: {}", e))?;

        // Set up loop device
        let output = run_cmd(
            "losetup",
            &["-f", "--show", self.backing_file.to_str().unwrap()],
        )?;
        let loop_dev = output.trim().to_string();
        self.loop_device = Some(loop_dev.clone());

        // Get device size in sectors
        let size_output = run_cmd("blockdev", &["--getsz", &loop_dev])?;
        let sectors: u64 = size_output
            .trim()
            .parse()
            .map_err(|e| format!("Failed to parse sector count: {}", e))?;

        // Create dm-flakey device (starts in normal mode)
        // Table: "0 <sectors> flakey <dev> 0 <up_interval> <down_interval>"
        // up_interval=3600 (1 hour), down_interval=0 (no failures initially)
        let table = format!("0 {} flakey {} 0 3600 0", sectors, loop_dev);
        run_cmd("dmsetup", &["create", &self.dm_name, "--table", &table])?;

        // Format with ext4 (simple filesystem for testing)
        let dm_path = format!("/dev/mapper/{}", self.dm_name);
        run_cmd("mkfs.ext4", &["-q", &dm_path])?;

        // Create and mount
        fs::create_dir_all(&self.mount_point)
            .map_err(|e| format!("Failed to create mount point: {}", e))?;
        run_cmd("mount", &[&dm_path, self.mount_point.to_str().unwrap()])?;

        Ok(())
    }

    /// Get the path where seerdb should store data
    fn data_path(&self) -> PathBuf {
        self.mount_point.join("seerdb_data")
    }

    /// Simulate a crash by switching dm-flakey to drop_writes mode
    fn simulate_crash(&mut self) -> Result<(), String> {
        if self.in_crash_mode {
            return Ok(());
        }

        // Unmount first (simulates dirty unmount)
        let _ = run_cmd("umount", &["-l", self.mount_point.to_str().unwrap()]);

        // Reload dm-flakey with drop_writes
        let loop_dev = self.loop_device.as_ref().ok_or("No loop device")?;
        let size_output = run_cmd("blockdev", &["--getsz", loop_dev])?;
        let sectors: u64 = size_output
            .trim()
            .parse()
            .map_err(|e| format!("Failed to parse sector count: {}", e))?;

        // Switch to drop_writes mode
        let table = format!("0 {} flakey {} 0 0 3600 1 drop_writes", sectors, loop_dev);
        run_cmd("dmsetup", &["reload", &self.dm_name, "--table", &table])?;
        run_cmd("dmsetup", &["suspend", &self.dm_name])?;
        run_cmd("dmsetup", &["resume", &self.dm_name])?;

        self.in_crash_mode = true;
        Ok(())
    }

    /// Recover from crash (switch back to normal mode and remount)
    fn recover(&mut self) -> Result<(), String> {
        if !self.in_crash_mode {
            return Ok(());
        }

        // Reload dm-flakey in normal mode
        let loop_dev = self.loop_device.as_ref().ok_or("No loop device")?;
        let size_output = run_cmd("blockdev", &["--getsz", loop_dev])?;
        let sectors: u64 = size_output
            .trim()
            .parse()
            .map_err(|e| format!("Failed to parse sector count: {}", e))?;

        let table = format!("0 {} flakey {} 0 3600 0", sectors, loop_dev);
        run_cmd("dmsetup", &["reload", &self.dm_name, "--table", &table])?;
        run_cmd("dmsetup", &["suspend", &self.dm_name])?;
        run_cmd("dmsetup", &["resume", &self.dm_name])?;

        // Remount
        let dm_path = format!("/dev/mapper/{}", self.dm_name);
        run_cmd("mount", &[&dm_path, self.mount_point.to_str().unwrap()])?;

        self.in_crash_mode = false;
        Ok(())
    }

    /// Clean up all resources
    fn cleanup(&mut self) {
        // Best-effort cleanup
        let _ = run_cmd("umount", &["-l", self.mount_point.to_str().unwrap()]);
        let _ = run_cmd("dmsetup", &["remove", &self.dm_name]);
        if let Some(ref loop_dev) = self.loop_device {
            let _ = run_cmd("losetup", &["-d", loop_dev]);
        }
        let _ = fs::remove_file(&self.backing_file);
        let _ = fs::remove_dir(&self.mount_point);
    }
}

impl Drop for DmFlakeyHarness {
    fn drop(&mut self) {
        self.cleanup();
    }
}

// =============================================================================
// Tests (only run on Linux with root)
// =============================================================================

#[test]
#[ignore] // Run manually with: sudo -E cargo test --test power_failure_tests -- --ignored
fn test_crash_during_put() {
    if !is_root() {
        eprintln!("Skipping: requires root privileges");
        return;
    }

    let mut harness = DmFlakeyHarness::new("crash_put").expect("Failed to create harness");
    harness.setup().expect("Failed to setup harness");

    // Phase 1: Write some data
    {
        use seerdb::{DBOptions, DB};
        let opts = DBOptions {
            data_dir: harness.data_path(),
            ..Default::default()
        };
        let db = DB::open(opts).expect("Failed to open DB");

        // Write committed data
        for i in 0..100 {
            db.put(format!("committed_{:04}", i).as_bytes(), b"value")
                .expect("Put failed");
        }
        db.flush().expect("Flush failed");

        // Write uncommitted data (will be lost)
        for i in 0..50 {
            db.put(format!("uncommitted_{:04}", i).as_bytes(), b"value")
                .expect("Put failed");
        }

        // Simulate crash BEFORE flush
        harness.simulate_crash().expect("Failed to simulate crash");
    }

    // Phase 2: Recover and verify
    harness.recover().expect("Failed to recover");
    {
        use seerdb::{DBOptions, DB};
        let opts = DBOptions {
            data_dir: harness.data_path(),
            ..Default::default()
        };
        let db = DB::open(opts).expect("Failed to reopen DB after crash");

        // Committed data should be present
        for i in 0..100 {
            let key = format!("committed_{:04}", i);
            let value = db.get(key.as_bytes()).expect("Get failed");
            assert!(value.is_some(), "Committed key {} missing after crash", key);
        }

        // Uncommitted data may or may not be present (depends on WAL state)
        // The key invariant is: no corruption, consistent state
        println!("✓ Crash during put: recovery successful");
    }
}

#[test]
#[ignore]
fn test_crash_during_flush() {
    if !is_root() {
        eprintln!("Skipping: requires root privileges");
        return;
    }

    let mut harness = DmFlakeyHarness::new("crash_flush").expect("Failed to create harness");
    harness.setup().expect("Failed to setup harness");

    // Phase 1: Start a flush operation
    {
        use seerdb::{DBOptions, DB};
        let opts = DBOptions {
            data_dir: harness.data_path(),
            ..Default::default()
        };
        let db = DB::open(opts).expect("Failed to open DB");

        // Write data
        for i in 0..200 {
            db.put(format!("key_{:04}", i).as_bytes(), &vec![b'v'; 100])
                .expect("Put failed");
        }

        // Crash during flush (simulated by crashing right after flush call)
        // In a real test, we'd use failpoints to crash mid-flush
        db.flush().expect("Flush failed");
        harness.simulate_crash().expect("Failed to simulate crash");
    }

    // Phase 2: Recover and verify
    harness.recover().expect("Failed to recover");
    {
        use seerdb::{DBOptions, DB};
        let opts = DBOptions {
            data_dir: harness.data_path(),
            ..Default::default()
        };
        let db = DB::open(opts).expect("Failed to reopen DB after crash");

        // Count recovered keys
        let mut recovered = 0;
        for i in 0..200 {
            let key = format!("key_{:04}", i);
            if db.get(key.as_bytes()).expect("Get failed").is_some() {
                recovered += 1;
            }
        }

        // Should recover most/all data (flush completed before crash)
        println!("✓ Crash during flush: recovered {}/200 keys", recovered);
        assert!(
            recovered >= 190,
            "Too much data loss: only {}/200 recovered",
            recovered
        );
    }
}

#[test]
#[ignore]
fn test_repeated_crash_recovery() {
    if !is_root() {
        eprintln!("Skipping: requires root privileges");
        return;
    }

    let mut harness = DmFlakeyHarness::new("repeated_crash").expect("Failed to create harness");
    harness.setup().expect("Failed to setup harness");

    // Multiple crash-recover cycles
    for cycle in 0..5 {
        // Write phase
        {
            use seerdb::{DBOptions, DB};
            let opts = DBOptions {
                data_dir: harness.data_path(),
                ..Default::default()
            };
            let db = DB::open(opts).expect("Failed to open DB");

            for i in 0..50 {
                let key = format!("cycle{}_{:04}", cycle, i);
                db.put(key.as_bytes(), b"value").expect("Put failed");
            }
            db.flush().expect("Flush failed");
        }

        // Crash
        harness.simulate_crash().expect("Failed to simulate crash");
        harness.recover().expect("Failed to recover");
    }

    // Final verification
    {
        use seerdb::{DBOptions, DB};
        let opts = DBOptions {
            data_dir: harness.data_path(),
            ..Default::default()
        };
        let db = DB::open(opts).expect("Failed to open DB");

        // All flushed data from all cycles should be present
        for cycle in 0..5 {
            for i in 0..50 {
                let key = format!("cycle{}_{:04}", cycle, i);
                let value = db.get(key.as_bytes()).expect("Get failed");
                assert!(
                    value.is_some(),
                    "Key {} missing after repeated crashes",
                    key
                );
            }
        }
        println!("✓ Repeated crash recovery: all 250 keys recovered");
    }
}

#[test]
#[ignore]
fn test_crash_during_compaction() {
    if !is_root() {
        eprintln!("Skipping: requires root privileges");
        return;
    }

    let mut harness = DmFlakeyHarness::new("crash_compact").expect("Failed to create harness");
    harness.setup().expect("Failed to setup harness");

    // Phase 1: Create multiple SSTables to trigger compaction
    {
        use seerdb::{DBOptions, DB};
        let opts = DBOptions {
            data_dir: harness.data_path(),
            memtable_size: 1024 * 64, // 64KB memtable for faster flushes
            ..Default::default()
        };
        let db = DB::open(opts).expect("Failed to open DB");

        // Write enough data to trigger multiple flushes and compaction
        for batch in 0..10 {
            for i in 0..100 {
                let key = format!("batch{}_{:04}", batch, i);
                db.put(key.as_bytes(), &vec![b'v'; 100])
                    .expect("Put failed");
            }
            db.flush().expect("Flush failed");
        }

        // Trigger compaction and crash
        // Note: This may not catch mid-compaction state without failpoints
        harness.simulate_crash().expect("Failed to simulate crash");
    }

    // Phase 2: Recover and verify
    harness.recover().expect("Failed to recover");
    {
        use seerdb::{DBOptions, DB};
        let opts = DBOptions {
            data_dir: harness.data_path(),
            ..Default::default()
        };
        let db = DB::open(opts).expect("Failed to reopen DB after crash");

        // Verify all data is intact
        let mut found = 0;
        for batch in 0..10 {
            for i in 0..100 {
                let key = format!("batch{}_{:04}", batch, i);
                if db.get(key.as_bytes()).expect("Get failed").is_some() {
                    found += 1;
                }
            }
        }

        println!("✓ Crash during compaction: recovered {}/1000 keys", found);
        assert!(
            found >= 950,
            "Too much data loss: only {}/1000 recovered",
            found
        );
    }
}

/// Verify helper: print summary of db state after crash
#[allow(dead_code)]
fn verify_db_state(data_path: &Path) -> Result<(usize, usize), String> {
    use seerdb::{DBOptions, DB};

    let opts = DBOptions {
        data_dir: data_path.to_path_buf(),
        ..Default::default()
    };

    let db = DB::open(opts).map_err(|e| format!("Failed to open DB: {}", e))?;

    // Count SST files
    let sst_count = fs::read_dir(data_path)
        .map_err(|e| format!("Failed to read dir: {}", e))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
        .count();

    // Count WAL files
    let wal_count = fs::read_dir(data_path)
        .map_err(|e| format!("Failed to read dir: {}", e))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .count();

    Ok((sst_count, wal_count))
}
