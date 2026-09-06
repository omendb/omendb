//! Isolate per-sync latency on this host across the dimensions that could
//! explain the OmenDB-vs-PostgreSQL gap: sync primitive (sync_data vs
//! sync_all vs no-write barrier) and volume (TMPDIR vs an explicit path,
//! matching where each engine keeps its durable artifacts).
//!
//! The wave-cost probe measured the commit path at ~12.7 ms/txn with three
//! sync-bearing phases (~4.2 ms each), while PostgreSQL sustains ~0.5 ms
//! commits with one sync. This probe answers whether the difference is the
//! primitive, the volume, or the write pattern.
//!
//! Run with:
//!   cargo run --release -p seerdb --example fsync_dimensions -- [syncs] [path]

#![allow(clippy::disallowed_methods)]

use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

fn main() {
    let syncs: usize = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let path: std::path::PathBuf = env::args()
        .nth(2)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let _ = std::fs::create_dir_all(&path);

    let payload = vec![0u8; 4096];

    // Variant A: sync_data after a 4 KiB append (fdatasync-class).
    let ms = measure(path.join("a-sync-data"), &payload, syncs, |file| {
        file.sync_data()
    });
    println!("sync_data (4 KiB dirty):   {ms:>10.3} ms/sync");

    // Variant B: sync_all after a 4 KiB append (fsync-class).
    let ms = measure(path.join("b-sync-all"), &payload, syncs, |file| {
        file.sync_all()
    });
    println!("sync_all  (4 KiB dirty):   {ms:>10.3} ms/sync");

    // Variant C: sync_data with NO intervening write (pure barrier cost).
    let ms = measure(path.join("c-barrier"), &[], syncs, |file| file.sync_data());
    println!("sync_data (clean file):    {ms:>10.3} ms/sync");

    // Variant D: F_FULLFSYNC via libc fcntl — the strongest macOS flush
    // and PostgreSQL's fsync_writethrough equivalent.
    let full = measure_fullfsync(path.join("d-fullfsync"), &payload, syncs);
    println!("F_FULLFSYNC (4 KiB):       {full:>10.3} ms/sync");

    // Variant E: append-growth vs PRE-ALLOCATED write-in-place. If the
    // file is ftruncated to full size first, writes never grow it, so
    // each sync flushes data pages only — no inode/extent metadata.
    let pre_ms = measure_preallocated(path.join("e-prealloc"), &payload, syncs);
    println!("pre-allocated 4 KiB:      {pre_ms:>10.3} ms/sync");

    // Variant F: many small files (SeerDB writes pages spread across the
    // data file out-of-place; this checks spread vs append locality).
    let spread = measure_spread(path.join("f-spread"), &payload, syncs);
    println!("spread appends, one sync:  {spread:>10.3} ms/sync");

    // Variant G: TINY append per sync — PostgreSQL's WAL record shape
    // (~100 bytes). If sync latency scales with dirty bytes, this is
    // the single biggest behavioral difference.
    let tiny = vec![0u8; 128];
    let ms = measure(path.join("g-tiny-append"), &tiny, syncs, |file| {
        file.sync_data()
    });
    println!("sync_data (128 B append):  {ms:>10.3} ms/sync");

    // Variant H: PLAIN libc fsync() — what PostgreSQL's fsync method
    // calls on macOS (no F_FULLFSYNC). If Rust's sync_data maps to
    // F_FULLFSYNC (the device barrier), this measures the strength gap.
    let plain = measure_plain_fsync(path.join("h-plain-fsync"), &payload, syncs);
    println!("plain fsync (4 KiB):       {plain:>10.3} ms/sync");

    // Variant I: O_DSYNC writes — PostgreSQL's open_datasync method, the
    // installed oracle's default. The write itself is the barrier; no
    // separate sync call. This is the candidate default sync class.
    let odsync = measure_odsync(path.join("i-odsync"), &payload, syncs);
    println!("O_DSYNC writes (4 KiB):    {odsync:>10.3} ms/write");
}

fn measure_odsync(path: std::path::PathBuf, payload: &[u8], syncs: usize) -> f64 {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .custom_flags(libc::O_DSYNC)
        .open(&path)
        .expect("open O_DSYNC probe file");
    let started = Instant::now();
    for _ in 0..syncs {
        file.write_all(payload).expect("O_DSYNC write");
    }
    started.elapsed().as_secs_f64() * 1000.0 / syncs as f64
}

fn measure_plain_fsync(path: std::path::PathBuf, payload: &[u8], syncs: usize) -> f64 {
    use std::os::unix::io::AsRawFd;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open probe file");
    let started = Instant::now();
    for _ in 0..syncs {
        file.write_all(payload).expect("probe write");
        let outcome = unsafe { libc::fsync(file.as_raw_fd()) };
        if outcome != 0 {
            panic!("plain fsync failed: {}", std::io::Error::last_os_error());
        }
    }
    started.elapsed().as_secs_f64() * 1000.0 / syncs as f64
}

fn measure_preallocated(path: std::path::PathBuf, payload: &[u8], syncs: usize) -> f64 {
    use std::io::Seek;
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("open probe file");
    file.set_len((syncs as u64 + 1) * payload.len() as u64)
        .expect("pre-allocate");
    let mut offset = 0u64;
    let started = Instant::now();
    for _ in 0..syncs {
        file.seek(std::io::SeekFrom::Start(offset)).expect("seek");
        file.write_all(payload).expect("probe write");
        offset += payload.len() as u64;
        file.sync_data().expect("probe sync");
    }
    started.elapsed().as_secs_f64() * 1000.0 / syncs as f64
}

fn measure_spread(path: std::path::PathBuf, payload: &[u8], syncs: usize) -> f64 {
    use std::io::Seek;
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("open probe file");
    file.set_len((syncs as u64 + 1) * payload.len() as u64 * 8)
        .expect("pre-allocate spread");
    // Write 4 KiB at a different 32 KiB-strided offset each iteration,
    // then one sync — approximating a page flush spread across the file.
    let stride = 32 * 1024u64;
    let mut offset = 0u64;
    let started = Instant::now();
    for _ in 0..syncs {
        file.seek(std::io::SeekFrom::Start(offset)).expect("seek");
        file.write_all(payload).expect("probe write");
        offset = (offset + stride) % (syncs as u64 * stride);
        file.sync_data().expect("probe sync");
    }
    started.elapsed().as_secs_f64() * 1000.0 / syncs as f64
}

fn measure<F>(path: std::path::PathBuf, payload: &[u8], syncs: usize, sync: F) -> f64
where
    F: Fn(&std::fs::File) -> std::io::Result<()>,
{
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open probe file");
    let started = Instant::now();
    for _ in 0..syncs {
        if !payload.is_empty() {
            file.write_all(payload).expect("probe write");
        }
        sync(&file).expect("probe sync");
    }
    started.elapsed().as_secs_f64() * 1000.0 / syncs as f64
}

fn measure_fullfsync(path: std::path::PathBuf, payload: &[u8], syncs: usize) -> f64 {
    use std::os::unix::io::AsRawFd;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open probe file");
    let started = Instant::now();
    for _ in 0..syncs {
        file.write_all(payload).expect("probe write");
        // F_FULLFSYNC = 51 on macOS/darwin.
        let outcome = unsafe { libc::fcntl(file.as_raw_fd(), 51) };
        if outcome != 0 {
            let error = std::io::Error::last_os_error();
            println!("F_FULLFSYNC unsupported here: {error}");
            break;
        }
    }
    started.elapsed().as_secs_f64() * 1000.0 / syncs as f64
}
