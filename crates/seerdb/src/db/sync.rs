//! File syncs use the shared durability primitives; callers own metrics.

pub(crate) use durable_fs::{sync_file_all, sync_file_data};
