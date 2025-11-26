# TODO - seerdb

**Last Updated**: November 26, 2025
**Version**: 0.0.1-beta

---

## Current Focus

Beta testing phase - no active development tasks.

---

## Future Considerations

| Feature | Priority | Notes |
|---------|----------|-------|
| Object store backend | Low | S3/GCS for cloud deployment |
| Compaction filters | Low | Custom logic during compaction |
| Async API | Low | High-concurrency workloads |

---

## Not Planned

| Feature | Reason |
|---------|--------|
| io_uring | Security CVEs, cloud providers disable |
| SSI (serializable) | OCC+SI sufficient |
| Lock-free WAL | Batch API is the right pattern |
| Column families | Use key prefixes |
| MANIFEST file | Not needed - rebuild from SSTables |
