#define _GNU_SOURCE

// Deliberately small Linux-only LD_PRELOAD helper for the external syscall
// boundary gate. It returns EIO at one selected libc call and otherwise calls
// through to libc. This does not emulate torn sectors or a power cut.

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

typedef int (*fsync_fn)(int);
typedef int (*rename_fn)(const char *, const char *);
typedef int (*renameat_fn)(int, const char *, int, const char *);

static _Atomic unsigned long fsync_count;
static _Atomic unsigned long fdatasync_count;
static _Atomic unsigned long rename_count;

static void trace_call(const char *name, unsigned long count, int fd) {
    const char *path = getenv("SEERDB_FAULT_TRACE");
    if (path == NULL || *path == '\0') {
        return;
    }

    int saved_errno = errno;
    int trace_fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (trace_fd >= 0) {
        char line[96];
        int length = snprintf(line, sizeof(line), "%s %lu %d\n", name, count, fd);
        if (length > 0 && (size_t)length < sizeof(line)) {
            (void)syscall(SYS_write, trace_fd, line, (size_t)length);
        }
        (void)close(trace_fd);
    }
    errno = saved_errno;
}

static int should_fail(const char *name, unsigned long count) {
    const char *target = getenv("SEERDB_FAULT_SYSCALL");
    const char *after = getenv("SEERDB_FAULT_AFTER");
    if (target == NULL || after == NULL || strcmp(target, name) != 0) {
        return 0;
    }

    char *end = NULL;
    unsigned long selected = strtoul(after, &end, 10);
    return end != after && *end == '\0' && selected > 0 && count == selected;
}

static fsync_fn real_fsync(void) {
    static fsync_fn function;
    if (function == NULL) {
        function = (fsync_fn)dlsym(RTLD_NEXT, "fsync");
    }
    return function;
}

static fsync_fn real_fdatasync(void) {
    static fsync_fn function;
    if (function == NULL) {
        function = (fsync_fn)dlsym(RTLD_NEXT, "fdatasync");
    }
    return function;
}

static rename_fn real_rename(void) {
    static rename_fn function;
    if (function == NULL) {
        function = (rename_fn)dlsym(RTLD_NEXT, "rename");
    }
    return function;
}

static renameat_fn real_renameat(void) {
    static renameat_fn function;
    if (function == NULL) {
        function = (renameat_fn)dlsym(RTLD_NEXT, "renameat");
    }
    return function;
}

int fsync(int fd) {
    unsigned long count = atomic_fetch_add_explicit(&fsync_count, 1, memory_order_relaxed) + 1;
    trace_call("fsync", count, fd);
    if (should_fail("fsync", count)) {
        errno = EIO;
        return -1;
    }
    fsync_fn function = real_fsync();
    if (function == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return function(fd);
}

int fdatasync(int fd) {
    unsigned long count =
        atomic_fetch_add_explicit(&fdatasync_count, 1, memory_order_relaxed) + 1;
    trace_call("fdatasync", count, fd);
    if (should_fail("fdatasync", count)) {
        errno = EIO;
        return -1;
    }
    fsync_fn function = real_fdatasync();
    if (function == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return function(fd);
}

int rename(const char *old_path, const char *new_path) {
    unsigned long count = atomic_fetch_add_explicit(&rename_count, 1, memory_order_relaxed) + 1;
    trace_call("rename", count, -1);
    if (should_fail("rename", count)) {
        errno = EIO;
        return -1;
    }
    rename_fn function = real_rename();
    if (function == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return function(old_path, new_path);
}

int renameat(int old_directory, const char *old_path, int new_directory, const char *new_path) {
    unsigned long count = atomic_fetch_add_explicit(&rename_count, 1, memory_order_relaxed) + 1;
    trace_call("rename", count, -1);
    if (should_fail("rename", count)) {
        errno = EIO;
        return -1;
    }
    renameat_fn function = real_renameat();
    if (function == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return function(old_directory, old_path, new_directory, new_path);
}
