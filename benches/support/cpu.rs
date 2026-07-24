//! Process CPU time (user + system) for relative client-cost comparisons.
//!
//! Uses `getrusage` (POSIX). Allowed here despite crate `unsafe_code = deny`
//! because benches need process CPU attribution and this is the standard API.

#![allow(unsafe_code)]

use std::time::Duration;

/// Cumulative user+system CPU time for this process (`getrusage(RUSAGE_SELF)`).
pub fn process_cpu_time() -> Duration {
    // SAFETY: `RUSAGE_SELF` writes a fully initialized `rusage` on success (rc == 0).
    let usage = unsafe {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let rc = libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        assert_eq!(rc, 0, "getrusage(RUSAGE_SELF) failed");
        usage.assume_init()
    };
    timeval_to_duration(usage.ru_utime) + timeval_to_duration(usage.ru_stime)
}

fn timeval_to_duration(tv: libc::timeval) -> Duration {
    let secs = u64::try_from(tv.tv_sec).unwrap_or(0);
    let micros = u64::try_from(tv.tv_usec).unwrap_or(0);
    Duration::from_secs(secs) + Duration::from_micros(micros)
}
