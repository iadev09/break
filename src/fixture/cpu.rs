//! Process-wide CPU, excluding children, sampled outside the request path.
use std::io;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cpu {
    pub user_ns: u64,
    pub system_ns: u64,
}

impl Cpu {
    pub fn read() -> io::Result<Self> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes rusage on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) }
            != 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful call initialized the value.
        let usage = unsafe { usage.assume_init() };
        let nanos = |time: libc::timeval| -> io::Result<u64> {
            let seconds =
                u64::try_from(time.tv_sec).map_err(io::Error::other)?;
            let micros =
                u64::try_from(time.tv_usec).map_err(io::Error::other)?;
            seconds
                .checked_mul(1_000_000_000)
                .and_then(|ns| ns.checked_add(micros * 1_000))
                .ok_or_else(|| io::Error::other("CPU counter overflow"))
        };
        Ok(Self {
            user_ns: nanos(usage.ru_utime)?,
            system_ns: nanos(usage.ru_stime)?,
        })
    }

    pub fn since(
        self,
        before: Self,
    ) -> io::Result<Self> {
        Ok(Self {
            user_ns: self.user_ns.checked_sub(before.user_ns).ok_or_else(
                || io::Error::other("user CPU counter decreased"),
            )?,
            system_ns: self
                .system_ns
                .checked_sub(before.system_ns)
                .ok_or_else(|| {
                    io::Error::other("system CPU counter decreased")
                })?,
        })
    }

    pub fn seconds(self) -> f64 {
        (self.user_ns as f64 + self.system_ns as f64) / 1e9
    }
}
