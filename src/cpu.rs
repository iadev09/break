//! The machine's CPU over a window, from the kernel's own counters: two
//! readings, the difference between them. Every CPU and every process is
//! counted, so the busy part shows what the machine did and the idle part what
//! it had left.

use std::time::Instant;

/// One reading of every CPU's counters.
#[derive(Clone, Copy, Debug)]
pub struct MachineSample {
    at: Instant,
    cpus: u32,
    user: u64,
    system: u64,
    irq: u64,
    softirq: u64,
    iowait: u64,
    steal: u64,
    idle: u64,
}

impl MachineSample {
    /// Read the counters now. `None` where no reading is implemented for the
    /// platform (it is on Linux) or the counters cannot be read.
    pub fn read() -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            let stat = std::fs::read_to_string("/proc/stat").ok()?;
            Self::parse(&stat, Instant::now())
        }
        #[cfg(not(target_os = "linux"))]
        None
    }

    /// `/proc/stat`'s total line and the number of its per-CPU lines.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn parse(
        stat: &str,
        at: Instant,
    ) -> Option<Self> {
        let mut lines = stat.lines();
        let fields = lines
            .next()?
            .strip_prefix("cpu ")?
            .split_whitespace()
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        let field = |index: usize| fields.get(index).copied().unwrap_or(0);
        let cpus = lines
            .filter(|line| {
                line.strip_prefix("cpu").is_some_and(|rest| {
                    rest.starts_with(|c: char| c.is_ascii_digit())
                })
            })
            .count() as u32;
        // user nice system idle iowait irq softirq steal; guest time is
        // already counted in user.
        Some(Self {
            at,
            cpus,
            user: field(0) + field(1),
            system: field(2),
            idle: field(3),
            iowait: field(4),
            irq: field(5),
            softirq: field(6),
            steal: field(7),
        })
    }

    /// How the machine spent the time from this reading to `after`. `None`
    /// when a counter went backwards or no time passed.
    pub fn until(
        self,
        after: Self,
    ) -> Option<MachineCpu> {
        let user = after.user.checked_sub(self.user)?;
        let system = after.system.checked_sub(self.system)?;
        let irq = after.irq.checked_sub(self.irq)?;
        let softirq = after.softirq.checked_sub(self.softirq)?;
        let iowait = after.iowait.checked_sub(self.iowait)?;
        let steal = after.steal.checked_sub(self.steal)?;
        let idle = after.idle.checked_sub(self.idle)?;
        let total = user + system + irq + softirq + iowait + steal + idle;
        if total == 0 {
            return None;
        }
        let share = |part: u64| 100.0 * part as f64 / total as f64;
        let busy = total - idle - iowait;
        Some(MachineCpu {
            seconds: after.at.saturating_duration_since(self.at).as_secs_f64(),
            cpus: after.cpus,
            busy_cpus: busy as f64 / total as f64 * after.cpus as f64,
            busy_percent: share(busy),
            user_percent: share(user),
            system_percent: share(system),
            irq_percent: share(irq),
            softirq_percent: share(softirq),
            iowait_percent: share(iowait),
            steal_percent: share(steal),
            idle_percent: share(idle),
        })
    }
}

/// The machine over one window: shares of all CPUs' time, and the busy part
/// as CPUs' worth. `cpus - busy_cpus` is what was left unused.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct MachineCpu {
    pub seconds: f64,
    pub cpus: u32,
    pub busy_cpus: f64,
    pub busy_percent: f64,
    pub user_percent: f64,
    pub system_percent: f64,
    pub irq_percent: f64,
    pub softirq_percent: f64,
    pub iowait_percent: f64,
    pub steal_percent: f64,
    pub idle_percent: f64,
}

#[cfg(test)]
#[path = "../tests/cpu.rs"]
mod tests;
