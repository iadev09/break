use std::time::Duration;

/// Request admission after TCP setup to a completely decoded HTTP response.
/// Includes write backpressure, response wait and validation, including drain.
/// Incomplete/abandoned attempts have no sample; late and invalid replies do.
#[derive(Clone, Copy, Debug, Default)]
pub struct LatencyStats {
    pub responses: u64,
    pub total_nanos: u128,
}

impl LatencyStats {
    pub fn mean_millis(&self) -> Option<f64> {
        (self.responses != 0)
            .then(|| self.total_nanos as f64 / self.responses as f64 / 1e6)
    }

    pub(crate) fn record(
        &mut self,
        elapsed: Duration,
    ) {
        self.responses += 1;
        self.total_nanos += elapsed.as_nanos();
    }

    pub(crate) fn add(
        &mut self,
        other: Self,
    ) {
        self.responses += other.responses;
        self.total_nanos += other.total_nanos;
    }
}

#[cfg(test)]
#[path = "../tests/latency.rs"]
mod tests;
