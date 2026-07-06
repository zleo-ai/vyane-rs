use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderMap, RETRY_AFTER};
use vyane_core::ErrorKind;

type SleepFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type Sleeper = Arc<dyn Fn(Duration) -> SleepFuture + Send + Sync + 'static>;

#[derive(Clone)]
pub struct RetryConfig {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
    jitter_ratio_millis: u32,
    sleeper: Sleeper,
}

impl fmt::Debug for RetryConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryConfig")
            .field("max_attempts", &self.max_attempts)
            .field("base_delay", &self.base_delay)
            .field("max_delay", &self.max_delay)
            .field("jitter_ratio_millis", &self.jitter_ratio_millis)
            .finish_non_exhaustive()
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            jitter_ratio_millis: 200,
            sleeper: Arc::new(|delay| Box::pin(tokio::time::sleep(delay))),
        }
    }
}

impl RetryConfig {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            ..Self::default()
        }
    }

    pub fn with_base_delay(mut self, delay: Duration) -> Self {
        self.base_delay = delay;
        self
    }

    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn with_jitter_ratio(mut self, ratio: f32) -> Self {
        let ratio = ratio.clamp(0.0, 1.0);
        self.jitter_ratio_millis = (ratio * 1000.0).round() as u32;
        self
    }

    pub fn with_sleeper<F, Fut>(mut self, sleeper: F) -> Self
    where
        F: Fn(Duration) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.sleeper = Arc::new(move |delay| Box::pin(sleeper(delay)));
        self
    }

    pub fn without_sleep(self) -> Self {
        self.with_sleeper(|_| async {})
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts.max(1)
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts()
    }

    pub(crate) fn decision_for(&self, attempt: u32, kind: ErrorKind) -> RetryDecision {
        match kind {
            ErrorKind::RateLimited | ErrorKind::Protocol | ErrorKind::Transport
                if self.should_retry(attempt) =>
            {
                RetryDecision::Retry(self.delay_for(attempt))
            }
            _ => RetryDecision::Stop,
        }
    }

    pub fn delay_for(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31);
        let multiplier = 1_u128 << exponent;
        let base_ms = self.base_delay.as_millis().saturating_mul(multiplier);
        let capped_ms = base_ms.min(self.max_delay.as_millis());
        let jitter_ms = capped_ms
            .saturating_mul(u128::from(self.jitter_ratio_millis))
            .saturating_mul(u128::from(jitter_bucket(attempt)))
            / 1_000_000;
        millis_to_duration(capped_ms.saturating_add(jitter_ms).min(u64::MAX.into()))
    }

    pub async fn sleep(&self, delay: Duration) {
        (self.sleeper)(delay).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    Retry(Duration),
    Stop,
}

pub(crate) fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let raw = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    let seconds = raw.parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

fn jitter_bucket(attempt: u32) -> u32 {
    attempt.wrapping_mul(1_103_515_245).wrapping_add(12_345) % 1_000
}

fn millis_to_duration(ms: u128) -> Duration {
    let ms = u64::try_from(ms).unwrap_or(u64::MAX);
    Duration::from_millis(ms)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use reqwest::header::HeaderValue;

    use super::*;

    #[test]
    fn retry_after_parses_delta_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(2)));
    }

    #[test]
    fn delay_is_bounded() {
        let retry = RetryConfig::default()
            .with_base_delay(Duration::from_secs(10))
            .with_max_delay(Duration::from_secs(1))
            .with_jitter_ratio(0.0);
        assert_eq!(retry.delay_for(3), Duration::from_secs(1));
    }
}
