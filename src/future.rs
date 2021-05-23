use std::future::Future;
use std::time::Duration;
use tokio::time;

pub async fn timeout<T, F>(future: F, timeout_secs: u64) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    let duration = Duration::from_secs(timeout_secs);
    time::timeout(duration, future).await?
}
