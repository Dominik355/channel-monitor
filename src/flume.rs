use crate::common::{Monitor, UPDATE_INTERVAL_MILLIS};
pub use flume;

#[cfg(feature = "broadcast")]
pub mod broadcast;

pub fn create<T: Send + 'static>(
    name: impl Into<String>,
    capacity: Option<usize>,
) -> (flume::Sender<T>, flume::Receiver<T>) {
    let (tx, rx) = capacity
        .map(|cap| flume::bounded(cap))
        .unwrap_or_else(|| flume::unbounded());
    spawn_monitor(name, &tx);
    (tx, rx)
}

fn spawn_monitor<T: Send + 'static>(name: impl Into<String>, tx: &flume::Sender<T>) {
    let name = name.into();
    let weak_sender = tx.downgrade();

    tokio::spawn(async move {
        tracing::info!("Starting flume channel monitor for '{}'", name);
        let monitor = Monitor {
            name: name.as_str(),
        };

        loop {
            match weak_sender.upgrade() {
                Some(sender) => monitor.set_metrics(
                    sender.capacity().map(|cap| cap as i64).unwrap_or(-1),
                    sender.len() as i64,
                    // Subtract one since one sender is acquired here inside the monitor
                    sender.sender_count().saturating_sub(1) as i64,
                    sender.receiver_count() as i64,
                ),
                None => break,
            }
            let update_interval = UPDATE_INTERVAL_MILLIS.load(std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(update_interval)).await;
        }
        tracing::info!("Stopping flume channel monitor for '{}'", name);
    });
}

#[cfg(test)]
mod tests {
    use crate::common::{
        CHANNEL_CAPACITY, CHANNEL_LEN, RECEIVER_COUNT, SENDER_COUNT, set_update_interval,
    };

    #[tokio::test]
    async fn test_channel() {
        set_update_interval(std::time::Duration::from_millis(100));

        for (name, cap) in [
            ("flume_test_channel_bounded", Some(10)),
            ("flume_test_channel_unbounded", None),
        ] {
            let (tx, rx) = super::create(name, cap);

            tx.send_async(1).await.unwrap();
            tx.send_async(2).await.unwrap();
            tx.send_async(3).await.unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            assert_eq!(
                CHANNEL_CAPACITY
                    .get_metric_with_label_values(&[name])
                    .unwrap()
                    .get(),
                cap.map(|c| c as i64).unwrap_or(-1)
            );
            assert_eq!(
                CHANNEL_LEN
                    .get_metric_with_label_values(&[name])
                    .unwrap()
                    .get(),
                3
            );
            assert_eq!(
                SENDER_COUNT
                    .get_metric_with_label_values(&[name])
                    .unwrap()
                    .get(),
                1
            );
            assert_eq!(
                RECEIVER_COUNT
                    .get_metric_with_label_values(&[name])
                    .unwrap()
                    .get(),
                1
            );

            assert_eq!(rx.recv_async().await.unwrap(), 1);
            assert_eq!(rx.recv_async().await.unwrap(), 2);
            assert_eq!(rx.recv_async().await.unwrap(), 3);
        }
    }
}
