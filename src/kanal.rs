use crate::common::{Monitor, UPDATE_INTERVAL_MILLIS};
pub use kanal;

#[macro_export]
macro_rules! spawn_monitor {
    ($name:expr, $rx:expr) => {{
        let name = $name.into();
        let receiver = $rx.clone();

        tokio::spawn(async move {
            tracing::info!("Starting kanal channel monitor for '{name}'");
            let monitor = Monitor {
                name: name.as_str(),
            };
            loop {
                if receiver.is_disconnected() {
                    break;
                } else {
                    monitor.set_metrics(
                        receiver.capacity() as i64,
                        receiver.len() as i64,
                        receiver.sender_count() as i64,
                        receiver.receiver_count().saturating_sub(1) as i64,
                    );
                }

                let update_interval =
                    UPDATE_INTERVAL_MILLIS.load(std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(update_interval)).await;
            }

            tracing::info!("Stopping kanal channel monitor for '{name}'");
        })
    }};
}

pub fn create<T: Send + 'static>(
    name: impl Into<String>,
    capacity: Option<usize>,
) -> (kanal::Sender<T>, kanal::Receiver<T>) {
    let (tx, rx) = capacity
        .map(|cap| kanal::bounded(cap))
        .unwrap_or_else(|| kanal::unbounded());
    spawn_monitor!(name, &rx);
    (tx, rx)
}

pub fn create_async<T: Send + 'static>(
    name: impl Into<String>,
    capacity: Option<usize>,
) -> (kanal::AsyncSender<T>, kanal::AsyncReceiver<T>) {
    let (tx, rx) = capacity
        .map(|cap| kanal::bounded_async(cap))
        .unwrap_or_else(|| kanal::unbounded_async());
    spawn_monitor!(name, &rx);
    (tx, rx)
}

#[cfg(test)]
mod tests {
    use crate::common::{
        CHANNEL_CAPACITY, CHANNEL_LEN, RECEIVER_COUNT, SENDER_COUNT, set_update_interval,
    };

    #[tokio::test]
    async fn test_channel_async() {
        set_update_interval(std::time::Duration::from_millis(100));

        for (name, cap) in [
            ("kanal_async_test_channel_bounded", Some(10)),
            ("kanal_async_test_channel_unbounded", None),
        ] {
            let (tx, rx) = super::create_async(name, cap);

            tx.send(1).await.unwrap();
            tx.send(2).await.unwrap();
            tx.send(3).await.unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            assert_eq!(
                CHANNEL_CAPACITY
                    .get_metric_with_label_values(&[name])
                    .unwrap()
                    .get(),
                cap.map(|c| c as i64).unwrap_or(usize::MAX as i64)
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

            assert_eq!(rx.recv().await.unwrap(), 1);
            assert_eq!(rx.recv().await.unwrap(), 2);
            assert_eq!(rx.recv().await.unwrap(), 3);
        }
    }

    #[tokio::test]
    async fn test_channel_sync() {
        set_update_interval(std::time::Duration::from_millis(100));

        for (name, cap) in [
            ("kanal_sync_test_channel_bounded", Some(10)),
            ("kanal_sync_test_channel_unbounded", None),
        ] {
            let (tx, rx) = super::create(name, cap);

            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            assert_eq!(
                CHANNEL_CAPACITY
                    .get_metric_with_label_values(&[name])
                    .unwrap()
                    .get(),
                cap.map(|c| c as i64).unwrap_or(usize::MAX as i64)
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

            assert_eq!(rx.recv().unwrap(), 1);
            assert_eq!(rx.recv().unwrap(), 2);
            assert_eq!(rx.recv().unwrap(), 3);
        }
    }
}
