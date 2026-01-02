use std::sync::LazyLock;

pub(crate) static CHANNEL_CAPACITY: LazyLock<prometheus::IntGaugeVec> = LazyLock::new(|| {
    prometheus::register_int_gauge_vec!(
        "channel_capacity",
        "Capacity of flume channel",
        &["channel_name"]
    )
    .expect("Failed to register 'channel_capacity' metric")
});

pub(crate) static CHANNEL_LEN: LazyLock<prometheus::IntGaugeVec> = LazyLock::new(|| {
    prometheus::register_int_gauge_vec!("channel_len", "Length of flume channel", &["channel_name"])
        .expect("Failed to register 'channel_len' metric")
});

pub(crate) static SENDER_COUNT: LazyLock<prometheus::IntGaugeVec> = LazyLock::new(|| {
    prometheus::register_int_gauge_vec!(
        "channel_sender_count",
        "Number of flume senders",
        &["channel_name"]
    )
    .expect("Failed to register 'sender_count' metric")
});

pub(crate) static RECEIVER_COUNT: LazyLock<prometheus::IntGaugeVec> = LazyLock::new(|| {
    prometheus::register_int_gauge_vec!(
        "channel_receiver_count",
        "Number of flume receivers",
        &["channel_name"]
    )
    .expect("Failed to register 'receiver_count' metric")
});

pub(crate) static UPDATE_INTERVAL_MILLIS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(200);

pub fn set_update_interval(duration: std::time::Duration) {
    UPDATE_INTERVAL_MILLIS.store(
        duration.as_millis() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
}

pub(crate) struct Monitor<'a> {
    pub name: &'a str,
}

impl<'a> Monitor<'a> {
    pub(crate) fn set_metrics(&self, cap: i64, len: i64, senders: i64, receivers: i64) {
        CHANNEL_CAPACITY.with_label_values(&[self.name]).set(cap);
        CHANNEL_LEN.with_label_values(&[self.name]).set(len);
        SENDER_COUNT.with_label_values(&[self.name]).set(senders);
        RECEIVER_COUNT
            .with_label_values(&[self.name])
            .set(receivers);
    }
}

/// 'remove_label_values' docs says:
/// "It returns an error if the number of label values is not the same as the
/// number of VariableLabels in Desc."
///
/// And that is a lie it returns an error if metric for given label is never set,
/// so if monitor is dropped before ever increasing given label.
/// So just ignore the value.
impl<'a> Drop for Monitor<'a> {
    fn drop(&mut self) {
        let _ = CHANNEL_CAPACITY.remove_label_values(&[self.name]);
        let _ = CHANNEL_LEN.remove_label_values(&[self.name]);
        let _ = SENDER_COUNT.remove_label_values(&[self.name]);
        let _ = RECEIVER_COUNT.remove_label_values(&[self.name]);
    }
}

#[cfg(test)]
mod tests {
    use super::{CHANNEL_CAPACITY, CHANNEL_LEN, Monitor, RECEIVER_COUNT, SENDER_COUNT};

    #[tokio::test]
    async fn monitor_test() {
        let name = "test";
        {
            let monitor = Monitor { name };
            monitor.set_metrics(1, 2, 3, 4);

            assert_eq!(CHANNEL_CAPACITY.with_label_values(&[name]).get(), 1);
            assert_eq!(CHANNEL_LEN.with_label_values(&[name]).get(), 2);
            assert_eq!(SENDER_COUNT.with_label_values(&[name]).get(), 3);
            assert_eq!(RECEIVER_COUNT.with_label_values(&[name]).get(), 4);
        }
        assert_eq!(CHANNEL_CAPACITY.with_label_values(&[name]).get(), 0);
        assert_eq!(CHANNEL_LEN.with_label_values(&[name]).get(), 0);
        assert_eq!(SENDER_COUNT.with_label_values(&[name]).get(), 0);
        assert_eq!(RECEIVER_COUNT.with_label_values(&[name]).get(), 0);
    }
}
