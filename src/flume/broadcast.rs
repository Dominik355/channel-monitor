use flume::{Receiver, SendError, Sender};

/// 1-N broadcast channel using flume channels
///
/// Doesn't drop anything, waits for the    slowest receiver
#[derive(Debug)]
pub struct FlumeBroadcastSender<T> {
    cap: usize,
    senders: Vec<(Sender<T>, String)>,
    fail_type: FailType,
    name_prefix: Option<String>,
}

#[allow(dead_code)]
impl<T: Clone + Send + 'static> FlumeBroadcastSender<T> {
    /// Attempts to send a value to all active Receivers.
    ///
    /// Based on the FailType, the message fails if:
    /// - Eager - any receiver has been dropped
    /// - Lazy - there is no active receiver left
    ///
    /// # Return
    ///
    /// On success, the number of subscribed [`Receiver`] handles is returned.
    ///
    ///  # Error
    /// In case FailType::Eager is used, we also get name of the channel which has been dropped
    pub async fn broadcast(&mut self, msg: T) -> Result<usize, (SendError<T>, Option<String>)> {
        let mut idx = 0;
        while idx < self.senders.len() {
            match self.senders[idx].0.send_async(msg.clone()).await {
                Ok(_) => {
                    idx += 1;
                }
                Err(_) => match self.fail_type {
                    FailType::Lazy => {
                        self.senders.remove(idx);
                    }
                    FailType::Eager => {
                        return Err((SendError(msg), Some(self.senders.remove(idx).1)));
                    }
                },
            }
        }

        // if FailyType::Lazy is used and there is no single receiver left
        if idx == 0 {
            Err((SendError(msg), None))
        } else {
            Ok(idx)
        }
    }

    /// Creates a new Receiver
    pub fn subscribe(&mut self, name: impl Into<String>) -> Receiver<T> {
        let name = match self.name_prefix {
            None => name.into(),
            Some(ref prefix) => format!("{}-{}", prefix, name.into()),
        };
        let (tx, rx): (Sender<T>, Receiver<T>) = super::create(&name, Some(self.cap));
        self.senders.push((tx, name));
        rx
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// let's say that current length is length of first receiver,
    /// which might be mostly greater by 1 than others.
    pub fn len(&self) -> Vec<usize> {
        self.senders.iter().map(|s| s.0.len()).collect()
    }

    /// let's say that current length is length of first receiver,
    /// which might be mostly greater by 1 than others.
    pub fn estimated_len(&self) -> usize {
        self.senders.iter().map(|s| s.0.len()).max().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        !self.senders.iter().any(|s| !s.0.is_empty())
    }

    pub fn estimated_space(&self) -> usize {
        self.capacity() - self.estimated_len()
    }
}

/// Drops created receiver so receivers are created by subscribing,
/// so we can register flume metric channel under the given name
///
/// Params.
///
/// * `cap` - Capacity of the channel
/// * `fail_type` - type describing when the broadcast should fail
/// * `name_prefix` - name prefix used for every created channel
pub fn prefixed_channel<T: Clone + Send + 'static>(
    cap: usize,
    fail_type: FailType,
    name_prefix: impl Into<String>,
) -> FlumeBroadcastSender<T> {
    FlumeBroadcastSender {
        cap,
        senders: vec![],
        fail_type,
        name_prefix: Some(name_prefix.into()),
    }
}

pub fn channel<T: Clone + Send + 'static>(
    cap: usize,
    fail_type: FailType,
) -> FlumeBroadcastSender<T> {
    FlumeBroadcastSender {
        cap,
        senders: vec![],
        fail_type,
        name_prefix: None,
    }
}

#[derive(Debug, Clone, Eq, PartialOrd, PartialEq, Copy)]
pub enum FailType {
    /// fail only if there are no receivers left
    Lazy,
    /// fail if any receiver is dropped
    Eager,
}

#[cfg(test)]
mod tests {
    use super::*;
    use flume::{SendError, TryRecvError};

    #[tokio::test]
    async fn test_lazy_fail() {
        let mut tx: FlumeBroadcastSender<usize> = channel(3, FailType::Lazy);

        let rx_1 = tx.subscribe("rx_1");

        assert_eq!(tx.cap, 3);
        assert_eq!(tx.estimated_len(), 0);
        assert_eq!(tx.estimated_space(), 3);

        assert_eq!(tx.broadcast(1).await.unwrap(), 1);
        assert_eq!(tx.estimated_len(), 1);
        assert_eq!(tx.estimated_space(), 2);

        assert!(matches!(rx_1.recv(), Ok(x) if x == 1));
        assert_eq!(tx.estimated_len(), 0);
        assert_eq!(tx.estimated_space(), 3);

        let rx_2 = tx.subscribe("rx_2");

        assert!(matches!(rx_2.try_recv(), Err(err) if err == TryRecvError::Empty));

        assert_eq!(tx.broadcast(2).await.unwrap(), 2);
        assert_eq!(tx.estimated_len(), 1);
        assert_eq!(tx.estimated_space(), 2);

        assert_eq!(tx.broadcast(3).await.unwrap(), 2);
        assert_eq!(tx.estimated_len(), 2);
        assert_eq!(tx.estimated_space(), 1);

        assert_eq!(rx_1.drain().collect::<Vec<usize>>(), vec![2, 3]);
        assert_eq!(rx_2.drain().collect::<Vec<usize>>(), vec![2, 3]);

        drop(rx_1);

        assert_eq!(tx.broadcast(4).await.unwrap(), 1);
        assert_eq!(rx_2.drain().collect::<Vec<usize>>(), vec![4]);

        drop(rx_2);
        let res = tx.broadcast(5).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap(), (SendError(5), None))
    }

    #[tokio::test]
    async fn test_eager_fail() {
        let mut tx: FlumeBroadcastSender<usize> = prefixed_channel(3, FailType::Eager, "test-ch");

        let rx_1 = tx.subscribe("rx_1");
        let rx_2 = tx.subscribe("rx_2");

        assert_eq!(tx.capacity(), 3);
        assert_eq!(tx.estimated_len(), 0);
        assert_eq!(tx.estimated_space(), 3);

        assert_eq!(tx.broadcast(1).await.unwrap(), 2);
        assert_eq!(tx.estimated_len(), 1);
        assert_eq!(tx.estimated_space(), 2);

        assert_eq!(tx.broadcast(2).await.unwrap(), 2);
        assert_eq!(tx.estimated_len(), 2);
        assert_eq!(tx.estimated_space(), 1);

        assert_eq!(rx_1.drain().collect::<Vec<usize>>(), vec![1, 2]);

        let rx_3 = tx.subscribe("rx_2");
        assert_eq!(tx.broadcast(3).await.unwrap(), 3);
        assert_eq!(tx.estimated_len(), 3);
        assert_eq!(tx.estimated_space(), 0);

        assert_eq!(rx_1.drain().collect::<Vec<usize>>(), vec![3]);
        assert_eq!(rx_2.drain().collect::<Vec<usize>>(), vec![1, 2, 3]);
        assert_eq!(rx_3.drain().collect::<Vec<usize>>(), vec![3]);
        assert_eq!(tx.estimated_len(), 0);
        assert_eq!(tx.estimated_space(), 3);

        drop(rx_1);
        let res = tx.broadcast(4).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap(),
            (SendError(4), Some("test-ch-rx_1".to_owned()))
        )
    }
}
