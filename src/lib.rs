use futures::{Stream, StreamExt};
use pin_project::pin_project;
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeSeq;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::time::sleep;

#[pin_project]
pub struct RecordedStream<S: Stream> {
    #[pin]
    inner: S,
    recording: Recording<S::Item>,
    pub seq: u64,
    pub last_timestamp: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct Recording<S>(Arc<Mutex<Vec<RecordedItem<S>>>>);

#[derive(Debug, Clone)]
pub struct RecordedItem<T>
{
    pub seq: u64,
    pub timestamp: Instant,
    pub delta: Duration,
    pub data: Arc<T>,
}

impl<S: Stream> Stream for RecordedStream<S>
where
    S::Item: Clone,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let result = this.inner.poll_next(cx);

        if let Poll::Ready(Some(ref item)) = result {
            let now = Instant::now();
            let delta = this
                .last_timestamp
                .map(|last| now.duration_since(last))
                .unwrap_or(Duration::ZERO);

            let mut recording = this.recording.0.lock().unwrap();
            recording.push(RecordedItem {
                seq: *this.seq,
                timestamp: now,
                delta,
                data: Arc::new(item.clone()),
            });

            *this.seq += 1;
            *this.last_timestamp = Some(now);
        }

        result
    }
}

impl<S> Recording<S> {
    pub fn new() -> Self {
        Recording(Arc::new(Mutex::new(Vec::new())))
    }

    pub fn peek_at(&self, seq: u64) -> Option<Arc<S>> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .find(|item| item.seq == seq)
            .map(|item| Arc::clone(&item.data))
    }

    pub fn peek_range(&self, start: u64, end: u64) -> Vec<Arc<S>> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .filter(|item| item.seq >= start && item.seq <= end)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    pub fn peek_last(&self, n: usize) -> Vec<Arc<S>> {
        let recording = self.0.lock().unwrap();
        let len = recording.len();
        let start = len.saturating_sub(n);
        recording
            .iter()
            .skip(start)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    pub fn peek_since(&self, since: Instant) -> Vec<Arc<S>> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .filter(|item| item.timestamp >= since)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    pub fn peek_between(&self, start: Instant, end: Instant) -> Vec<Arc<S>> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .filter(|item| item.timestamp >= start && item.timestamp <= end)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    pub fn peek_last_duration(&self, duration: Duration) -> Vec<Arc<S>> {
        let recording = self.0.lock().unwrap();
        if recording.is_empty() {
            return Vec::new();
        }

        let first_timestamp = recording.first().unwrap().timestamp;
        let last_timestamp = recording.last().unwrap().timestamp;
        let cutoff = last_timestamp
            .checked_sub(duration)
            .unwrap_or(first_timestamp);

        let mut items: Vec<_> = recording
            .iter()
            .rev()
            .take_while(|item| item.timestamp >= cutoff)
            .map(|item| Arc::clone(&item.data))
            .collect();
        items.reverse();
        items
    }
}

impl<S: Stream> RecordedStream<S>
where
    S::Item: std::fmt::Debug + Clone,
{
    pub fn recording(&self) -> Recording<S::Item> {
        self.recording.clone()
    }

    pub fn replay(&self) -> impl Stream<Item = Arc<S::Item>> {
        let items: Vec<_> = self.recording.0.lock().unwrap().clone();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }

    pub fn replay_from(&self, start_seq: u64) -> impl Stream<Item = Arc<S::Item>> {
        let items: Vec<_> = self
            .recording
            .0
            .lock()
            .unwrap()
            .iter()
            .skip_while(|i| i.seq < start_seq)
            .cloned()
            .collect();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }

    pub fn replay_since(&self, since: Instant) -> impl Stream<Item = Arc<S::Item>> {
        let items: Vec<_> = self
            .recording
            .0
            .lock()
            .unwrap()
            .iter()
            .skip_while(|i| i.timestamp < since)
            .cloned()
            .collect();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }

    pub fn replay_range(&self, start: u64, end: u64) -> impl Stream<Item = Arc<S::Item>> {
        let items: Vec<_> = self
            .recording
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|i| i.seq >= start && i.seq <= end)
            .cloned()
            .collect();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }
}

pub fn record<S: Stream<Item = T>, T: Clone>(s: S) -> RecordedStream<S> {
    RecordedStream {
        inner: s,
        recording: Recording::new(),
        seq: 0,
        last_timestamp: Some(Instant::now()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant as TokioInstant;

    #[tokio::test]
    async fn test_replay_timing_matches_original() {
        // Create a stream with known delays
        let delays = vec![
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(150),
        ];

        let stream = futures::stream::iter(vec![1, 2, 3])
            .zip(futures::stream::iter(delays.clone()))
            .then(|(x, delay)| async move {
                sleep(delay).await;
                x
            });

        let mut recorded = record(stream);
        tokio::pin!(recorded);

        // Record original timing
        let start = TokioInstant::now();
        let mut original_timings = Vec::new();

        while recorded.next().await.is_some() {
            original_timings.push(start.elapsed());
        }

        // Replay and record new timing
        let replay_stream = recorded.replay();
        tokio::pin!(replay_stream);

        let replay_start = TokioInstant::now();
        let mut replay_timings = Vec::new();

        while replay_stream.next().await.is_some() {
            replay_timings.push(replay_start.elapsed());
        }

        // Compare timings with threshold
        let threshold = Duration::from_millis(10);
        assert_eq!(original_timings.len(), replay_timings.len());

        for (orig, replay) in original_timings.iter().zip(replay_timings.iter()) {
            let diff = if orig > replay {
                *orig - *replay
            } else {
                *replay - *orig
            };
            assert!(
                diff < threshold,
                "Timing mismatch: original={:?}, replay={:?}, diff={:?}",
                orig,
                replay,
                diff
            );
        }
    }

    #[test]
    fn peek_last_duration_returns_chronological_order() {
        let mut recording = Recording::new();
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(100);

        {
            let mut inner = recording.0.lock().unwrap();
            inner.push(Arc::new(RecordedItem {
                seq: 0,
                timestamp: base,
                delta: Duration::ZERO,
                data: Arc::new(1),
            }));

            inner.push(Arc::new(RecordedItem {
                seq: 1,
                timestamp: base + Duration::from_secs(1),
                delta: Duration::from_secs(1),
                data: Arc::new(2),
            }));

            inner.push(Arc::new(RecordedItem {
                seq: 2,
                timestamp: base + Duration::from_secs(3),
                delta: Duration::from_secs(2),
                data: Arc::new(3),
            }));
        }

        let values: Vec<_> = recording
            .peek_last_duration(Duration::from_secs(2))
            .into_iter()
            .map(|item| *item)
            .collect();

        assert_eq!(values, vec![2, 3]);
    }
}
