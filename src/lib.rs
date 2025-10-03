use futures::{Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime},
};
use tokio::time::sleep;

#[pin_project]
pub struct RecordedStream<S: Stream> {
    #[pin]
    inner: S,
    recording: Recording<S::Item>,
    pub seq: u64,
    pub last_timestamp: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct Recording<S>(Arc<Mutex<Vec<RecordedItem<S>>>>);

impl<T: Serialize> Serialize for Recording<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.lock().unwrap().serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Recording<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let items = Vec::<RecordedItem<T>>::deserialize(deserializer)?;
        Ok(Recording(Arc::new(Mutex::new(items))))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedItem<T> {
    pub seq: u64,
    pub timestamp: SystemTime,
    pub delta: Duration,
    pub data: T,
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
            let now = SystemTime::now();
            let delta = this
                .last_timestamp
                .and_then(|last| now.duration_since(last).ok())
                .unwrap_or(Duration::ZERO);

            let mut recording = this.recording.0.lock().unwrap();
            recording.push(RecordedItem {
                seq: *this.seq,
                timestamp: now,
                delta,
                data: item.clone(),
            });

            *this.seq += 1;
            *this.last_timestamp = Some(now);
        }

        result
    }
}

impl<S: Clone> Recording<S> {
    pub fn new() -> Self {
        Recording(Arc::new(Mutex::new(Vec::new())))
    }

    pub fn peek_at(&mut self, seq: u64) -> Option<S> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .find(|item| item.seq == seq)
            .map(|item| item.data.clone())
    }

    pub fn peek_range(&mut self, start: u64, end: u64) -> Vec<S> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .filter(|item| item.seq >= start && item.seq <= end)
            .map(|item| item.data.clone())
            .collect()
    }

    pub fn peek_last(&mut self, n: usize) -> Vec<S> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .rev()
            .take(n)
            .map(|item| item.data.clone())
            .collect()
    }

    pub fn peek_since(&mut self, since: SystemTime) -> Vec<S> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .filter(|item| item.timestamp >= since)
            .map(|item| item.data.clone())
            .collect()
    }

    pub fn peek_between(&mut self, start: SystemTime, end: SystemTime) -> Vec<S> {
        let recording = self.0.lock().unwrap();
        recording
            .iter()
            .filter(|item| item.timestamp >= start && item.timestamp <= end)
            .map(|item| item.data.clone())
            .collect()
    }

    pub fn peek_last_duration(&mut self, duration: Duration) -> Vec<S> {
        let recording = self.0.lock().unwrap();
        let cutoff = recording
            .last()
            .map(|last| last.timestamp - duration)
            .unwrap_or(SystemTime::now());

        recording
            .iter()
            .rev()
            .take_while(|item| item.timestamp >= cutoff)
            .map(|item| item.data.clone())
            .collect()
    }
}

impl<S: Stream> RecordedStream<S>
where
    S::Item: std::fmt::Debug + Clone,
{
    pub fn recording(&mut self) -> Recording<S::Item> {
        self.recording.clone()
    }

    pub fn replay(&self) -> impl Stream<Item = S::Item> {
        let items = self.recording.0.lock().unwrap().clone();
        futures::stream::iter(items).then(|item| async move {
            println!("Replaying item: {:?}", item);
            sleep(item.delta).await;
            item.data
        })
    }
}

pub fn record<S: Stream<Item = T>, T: Clone>(s: S) -> RecordedStream<S> {
    RecordedStream {
        inner: s,
        recording: Recording::new(),
        seq: 0,
        last_timestamp: Some(SystemTime::now()),
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
}
