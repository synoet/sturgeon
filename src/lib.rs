use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::time::sleep;

/// A stream wrapper that records all items passing through it.
///
/// `RecordedStream` wraps any stream and records each item along with timing information,
/// allowing for later replay with the same timing characteristics.
#[pin_project]
pub struct RecordedStream<S: Stream> {
    #[pin]
    inner: S,
    recording: Recording<S::Item>,
    /// The current sequence number for recorded items
    pub seq: u64,
    /// The timestamp of the last recorded item
    pub last_timestamp: Option<Instant>,
    /// The timestamp when recording started
    pub start_timestamp: Instant,
}

/// A thread-safe recording of stream items with optional capacity limits.
///
/// `Recording` stores items with their timing information and can be shared across threads.
/// It supports bounded or unbounded storage.
#[derive(Debug, Clone)]
pub struct Recording<S> {
    items: Arc<Mutex<Vec<RecordedItem<S>>>>,
    capacity: Option<usize>,
}

/// A single recorded item with timing information.
///
/// Each item contains:
/// - `seq`: A sequence number for ordering
/// - `timestamp`: The instant when the item was recorded
/// - `delta`: The duration since the previous item
/// - `data`: The actual item data wrapped in Arc for efficient sharing
#[derive(Debug, Clone)]
pub struct RecordedItem<T>
{
    /// The sequence number of this item in the recording
    pub seq: u64,
    /// The instant when this item was recorded
    pub timestamp: Instant,
    /// The duration since the previous item
    pub delta: Duration,
    /// The actual data item
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
                .unwrap_or_else(|| now.duration_since(*this.start_timestamp));

            this.recording.push(RecordedItem {
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
    /// Creates a new unbounded recording.
    pub fn new() -> Self {
        Recording {
            items: Arc::new(Mutex::new(Vec::new())),
            capacity: None,
        }
    }

    /// Creates a new recording with a maximum capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Recording {
            items: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
            capacity: Some(capacity),
        }
    }

    /// Pushes a new item to the recording, respecting capacity limits.
    fn push(&self, item: RecordedItem<S>) {
        let mut items = self.items.lock();
        if let Some(cap) = self.capacity {
            if items.len() >= cap {
                items.remove(0);
            }
        }
        items.push(item);
    }

    /// Returns the item at the specified sequence number.
    pub fn peek_at(&self, seq: u64) -> Option<Arc<S>> {
        let recording = self.items.lock();
        recording
            .iter()
            .find(|item| item.seq == seq)
            .map(|item| Arc::clone(&item.data))
    }

    /// Returns items within the specified sequence range.
    pub fn peek_range(&self, start: u64, end: u64) -> Vec<Arc<S>> {
        let recording = self.items.lock();
        recording
            .iter()
            .filter(|item| item.seq >= start && item.seq <= end)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    /// Returns the last `n` items from the recording.
    pub fn peek_last(&self, n: usize) -> Vec<Arc<S>> {
        let recording = self.items.lock();
        let len = recording.len();
        let start = len.saturating_sub(n);
        recording
            .iter()
            .skip(start)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    /// Returns items recorded since the specified instant.
    pub fn peek_since(&self, since: Instant) -> Vec<Arc<S>> {
        let recording = self.items.lock();
        recording
            .iter()
            .filter(|item| item.timestamp >= since)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    /// Returns items recorded between the specified instants.
    pub fn peek_between(&self, start: Instant, end: Instant) -> Vec<Arc<S>> {
        let recording = self.items.lock();
        recording
            .iter()
            .filter(|item| item.timestamp >= start && item.timestamp <= end)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    /// Returns items from the last specified duration.
    pub fn peek_last_duration(&self, duration: Duration) -> Vec<Arc<S>> {
        let recording = self.items.lock();
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

    /// Returns a snapshot of all recorded items.
    pub fn events(&self) -> Vec<RecordedItem<S>>
    where
        S: Clone,
    {
        self.items.lock().clone()
    }
}

impl<S: Stream> RecordedStream<S> {
    /// Returns a clone of the recording.
    ///
    /// Requires S::Item: Clone because Recording derives Clone
    pub fn recording(&self) -> Recording<S::Item>
    where
        S::Item: Clone,
    {
        self.recording.clone()
    }

    /// Replays all recorded items with their original timing.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay(&self) -> impl Stream<Item = Arc<S::Item>>
    where
        S::Item: Clone,
    {
        let items: Vec<_> = self.recording.items.lock().clone();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }

    /// Replays items starting from the specified sequence number.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_from(&self, start_seq: u64) -> impl Stream<Item = Arc<S::Item>>
    where
        S::Item: Clone,
    {
        let items: Vec<_> = self
            .recording
            .items
            .lock()
            .iter()
            .skip_while(|i| i.seq < start_seq)
            .cloned()
            .collect();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }

    /// Replays items recorded since the specified instant.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_since(&self, since: Instant) -> impl Stream<Item = Arc<S::Item>>
    where
        S::Item: Clone,
    {
        let items: Vec<_> = self
            .recording
            .items
            .lock()
            .iter()
            .skip_while(|i| i.timestamp < since)
            .cloned()
            .collect();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }

    /// Replays items within the specified sequence range.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_range(&self, start: u64, end: u64) -> impl Stream<Item = Arc<S::Item>>
    where
        S::Item: Clone,
    {
        let items: Vec<_> = self
            .recording
            .items
            .lock()
            .iter()
            .filter(|i| i.seq >= start && i.seq <= end)
            .cloned()
            .collect();

        futures::stream::iter(items).then(|item| async move {
            sleep(item.delta).await;
            Arc::clone(&item.data)
        })
    }

    /// Replays all recorded items with adjusted speed.
    ///
    /// # Arguments
    /// * `speed` - Playback speed multiplier (e.g., 2.0 for 2x speed, 0.5 for half speed)
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_with_speed(&self, speed: f64) -> impl Stream<Item = Arc<S::Item>>
    where
        S::Item: Clone,
    {
        let items: Vec<_> = self.recording.items.lock().clone();

        futures::stream::iter(items).then(move |item| async move {
            let adjusted_duration = if speed > 0.0 {
                Duration::from_secs_f64(item.delta.as_secs_f64() / speed)
            } else {
                Duration::ZERO
            };
            sleep(adjusted_duration).await;
            Arc::clone(&item.data)
        })
    }
}

/// Creates a `RecordedStream` that records all items from the underlying stream.
pub fn record<S: Stream<Item = T>, T: Clone>(s: S) -> RecordedStream<S> {
    let now = Instant::now();
    RecordedStream {
        inner: s,
        recording: Recording::new(),
        seq: 0,
        last_timestamp: None,
        start_timestamp: now,
    }
}

/// Creates a `RecordedStream` with bounded memory that keeps only the last `capacity` items.
pub fn record_with_capacity<S: Stream<Item = T>, T: Clone>(s: S, capacity: usize) -> RecordedStream<S> {
    let now = Instant::now();
    RecordedStream {
        inner: s,
        recording: Recording::with_capacity(capacity),
        seq: 0,
        last_timestamp: None,
        start_timestamp: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant as TokioInstant;

    #[tokio::test]
    async fn test_replay_timing_matches_original() {
        // Create a stream with known delays between items
        let stream = futures::stream::iter(vec![1, 2, 3])
            .then(|x| async move {
                if x > 1 {
                    sleep(Duration::from_millis(50)).await;
                }
                x
            });

        let recorded = record(stream);
        tokio::pin!(recorded);

        // Record the stream
        while recorded.next().await.is_some() {}

        // Get the recorded events to check deltas
        let events = recorded.recording().events();

        // Replay and verify timing
        let replay_stream = recorded.replay();
        tokio::pin!(replay_stream);

        let start = TokioInstant::now();
        let mut count = 0;
        let mut timings = Vec::new();

        while replay_stream.next().await.is_some() {
            timings.push(start.elapsed());
            count += 1;
        }

        assert_eq!(count, 3);

        // The first item's delta represents time from recording start
        // Since we consumed immediately, it should be very small
        assert!(events[0].delta < Duration::from_millis(10), "First item consumed quickly after record start");

        // Subsequent items should have delays as specified
        if events.len() > 1 && events[1].delta > Duration::ZERO {
            assert!(timings[1] >= Duration::from_millis(40), "Second item should have delay");
        }
    }

    #[test]
    fn peek_last_duration_returns_chronological_order() {
        let recording = Recording::new();
        let base = Instant::now();

        {
            let mut inner = recording.items.lock();
            inner.push(RecordedItem {
                seq: 0,
                timestamp: base,
                delta: Duration::ZERO,
                data: Arc::new(1),
            });

            inner.push(RecordedItem {
                seq: 1,
                timestamp: base + Duration::from_secs(1),
                delta: Duration::from_secs(1),
                data: Arc::new(2),
            });

            inner.push(RecordedItem {
                seq: 2,
                timestamp: base + Duration::from_secs(3),
                delta: Duration::from_secs(2),
                data: Arc::new(3),
            });
        }

        let values: Vec<_> = recording
            .peek_last_duration(Duration::from_secs(2))
            .into_iter()
            .map(|item| *item)
            .collect();

        assert_eq!(values, vec![2, 3]);
    }

    #[tokio::test]
    async fn test_first_event_respects_initial_delay() {
        let stream = futures::stream::iter(vec![1, 2, 3])
            .then(|x| async move {
                if x == 1 {
                    // First item has a delay
                    sleep(Duration::from_millis(50)).await;
                }
                x
            });

        let recorded = record(stream);
        tokio::pin!(recorded);

        // Consume the stream
        let mut items = Vec::new();
        while let Some(item) = recorded.next().await {
            items.push(item);
        }

        // Check the deltas in the recording
        let events = recorded.recording().events();
        // First event's delta should reflect the time from start to first item
        assert!(events[0].delta >= Duration::from_millis(40), "First event should have initial delay");
        // Subsequent events should have small or zero deltas since they come immediately after
        assert!(events[1].delta < Duration::from_millis(10), "Second event should come quickly");
        assert!(events[2].delta < Duration::from_millis(10), "Third event should come quickly");
    }

    #[tokio::test]
    async fn test_bounded_capacity() {
        let stream = futures::stream::iter(vec![1, 2, 3, 4, 5]);
        let recorded = record_with_capacity(stream, 3);
        tokio::pin!(recorded);

        // Consume the stream
        while let Some(_) = recorded.next().await {}

        let events = recorded.recording().events();
        assert_eq!(events.len(), 3, "Should only keep last 3 items");
        assert_eq!(*events[0].data, 3);
        assert_eq!(*events[1].data, 4);
        assert_eq!(*events[2].data, 5);
    }

    #[tokio::test]
    async fn test_replay_with_speed() {
        use tokio::time::Instant as TokioInstant;

        let stream = futures::stream::iter(vec![1, 2, 3])
            .then(|x| async move {
                sleep(Duration::from_millis(100)).await;
                x
            });

        let recorded = record(stream);
        tokio::pin!(recorded);

        // Record the stream
        while let Some(_) = recorded.next().await {}

        // Test 2x speed replay
        let replay_stream = recorded.replay_with_speed(2.0);
        tokio::pin!(replay_stream);

        let start = TokioInstant::now();
        let mut count = 0;
        while let Some(_) = replay_stream.next().await {
            count += 1;
        }
        let elapsed = start.elapsed();

        assert_eq!(count, 3);
        // Should take roughly half the time (with some tolerance)
        assert!(elapsed < Duration::from_millis(250), "2x speed should be faster");
    }

    #[tokio::test]
    async fn test_peek_range() {
        let stream = futures::stream::iter(vec![10, 20, 30, 40, 50]);
        let recorded = record(stream);
        tokio::pin!(recorded);

        while let Some(_) = recorded.next().await {}

        let recording = recorded.recording();
        let range = recording.peek_range(1, 3);
        assert_eq!(range.len(), 3);
        assert_eq!(*range[0], 20);
        assert_eq!(*range[1], 30);
        assert_eq!(*range[2], 40);
    }

    #[tokio::test]
    async fn test_peek_last() {
        let stream = futures::stream::iter(vec![1, 2, 3, 4, 5]);
        let recorded = record(stream);
        tokio::pin!(recorded);

        while let Some(_) = recorded.next().await {}

        let recording = recorded.recording();
        let last_3 = recording.peek_last(3);
        assert_eq!(last_3.len(), 3);
        assert_eq!(*last_3[0], 3);
        assert_eq!(*last_3[1], 4);
        assert_eq!(*last_3[2], 5);
    }

    #[tokio::test]
    async fn test_replay_from_seq() {
        let stream = futures::stream::iter(vec![10, 20, 30, 40]);
        let recorded = record(stream);
        tokio::pin!(recorded);

        while let Some(_) = recorded.next().await {}

        let replay_stream = recorded.replay_from(2);
        tokio::pin!(replay_stream);

        let mut replayed = Vec::new();
        while let Some(item) = replay_stream.next().await {
            replayed.push(*item);
        }

        assert_eq!(replayed, vec![30, 40]);
    }

    #[tokio::test]
    async fn test_events_method() {
        let stream = futures::stream::iter(vec![100, 200, 300]);
        let recorded = record(stream);
        tokio::pin!(recorded);

        while let Some(_) = recorded.next().await {}

        let events = recorded.recording().events();
        assert_eq!(events.len(), 3);
        assert_eq!(*events[0].data, 100);
        assert_eq!(*events[1].data, 200);
        assert_eq!(*events[2].data, 300);
        assert_eq!(events[0].seq, 0);
        assert_eq!(events[1].seq, 1);
        assert_eq!(events[2].seq, 2);
    }

    #[tokio::test]
    async fn test_peek_since_timestamp() {
        let stream = futures::stream::iter(vec![1, 2, 3])
            .then(|x| async move {
                sleep(Duration::from_millis(50)).await;
                x
            });

        let recorded = record(stream);
        tokio::pin!(recorded);

        let mut mid_timestamp = None;
        let mut count = 0;
        while let Some(_) = recorded.next().await {
            count += 1;
            if count == 2 {
                mid_timestamp = Some(Instant::now());
            }
        }

        let recording = recorded.recording();
        let since_mid = recording.peek_since(mid_timestamp.unwrap());

        // Should get items 2 and 3 (or just 3, depending on exact timing)
        assert!(since_mid.len() >= 1 && since_mid.len() <= 2);
        assert_eq!(**since_mid.last().unwrap(), 3);
    }
}
