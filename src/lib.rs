//! A library for recording and replaying asynchronous streams with timing information.
//!
//! This crate provides utilities to record items from any [`Stream`] along with their
//! timing information, and replay them later with the same timing characteristics.
//!
//! # Examples
//!
//! ## Basic Recording and Replay
//!
//! ```no_run
//! use sturgeon::record;
//! use futures::{stream, StreamExt};
//!
//! # async fn example() {
//! let stream = stream::iter(vec![1, 2, 3]);
//! let mut recorded = record(stream);
//!
//! while let Some(item) = recorded.next().await {
//!     println!("Got: {}", item);
//! }
//!
//! // Get the recording and replay with original timing
//! let recording = recorded.recording();
//! let replay = recording.replay();
//! tokio::pin!(replay);
//! while let Some(item) = replay.next().await {
//!     println!("Replayed: {}", item);
//! }
//! # }
//! ```
//!
//! ## Speed-Controlled Replay
//!
//! ```no_run
//! use sturgeon::{record, Speed};
//! use futures::{stream, StreamExt};
//!
//! # async fn example() {
//! let stream = stream::iter(vec![1, 2, 3]);
//! let mut recorded = record(stream);
//!
//! while recorded.next().await.is_some() {}
//!
//! let recording = recorded.recording();
//!
//! // Replay at 2x speed
//! let fast = recording.replay_with_speed(Speed::new(2.0).unwrap());
//!
//! // Replay at half speed
//! let slow = recording.replay_with_speed(Speed::new(0.5).unwrap());
//! # }
//! ```
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fmt,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime},
};
use tokio::io;
use tokio::time::sleep_until;

/// Errors that can occur when working with recorded streams.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The recording is empty
    EmptyRecording,
    /// Invalid speed parameter (must be positive)
    InvalidSpeed(f64),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::EmptyRecording => write!(f, "The recording is empty"),
            Error::InvalidSpeed(speed) => write!(f, "Invalid speed {}: must be positive", speed),
        }
    }
}

impl std::error::Error for Error {}

/// Result type for sturgeon operations.
pub type SturgeonResult<T> = std::result::Result<T, Error>;

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
    seq: u64,
    /// The timestamp of the last recorded item
    last_timestamp: Option<Instant>,
    /// The timestamp when recording started
    start_timestamp: Instant,
}

/// A thread-safe recording of stream items with optional capacity limits.
///
/// `Recording` stores items with their timing information and can be shared across threads.
/// It supports bounded or unbounded storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recording<S> {
    items: Arc<Mutex<VecDeque<RecordedItem<S>>>>,
    capacity: Option<usize>,
}

/// A single recorded item with timing information.
///
/// Each item contains:
/// - `seq`: A sequence number for ordering
/// - `timestamp`: The instant when the item was recorded
/// - `delta`: The duration since the previous item
/// - `data`: The actual item data wrapped in Arc for efficient sharing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedItem<T> {
    /// The sequence number of this item in the recording
    seq: u64,
    /// The instant when this item was recorded
    timestamp: SystemTime,
    /// The duration since the previous item
    delta: Duration,
    /// The actual data item
    data: Arc<T>,
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

            // Calculate delta: time since last item or since recording started
            let delta = this
                .last_timestamp
                .map(|last| now.duration_since(last))
                .unwrap_or_else(|| now.duration_since(*this.start_timestamp));

            // Record the item with its timing information
            this.recording.push(RecordedItem {
                seq: *this.seq,
                timestamp: SystemTime::now(),
                delta,
                data: Arc::new(item.clone()),
            });

            // Update state for next item
            *this.seq += 1;
            *this.last_timestamp = Some(now);
        }

        result
    }
}

impl<S> Default for Recording<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Recording<S> {
    /// Creates a new unbounded recording.
    pub fn new() -> Self {
        Recording {
            items: Arc::new(Mutex::new(VecDeque::new())),
            capacity: None,
        }
    }

    /// Creates a recording that keeps only the last `capacity` items.
    /// Older items are dropped as new ones arrive.
    pub fn with_capacity(capacity: usize) -> Self {
        Recording {
            items: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity: Some(capacity),
        }
    }

    /// Pushes a new item to the recording, respecting capacity limits.
    fn push(&self, item: RecordedItem<S>) {
        let mut items = self.items.lock();
        if let Some(cap) = self.capacity
            && items.len() >= cap
        {
            items.remove(0);
        }
        items.push_back(item);
    }

    /// Saves the recording to disk with timing information.
    pub async fn save(&self, path: impl AsRef<Path>) -> io::Result<()>
    where
        S: Serialize,
    {
        let bytes = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(std::io::Error::other)?;
        tokio::fs::write(path, bytes).await
    }

    /// Loads a recording from disk.
    pub async fn load(path: impl AsRef<Path>) -> io::Result<Self>
    where
        S: for<'de> Deserialize<'de>,
    {
        let bytes = tokio::fs::read(path).await?;
        let (rec, _) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
            .map_err(io::Error::other)?;
        Ok(rec)
    }
}
/// A positive playback speed multiplier
///
/// Use [`Speed::new`] to validate, and [`Speed::NORMAL`] for 1.0x playback speed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Speed(f64);

impl Speed {
    /// Creates a new playback speed.
    /// Returns [`Error::InvalidSpeed`] if speed is not positive.
    pub fn new(value: f64) -> SturgeonResult<Self> {
        if value <= 0.0 {
            Err(Error::InvalidSpeed(value))
        } else {
            Ok(Speed(value))
        }
    }

    /// Standard playback speed of 1.0x
    pub const NORMAL: Speed = Speed(1.0);

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

impl<S: Clone> Recording<S> {
    #[must_use = "streams do nothing unless polled"]
    fn replay_items(
        &self,
        items: Vec<RecordedItem<S>>,
        speed: Speed,
    ) -> impl Stream<Item = Arc<S>> {
        let start = tokio::time::Instant::now();

        let mut cumulative = Duration::ZERO;
        let timed_items: Vec<_> = items
            .into_iter()
            .map(|item| {
                let adjusted_delta =
                    Duration::from_secs_f64(item.delta.as_secs_f64() / speed.as_f64());
                cumulative += adjusted_delta;
                (start + cumulative, item)
            })
            .collect();

        futures::stream::iter(timed_items).then(|(target, item)| async move {
            sleep_until(target).await;
            Arc::clone(&item.data)
        })
    }

    /// Replays items with original timing delays between them.
    #[must_use = "streams do nothing unless polled"]
    pub fn replay(&self) -> impl Stream<Item = S> {
        let items: Vec<_> = self.items.lock().clone().into_iter().collect();

        self.replay_items(items, Speed::NORMAL)
            .map(|item| (*item).clone())
    }

    /// Replays items starting from sequence number `start_seq`.
    /// Timing between replayed items is preserved.
    #[must_use = "streams do nothing unless polled"]
    pub fn replay_from(&self, start_seq: u64) -> impl Stream<Item = S> {
        let items: Vec<_> = self
            .items
            .lock()
            .iter()
            .skip_while(|i| i.seq < start_seq)
            .cloned()
            .collect();

        self.replay_items(items, Speed::NORMAL)
            .map(|item| (*item).clone())
    }

    /// Replays items recorded after `since`.
    /// Timing between replayed items is preserved.
    #[must_use = "streams do nothing unless polled"]
    pub fn replay_since(&self, since: SystemTime) -> impl Stream<Item = S> {
        let items: Vec<_> = self
            .items
            .lock()
            .iter()
            .skip_while(|i| i.timestamp < since)
            .cloned()
            .collect();

        self.replay_items(items, Speed::NORMAL)
            .map(|item| (*item).clone())
    }

    /// Replays items within the sequence range `[start, end]` (inclusive).
    /// Timing between replayed items is preserved.
    #[must_use = "streams do nothing unless polled"]
    pub fn replay_range(&self, start: u64, end: u64) -> impl Stream<Item = S> {
        let items: Vec<_> = self
            .items
            .lock()
            .iter()
            .filter(|i| i.seq >= start && i.seq <= end)
            .cloned()
            .collect();

        self.replay_items(items, Speed::NORMAL)
            .map(|item| (*item).clone())
    }

    /// Replays with adjusted timing. Speed of 2.0 is twice as fast, 0.5 is half the speed.
    /// Relative timing between replayed items is preserved.
    #[must_use = "streams do nothing unless polled"]
    pub fn replay_with_speed(&self, speed: Speed) -> impl Stream<Item = S> {
        let items: Vec<_> = self.items.lock().iter().cloned().collect();

        self.replay_items(items, speed).map(|item| (*item).clone())
    }

    /// Replays items immediately without any delays.
    /// Useful for tests that don't care about timing.
    #[must_use = "streams do nothing unless polled"]
    pub fn replay_immediate(&self) -> impl Stream<Item = S> {
        let items: Vec<_> = self.items.lock().iter().cloned().collect();
        tokio_stream::iter(items).map(|item| (*item.data).clone())
    }
}

#[cfg(test)]
impl<S: PartialEq + std::fmt::Debug> Recording<S> {
    pub fn assert_count(&self, expected: usize) {
        assert_eq!(
            self.items.lock().len(),
            expected,
            "sequence length mismatch"
        );
    }

    pub fn assert_sequence(&self, expected: &[S]) {
        let recording = self.items.lock();
        let mismatches: Vec<(usize, &S, &S)> = recording
            .iter()
            .zip(expected)
            .enumerate()
            .filter_map(|(i, (rec, exp))| (*rec.data != *exp).then_some((i, &*(rec.data), exp)))
            .collect();

        assert!(
            mismatches.is_empty(),
            "sequence mismatch at indices {mismatches:?}",
        )
    }

    pub fn assert_timing(&self, min: Duration, max: Duration) {
        let recording = self.items.lock();
        let violations: Vec<(usize, Duration, Duration)> = recording
            .iter()
            .enumerate()
            .filter_map(|(i, rec)| {
                (rec.delta < min || rec.delta > max).then_some((i, rec.delta, min))
            })
            .collect();

        assert!(
            violations.is_empty(),
            "timing violations at indices {violations:?}",
        )
    }
}

impl<S: Stream> RecordedStream<S>
where
    S::Item: Clone,
{
    /// Returns the recording captured so far.
    /// Recording continues as the stream is consumed.
    pub fn recording(&self) -> Recording<S::Item> {
        self.recording.clone()
    }
}

/// Wraps a stream to record all items with timing information.
/// The stream passes through - items are cloned for recording.
///
/// # Example
/// ```
/// use sturgeon::record;
/// use futures::{stream, StreamExt};
/// # async fn example() {
/// let stream = stream::iter(vec![1, 2, 3]);
/// let mut recorded = record(stream);
/// while let Some(item) = recorded.next().await {
///     // Process items normally
/// }
/// // Later: replay with timing preserved
/// let recording = recorded.recording();
/// let replay = recording.replay();
/// # }
/// ```
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

/// Like [`record`] but only keeps the last `capacity` items in memory.
/// Useful for long-running streams where full history isn't needed.
pub fn record_with_capacity<S: Stream<Item = T>, T: Clone>(
    s: S,
    capacity: usize,
) -> RecordedStream<S> {
    let now = Instant::now();
    RecordedStream {
        inner: s,
        recording: Recording::with_capacity(capacity),
        seq: 0,
        last_timestamp: None,
        start_timestamp: now,
    }
}
