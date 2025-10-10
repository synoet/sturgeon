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
//! // Create a stream and wrap it with recording
//! let stream = stream::iter(vec![1, 2, 3]);
//! let mut recorded = record(stream);
//!
//! // Consume the stream (items are recorded as they pass through)
//! while let Some(item) = recorded.next().await {
//!     println!("Got: {}", item);
//! }
//!
//! // Replay the recorded items with original timing
//! let replay = recorded.replay();
//! tokio::pin!(replay);
//! while let Some(item) = replay.next().await {
//!     println!("Replayed: {:?}", item);
//! }
//! # }
//! ```
//!
//! ## Bounded Recording
//!
//! ```no_run
//! use sturgeon::record_with_capacity;
//! use futures::stream;
//!
//! # async fn example() {
//! // Only keep the last 100 items in memory
//! let stream = stream::iter(vec![1, 2, 3, 4, 5]);
//! let recorded = record_with_capacity(stream, 100);
//! # }
//! ```
//!
//! ## Speed-Controlled Replay
//!
//! ```no_run
//! use sturgeon::record;
//! use futures::{stream, StreamExt};
//!
//! # async fn example() {
//! let stream = stream::iter(vec![1, 2, 3]);
//! let mut recorded = record(stream);
//!
//! // Consume stream...
//! while let Some(_) = recorded.next().await {}
//!
//! // Replay at 2x speed
//! let fast_replay = recorded.replay_with_speed(2.0).unwrap();
//!
//! // Replay at half speed
//! let slow_replay = recorded.replay_with_speed(0.5).unwrap();
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

impl<S: Clone> Default for Recording<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Clone> Recording<S> {
    /// Creates a new unbounded recording.
    pub fn new() -> Self {
        Recording {
            items: Arc::new(Mutex::new(VecDeque::new())),
            capacity: None,
        }
    }

    /// Creates a new recording with a maximum capacity.
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
    pub fn peek_since(&self, since: SystemTime) -> Vec<Arc<S>> {
        let recording = self.items.lock();
        recording
            .iter()
            .filter(|item| item.timestamp >= since)
            .map(|item| Arc::clone(&item.data))
            .collect()
    }

    /// Returns items recorded between the specified instants.
    pub fn peek_between(&self, start: SystemTime, end: SystemTime) -> Vec<Arc<S>> {
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

        let first_timestamp = recording.front().unwrap().timestamp;
        let last_timestamp = recording.back().unwrap().timestamp;
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

    fn replay_items(
        &self,
        items: Vec<RecordedItem<S>>,
        speed: Option<f64>,
    ) -> impl Stream<Item = Arc<S>> {
        let start = tokio::time::Instant::now();
        let speed = speed.unwrap_or(1.0);

        let mut cumulative = Duration::ZERO;
        let timed_items: Vec<_> = items
            .into_iter()
            .map(|item| {
                let adjusted_delta = Duration::from_secs_f64(item.delta.as_secs_f64() / speed);
                cumulative += adjusted_delta;
                (start + cumulative, item)
            })
            .collect();

        futures::stream::iter(timed_items).then(|(target, item)| async move {
            sleep_until(target).await;
            Arc::clone(&item.data)
        })
    }

    /// Replays all recorded items with their original timing.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay(&self) -> impl Stream<Item = Arc<S>> {
        let items: Vec<_> = self.items.lock().clone().into_iter().collect();

        self.replay_items(items, None)
    }

    /// Replays items starting from the specified sequence number.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_from(&self, start_seq: u64) -> impl Stream<Item = Arc<S>> {
        let items: Vec<_> = self
            .items
            .lock()
            .iter()
            .skip_while(|i| i.seq < start_seq)
            .cloned()
            .collect();

        self.replay_items(items, None)
    }

    /// Replays items recorded since the specified instant.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_since(&self, since: SystemTime) -> impl Stream<Item = Arc<S>> {
        let items: Vec<_> = self
            .items
            .lock()
            .iter()
            .skip_while(|i| i.timestamp < since)
            .cloned()
            .collect();

        self.replay_items(items, None)
    }

    /// Replays items within the specified sequence range.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_range(&self, start: u64, end: u64) -> impl Stream<Item = Arc<S>> {
        let items: Vec<_> = self
            .items
            .lock()
            .iter()
            .filter(|i| i.seq >= start && i.seq <= end)
            .cloned()
            .collect();

        self.replay_items(items, None)
    }

    /// Replays all recorded items with adjusted speed.
    ///
    /// # Arguments
    /// * `speed` - Playback speed multiplier (e.g., 2.0 for 2x speed, 0.5 for half speed)
    ///
    /// # Errors
    /// Returns [`Error::InvalidSpeed`] if speed is not positive.
    ///
    /// Requires S::Item: Clone to clone the recorded items for replay
    pub fn replay_with_speed(&self, speed: f64) -> SturgeonResult<impl Stream<Item = Arc<S>>> {
        if speed <= 0.0 {
            return Err(Error::InvalidSpeed(speed));
        }

        let items: Vec<_> = self.items.lock().iter().cloned().collect();

        Ok(self.replay_items(items, Some(speed)))
    }

    pub fn events(&self) -> Vec<RecordedItem<S>> {
        self.items.lock().iter().cloned().collect()
    }

    pub async fn save(&self, path: impl AsRef<Path>) -> io::Result<()>
    where
        S: Serialize,
    {
        let bytes = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        tokio::fs::write(path, bytes).await
    }

    pub async fn load(path: impl AsRef<Path>) -> io::Result<Self>
    where
        S: for<'de> Deserialize<'de>,
    {
        let bytes = tokio::fs::read(path).await?;
        let (rec, _) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(rec)
    }
}

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
    /// Returns a clone of the recording.
    ///
    /// Requires S::Item: Clone because Recording derives Clone
    pub fn recording(&self) -> Recording<S::Item> {
        self.recording.clone()
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
