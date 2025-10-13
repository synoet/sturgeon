# sturgeon

Record async streams with timing information and replay them deterministically.

## What it does

Wraps any `Stream`, records each item with a timestamp, then lets you replay the recording with the same timing characteristics. Think of it as a DVR for async streams.

```rust
use sturgeon::record;
use futures::StreamExt;

let mut recorded = record(your_stream);
while let Some(item) = recorded.next().await {
    // Stream passes through normally
}

// Later: replay with original timing
let replay = recorded.recording().replay();
tokio::pin!(replay);
while let Some(item) = replay.next().await {
    // Items arrive with same delays as original
}
```

## Why use it

**Deterministic tests for timing-sensitive code.** If you're testing rate limiters, batchers, debouncing, backpressure, or any logic that depends on *when* events arrive - not just *what* arrives - you need reproducible timing. Sturgeon records real timing patterns once, then replays them identically in tests.

Without sturgeon: sprinkle `tokio::time::sleep` throughout tests, hope the timing works out, deal with flaky CI.

With sturgeon: record production traffic or realistic patterns once, replay *perfectly* every time.

## Features

- **Pass-through recording** - items flow through unchanged, timing captured alongside
- **Multiple replay modes** - original timing, speed-adjusted (2x, 0.5x), or instant
- **Persistence** - save/load recordings with serde
- **Bounded recording** - `record_with_capacity(stream, n)` keeps only last n items
- **Partial replay** - replay from sequence number, time range, or slice

## Writing tests

### Record once, replay many times

```rust
#[tokio::test]
async fn create_recording() {
    let stream = /* your stream */;
    let mut recorded = record(stream);
    while recorded.next().await.is_some() {}
    
    recorded.recording().save("traffic.bin").await.unwrap();
}

// Test with recorded timing
#[tokio::test]
async fn handles_burst_traffic() {
    let recording: Recording<Event> = Recording::load("traffic.bin").await.unwrap();
    let replay = recording.replay();
    tokio::pin!(replay);
    
    let mut handler = YourHandler::new();
    while let Some(event) = replay.next().await {
        handler.process(event).await;
    }
    
    assert!(handler.within_limits());
}
```

### Fast functional tests

```rust
#[tokio::test]
async fn processes_all_events() {
    let recording: Recording<Event> = Recording::load("traffic.bin").await.unwrap();
    
    // Instant replay - no timing delays
    let results: Vec<_> = recording
        .replay_immediate()
        .then(|event| process(event))
        .collect()
        .await;
    
    assert_eq!(results.len(), expected);
}
```

### Speed control

```rust
// 2x speed for faster tests
let fast = recording.replay_with_speed(Speed::new(2.0)?);

// Slow motion for debugging
let slow = recording.replay_with_speed(Speed::new(0.5)?);
```
