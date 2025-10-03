# sturgeon

Record and replay streams with timing.

## Example

```rust
use futures::StreamExt;

// record a stream
let mut recorded = sturgeon::record(websocket_stream);
while let Some(msg) = recorded.next().await {
    handle(msg).await;
}

// replay returns a stream with original timing
let mut replay = recorded.replay();
while let Some(msg) = replay.next().await {}

// replay from specific timestamp
let mut replay = recorded.replay_since(Instant::now() - Duration::from_secs(10));
```
