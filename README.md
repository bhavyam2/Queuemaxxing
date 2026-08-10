# Queuemaxxing

A durable HTTP message queue written from scratch in Go, using only the standard library. It
owns its own storage: a custom append-only log on local disk, one file per queue. There is no
Redis, Postgres, SQS, RabbitMQ, or Kafka behind it.

Each queue is created as either FIFO or LIFO. Priority and delay are per-message and always
available, so one design covers FIFO, LIFO, priority FIFO, priority LIFO, and any of those
combined with a per-message delay. Delivery is at-least-once: a received message stays in the
queue until it is acknowledged, and a consumer that dies gets its message redelivered.

## Additional questions

### How do you handle replay messages?

I use at-least-once delivery. If a consumer receives a message but doesn't ACK it before its visibility timeout expires, the queue puts the message back into the ready queue so another consumer can process it. If the application crashes, unacknowledged messages are also recovered from the persistent log and made available again.

### How would you refactor your queue into a Pub/Sub?
I would refactor the queue around the append-only log that I already have. Instead of deleting a message globally once a consumer ACKs it, I'd introduce topics and subscriptions. Each subscription would maintain its own offset into the durable log. When a consumer acknowledges a message, we'd advance that subscription's offset rather than deleting the message globally. This allows multiple independent consumers to receive the same message.

One thing worth clarifying: an ACK today doesn't actually erase anything from disk. It appends an ACK record to the log, which marks the message as done, and the message is only physically dropped later when the log gets compacted. So the log already holds on to messages after they've been acknowledged, which is most of what Pub/Sub needs. The refactor is mainly a change in meaning: an ACK would stop meaning "this message is finished for everyone" and start meaning "this subscription has moved past this message", and compaction would only drop a message once every subscription's offset had passed it.

### If you had more time, what other features would you add?

If I had more time, I'd focus on features that improve reliability and operation rather than just adding more queue functionality.

I'd add dead-letter queues. If a message fails repeatedly, I don't want it retrying forever and potentially blocking useful work. After some configurable number of attempts, I'd move it to a dead-letter queue so it can be inspected and replayed later. I'd also add message TTLs. Some messages become useless after a certain amount of time, so I'd allow producers to specify an expiration time and automatically discard expired messages. Third, I'd add metrics and observability. I'd expose things like queue depth, processing latency, number of retries, failed messages, and throughput. This would make the queue much easier to operate and debug. Finally, I'd add long polling and batch operations. Instead of consumers repeatedly polling an empty queue, they could wait for messages to arrive, and producers/consumers could process messages in batches to improve throughput.

### Why would users choose your queue over incumbents like Amazon SQS, RabbitMQ or Apache Pulsar?

The main advantage of this app is flexibility and simplicity. I designed the queue so that ordering semantics can change easily; for example, you can have priority + LIFO + delay without needing a separate messaging system or infrastructure. The storage is also built directly into the application, so you don't need to deploy and operate Redis, a database, or another queue just to get durable messaging. So I think users would choose it for smaller or specialized workloads where they want a lightweight queue with custom behavior and full control over how it works. If I needed a globally distributed, highly available production messaging system, I would then choose something like SQS, RabbitMQ, or Pulsar instead.

## Building and running

Requires Go 1.22 or newer. There are no dependencies to fetch.

```
go run ./cmd/server
```

The server listens on `:8080` and stores logs in `./data`. Flags:

```
--addr               HTTP listen address (default ":8080")
--data-dir           directory holding one append-only log per queue (default "data")
--compact-threshold  log record count that triggers compaction (default 10000)
```

Open `http://localhost:8080/` for the demo web UI. It creates and selects queues, sends with
priority and delay, receives, acks and nacks, and polls the stats endpoint once a second so the
ready, delayed, and in-flight counts update as you work.

### Docker

```
docker build -t queuemaxxing .
docker run -p 8080:8080 -v queuemaxxing-data:/data queuemaxxing
```

The image is built with `CGO_ENABLED=0` and runs from `scratch`, so it contains only the static
binary. The web UI is compiled in with `embed.FS` rather than read from disk. `/data` is a
volume; that is the entire persistent state, and it can be copied or backed up as a directory.

### A worked session

```
# Create a FIFO queue.
curl -X POST localhost:8080/queues -d '{"name":"orders","ordering":"fifo"}'

# Send four messages. D has the highest priority; C is not available for an hour.
curl -X POST localhost:8080/queues/orders/messages -d '{"body":{"label":"A"}}'
curl -X POST localhost:8080/queues/orders/messages -d '{"body":{"label":"B"}}'
curl -X POST localhost:8080/queues/orders/messages -d '{"body":{"label":"C"},"delay_seconds":3600}'
curl -X POST localhost:8080/queues/orders/messages -d '{"body":{"label":"D"},"priority":9}'

# Receive. D comes first because priority outranks arrival order.
curl -X POST localhost:8080/queues/orders/receive -d '{}'
# {"message_id":"cec20e65-...","body":{"label":"D"},"priority":9,"sequence":4,
#  "receive_count":1,"receipt":"7351eccc...","visibility_deadline":"..."}

# Acknowledge it with the receipt from that delivery.
curl -X POST localhost:8080/queues/orders/ack -d '{"receipt":"7351eccc..."}'

# Return a message to the queue, optionally after a retry delay.
curl -X POST localhost:8080/queues/orders/nack -d '{"receipt":"...","delay_seconds":30}'

# Long poll: park for up to 20 seconds waiting for work.
curl -X POST localhost:8080/queues/orders/receive -d '{"wait_seconds":20}'

# Inspect.
curl localhost:8080/queues
curl localhost:8080/queues/orders
curl localhost:8080/queues/orders/stats
# {"ready":0,"delayed":2,"in_flight":1,"total_enqueued":4,"total_acked":1}

curl -X DELETE localhost:8080/queues/orders
```

## API reference

All requests and responses are JSON. Endpoints whose fields are all optional accept an empty
body.

`POST /queues` - `{"name": string, "ordering": "fifo"|"lifo"}`. Returns 201 with the queue's
configuration and stats, 400 for a bad name or unknown ordering, 409 if the name is taken.
Names must match `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`, because the name is also the log's file name.

`GET /queues` - every queue with its configuration and current stats, sorted by name.

`GET /queues/{name}` - one queue's configuration and stats. 404 if unknown.

`DELETE /queues/{name}` - 204. Closes the queue, releases any consumer blocked in a long poll
with a 404, discards its in-flight messages, and removes its log file.

`POST /queues/{name}/messages` - `{"body": <any JSON>, "priority": int?, "delay_seconds": int?}`.
Returns 201 with `{"message_id": ...}`. `priority` defaults to 0 and may be negative;
`delay_seconds` defaults to 0 and is capped at 7 days. The body is required and is stored and
returned byte for byte. 413 if the request exceeds 8 MiB.

`POST /queues/{name}/receive` - `{"visibility_timeout_seconds": int?, "wait_seconds": int?}`.
Returns 200 with the delivery, or 204 when nothing is available. `visibility_timeout_seconds`
defaults to 30 and is capped at 12 hours. `wait_seconds` defaults to 0, meaning return
immediately, and is capped at 60. A delivery carries `message_id`, `body`, `priority`,
`sequence`, `receive_count`, `created_at`, `available_at`, `receipt`, and
`visibility_deadline`.

`POST /queues/{name}/ack` - `{"receipt": string}`. 200 on success. 404 if the receipt is unknown
or no longer current, which is what a consumer sees when its visibility window already expired.

`POST /queues/{name}/nack` - `{"receipt": string, "delay_seconds": int?}`. 200. Without a delay
the message becomes available immediately; with one it moves to the delayed set.

`GET /queues/{name}/stats` - `{"ready", "delayed", "in_flight", "total_enqueued", "total_acked"}`.

`GET /health` - liveness.

Errors are `{"error": "..."}`. 400 for malformed JSON or out-of-range parameters, 404 for an
unknown queue or receipt, 405 for the wrong method, 409 for a duplicate queue name, 413 for an
oversized body, 500 for an unexpected storage failure, and 503 for a queue whose log has been
marked unwritable.

## Design and tradeoffs

### One heap, one comparator

Ready messages live in a binary heap from `container/heap`. Priority decides first; ties are
broken by sequence number, ascending for FIFO and descending for LIFO. A single comparator
handles both modes, parameterised by the queue's ordering at construction:

```
A priority=10 seq=1   B priority=10 seq=2   C priority=5 seq=3   D priority=10 seq=4

Priority FIFO: A, B, D, C
Priority LIFO: D, B, A, C
```

Selection is O(log n) and nothing is ever sorted on receive.

### Sequence numbers, not timestamps

Ordering within a priority uses a per-queue counter assigned at enqueue, not a clock reading.
Two messages sent in the same nanosecond still need a total order, and that order has to be
identical after a restart. The sequence number is written into the log, so it survives; a
timestamp would not give a stable tie-break at all.

A sequence number is assigned once and never reassigned. This has a visible consequence worth
stating: under LIFO, a message that was delayed or nacked sorts by when it was originally sent,
not by when it became available. A message sent early with a long delay will be delivered
*after* messages sent later, because its sequence number is lower. The alternative, renumbering
on promotion, would make recovery non-deterministic, since replaying the log cannot reconstruct
the order in which promotions happened.

### Delayed messages and the scheduler

Each queue has three heaps: ready (the comparator above), delayed (a min-heap on `available_at`),
and expiry (a min-heap on the visibility deadline of in-flight messages). Delayed messages never
sit in the ready heap, so a delayed message cannot be delivered early.

One goroutine per queue handles both delay promotion and visibility expiry, because they are the
same problem: act at time T, and wake earlier if an earlier T appears. It sleeps on a
`time.Timer` armed for the earlier of the two heap minima, promotes everything that has come
due, signals waiting consumers, and re-arms. There is no polling loop anywhere.

The timer is owned exclusively by that goroutine. Producers and consumers never call `Reset` on
it; when an enqueue or a receive introduces an earlier deadline, it does a non-blocking send on a
capacity-one channel and the scheduler recomputes. This is behaviourally the same as resetting
the timer from the caller, and it avoids the well-known hazard of resetting a timer that has
already fired but not been drained. The kick is skipped entirely when the new deadline is not
the earliest, so a busy queue does not wake the scheduler on every operation.

### Waiting consumers

Each queue has one `sync.Mutex` and a `sync.Cond` paired with it. A consumer that asks for
`wait_seconds` above zero re-checks the ready heap and then blocks in `Cond.Wait`. Enqueue,
nack, and the scheduler all broadcast.

`sync.Cond` has no timed wait and cannot observe a cancelled request context, so both are turned
into broadcasts: a `time.AfterFunc` fires at the caller's deadline, and a small goroutine
watches the request context. Both acquire the queue mutex before broadcasting. That detail
matters: a broadcast issued without the lock can land between a waiter's predicate check and its
`Wait`, and that waiter would then sleep past its own deadline.

The code uses `Broadcast`, never `Signal`. Waiters are not interchangeable, and a signalled
consumer that loses the race for the message would consume the wakeup and leave a ready message
with every other consumer asleep. The cost is a thundering herd proportional to the number of
long pollers on one queue, which is accepted here in exchange for removing a whole class of
missed-wakeup bug.

### Concurrency and lock ordering

All mutable per-queue state is guarded by that one mutex: both message heaps, the expiry heap,
the in-flight map, the sequence counter, the lifetime counters, and the log handle. Receive is
atomic under it, so selecting a message, marking it in flight, and minting its receipt cannot
interleave, and two consumers can never be handed the same message.

The registry has its own `sync.RWMutex`. **Manager lock before queue lock, never the reverse.**
Handlers resolve the queue under the registry lock, take a reference, release it, and then call
into the queue. A deleted queue is closed rather than invalidated, so a stale reference is safe
and simply reports that the queue is gone.

Finer-grained locking is a deliberate non-goal. Per-queue locking already shards contention
across queues, and the durability model below is the real throughput bound, not lock contention.

### The log format

One file per queue, `data/<name>.log`, all integers big-endian.

```
File header, 16 bytes, written once:
  0   8  magic "QMXLOG\0\0"
  8   4  uint32 format version
 12   4  uint32 reserved

Record frame:
  0   4  uint32 payload length N, where 1 <= N <= 8 MiB
  4   1  uint8  record type: 1 HEADER, 2 ENQUEUE, 3 ACK, 4 NACK
  5   N  payload, JSON
 5+N  4  uint32 CRC-32C over bytes [0, 5+N), so the length and type are covered too
```

JSON payloads inside binary frames keep records readable when something goes wrong, while the
length prefix and checksum do the integrity work JSON cannot. Records are perhaps twice the size
of a packed binary encoding; compaction bounds the cost.

Three details are deliberate. The checksum covers the length field, so a corrupted length is
detected rather than silently steering the reader to the wrong offset. The length is validated
against the 8 MiB ceiling *before* any buffer is allocated, so corruption cannot induce a huge
allocation. Record type 0 is reserved invalid, so a zero-filled region, which is what a torn
write often leaves behind, is rejected instead of parsing as a legitimate empty frame.

There is no separate metadata file. The first record of every log is a HEADER carrying the
queue's name, ordering, creation time, the next sequence number, and the lifetime
`total_enqueued` and `total_acked` counters. Folding configuration into the log means one
crash-safe write path instead of two, and removes the question of what to do when a log and a
metadata file disagree. It is also the only sound place to keep the counters and the sequence
floor, which compaction would otherwise reset: a fully drained queue compacts to an almost empty
log, and without the header it would restart numbering at 1 and report zero messages ever sent.

### Durability: fsync before 2xx

Every state-changing operation serialises its record, appends it, calls `fsync`, and only then
returns success. A client holding a 2xx holds a durable fact. `ENQUEUE`, `ACK`, and `NACK` are
all written this way. Receive writes nothing at all, which is why receive latency is independent
of the disk.

Creating a queue also fsyncs the **data directory**, and so does every rename. `fsync` on a file
makes its contents durable but not its directory entry; without the directory fsync a newly
created queue can disappear entirely after a power loss even though its bytes were flushed.

The honest limit: on Linux, which is where the container runs, `fsync` is sufficient. On macOS,
`fsync` does not force the drive's own write cache to stable media, and `F_FULLFSYNC` would be
required for true power-loss durability there. For the guarantee this project actually claims,
surviving `kill -9`, `fsync` is sufficient on both, because the page cache outlives the process.

The cost is real and is the dominant performance characteristic. Measured on this project's own
test (`TestFsyncedAppendThroughput`, APFS on an Apple laptop): **247 fsynced appends per second**
per queue. Because the append happens with the queue mutex held, that is also the per-queue
write ceiling. Group commit, batching concurrent fsyncs into one flush, would raise it
substantially without weakening the guarantee, and is the first thing to reach for; it is
described but not implemented. Holding one lock across the flush was chosen because it makes the
log's byte order match operation order and leaves nothing subtle to reason about.

### Failed writes

A short write or a failed `fsync` is rolled back: the file is truncated to the offset recorded
before the append and the error is returned. The client never saw a 2xx, so discarding the
record is correct, and the log is never left with a partial frame in its middle that would
later be mistaken for corruption. A transient full disk therefore costs one failed request
rather than a permanently unreadable queue.

If the rollback itself fails, the log is marked unwritable. Every later write returns 503 and
names the file, rather than appending after bytes that could not be removed.

### Recovery

On startup the manager creates the data directory, deletes any leftover `*.log.tmp` (the rename
is compaction's commit point, so a surviving temporary is by definition uncommitted), and
replays every `*.log` before the listener opens.

Replay streams frames in order: HEADER sets the configuration, counters, and sequence floor;
ENQUEUE inserts a message; ACK removes one; NACK updates its `available_at`. An ACK or NACK for
a message that is not present is ignored, which is what makes replay idempotent across a
compaction boundary. Afterwards each surviving message goes to the delayed heap if its
`available_at` is still in the future and to the ready heap otherwise, and the scheduler starts.

In-flight state is deliberately never written. There is no record type for "delivered", so a
restart has exactly the same effect as a visibility timeout: every unacknowledged message
becomes available again. That single rule is what makes recovery simple enough to trust.

```
Before crash            After restart
A READY                 A available
B IN_FLIGHT             B available again (redelivery, at-least-once)
C DELAYED               C still delayed until its available_at
D ACKED                 D never delivered again
```

This has been verified both as a test and by hand against a running server with `kill -9`.

The cost, stated plainly: `receive_count` resets after a crash, because delivery attempts were
never persisted. A message redelivered because the process died reports attempt 1, not attempt
2. Receipts issued before the crash are also gone, so a consumer that returns after a restart
gets a 404 on ack rather than acknowledging a delivery that no longer exists.

### Damaged logs

Recovery distinguishes a torn tail, which is discarded, from corruption, which stops the server.
The rules:

- Running out of bytes part way through a frame is a torn tail. The file ends mid-record, so no
  complete operation was ever written there.
- A structurally complete frame whose checksum or record type is invalid is a torn tail only if
  nothing follows it, since a partial write can leave a full-length region containing stale or
  zeroed bytes.
- A zero length followed by zeros to the end of the file is a torn tail: that is the zero fill a
  partial write leaves, and nothing beyond it was ever committed.
- Anything else has valid data after it and is reported as corruption.

A torn tail is logged, the file is truncated back to the last valid frame, the truncation is
fsynced along with the directory, and the queue serves normally. That frame belonged to an
operation no client was ever told had succeeded.

Corruption anywhere else aborts startup with the file name and the byte offset, and the file is
left untouched. Those records were acknowledged to clients; continuing while quietly missing
them would be worse than refusing to start. Recovering from that is an operator decision: move
the file aside, restore a backup, or accept the loss by truncating it deliberately.

### Compaction

The log would otherwise grow without bound, since an acked message leaves two records behind.
When a queue's log passes `--compact-threshold` records it is rewritten under the queue mutex: a
fresh header carrying the current counters and sequence floor, then one ENQUEUE per live
message. Clean shutdown compacts as well.

Compaction only runs if it would at least halve the log. Without that condition a queue holding
more live messages than the threshold would recompact on every append, because the freshly
written log would already exceed the limit. Requiring the log to double before the next attempt
keeps the work amortised.

Crash safety rests on rename being the single commit point:

1. Write the replacement to `<name>.log.tmp`.
2. `fsync` it and close it.
3. `os.Rename` over the live log. This is the commit.
4. `fsync` the directory, which makes the rename itself durable.
5. Swap the in-memory file handle and close the old one, so no writer ever targets the replaced
   inode.

A crash before step 3 leaves the old log entirely intact plus a stray temporary that the next
startup deletes. A crash after it leaves the complete new log. There is no point at which a
reader could observe a mixture of the two.

In-flight messages are written out as live, with their original `available_at`. After a restart
they come back ready, which is the same policy that applies to any other unacknowledged message.

Compaction runs inline while holding the queue mutex, which pauses that queue for the duration.
At the default threshold this is a few tens of milliseconds. Compacting concurrently with
appends is a deliberate non-goal; the pause buys a version that is obviously correct.

### Receipts

A receipt is 16 bytes from `crypto/rand`, hex encoded, minted fresh on every delivery. It is a
capability for one specific delivery, not an identifier for the message, and the in-flight map
is keyed by it.

That choice makes the stale-consumer problem disappear rather than requiring special handling.
Consumer one receives message M with receipt R1 and stalls. The window expires, R1 is deleted,
and M returns to the ready heap. Consumer two receives M with a fresh receipt R2. Consumer one
finally finishes and acks with R1: the lookup misses and it gets a 404. It cannot acknowledge
the delivery consumer two is working on, and no generation counters or version numbers were
needed to arrange that.

`receive_count` is incremented at delivery and is one-based, so the first delivery reports 1 and
a redelivery reports 2. It matches SQS's `ApproximateReceiveCount` and is directly usable as the
threshold for a future dead-letter queue.

### Layering

```
HTTP (internal/api)          routing, JSON, status codes, seconds to Duration, body limits
Engine (internal/queue)      manager, heaps, mutex and cond, scheduler, delivery semantics
Storage (internal/storage)   framed append log, fsync, recovery, compaction
```

The engine does not import `net/http` and is usable as a library. Storage does not know that
queues, heaps, or deliveries exist; it deals in records. The HTTP layer holds no queue logic: it
decodes, validates, converts, calls one engine method, and encodes, with a single table mapping
engine errors onto status codes.

The API speaks whole seconds; the engine speaks `time.Duration`. Confining that conversion to
the HTTP layer is also what lets the timing tests run on 20 to 50 millisecond timers, so the
scheduler's real behaviour is exercised without a slow suite and without introducing a clock
abstraction into the engine.

There is exactly one interface, the queue's four-method view of its log. It earns its place: the
tests substitute an in-memory implementation for the ordering and concurrency tests, which would
otherwise pay several thousand fsyncs, and a failing one to exercise the rollback paths.

### Known limits

- **At-least-once, not exactly-once.** Consumers must tolerate duplicates. Every delivery carries
  a stable `message_id` and a `receive_count` so they can deduplicate.
- **A poison message is retried forever.** There is no dead-letter queue yet.
- **Priority can starve.** A steady stream of high-priority messages will hold back low-priority
  ones indefinitely. That is what a priority queue does; there is no ageing.
- **Single node.** No replication, no partitioning, no clustering.
- **Delay resolution depends on the wall clock.** In-process deadlines use monotonic time, but
  `available_at` is persisted as wall-clock milliseconds because a monotonic reading cannot be
  serialised. A backwards clock jump delays promotion by the size of the jump.
- **Deleting a queue discards its in-flight messages** along with the log.
- **One corrupt log stops the whole server**, not just that queue.

## Testing

```
go vet ./...
go test -race ./...
```

The suite runs in about 10 seconds of wall clock and covers ordering (both modes, priority, equal-priority
runs, the worked example above, and 5000 messages checked against an independently sorted
reference); delay and the scheduler, including the case that proves promotion is timer driven
rather than polled, where the scheduler is armed for a distant message and must wake early for a
nearer one; visibility timeouts, redelivery, and stale receipts; the framing layer, including
torn tails, bad checksums at the tail and in the middle, zero fill, non-zero garbage, oversized
lengths, rolled-back partial writes, and a log left unwritable; recovery, with a crash simulated
at every point where state changes and the exact surviving state asserted; compaction, including
a crash before the rename; and concurrency, where 16 producers and 8 consumers move 8000
messages with every one acked exactly once, no two consumers ever holding the same message, and
a watchdog so a deadlock fails rather than hangs.

`integration_test.go` drives the same properties through the real HTTP stack against a real data
directory, restarting the process between assertions.
