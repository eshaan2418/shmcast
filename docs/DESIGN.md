# shmcast design

## Two primitives
| | `Broadcast` | `Spsc` |
|---|---|---|
| shape | 1 producer, any number of consumer processes | 1 producer, 1 consumer |
| back-pressure | none: the producer never waits | the producer waits when the ring is full |
| a slow consumer | is lapped, learns exactly how many messages it lost, resynchronises | slows the producer |
| use | market data: the feed must not stall for anyone | order flow, where nothing may be dropped |

Both live in a shared mapping: a named POSIX object (`shm_open` + `mmap`), or an anonymous
`MAP_SHARED` mapping inherited across `fork()`. Neither involves a system call on the data path.

## Broadcast ring layout
```
BroadcastHeader  magic "SHMCAST1" | version | slot_bytes | nslots (2^k) | epoch
                 [128 B line] next: messages published (written by the producer after each message)
slot[i]          seq (u64) | len (u32) | type (u32) | payload words ...   (slot_bytes each)
```

### Seqlock per slot
For message `s` (slot `s & (n−1)`) the producer:
1. stores `seq = 2s+1` (relaxed), then a release fence,
2. stores len, type and the payload words (relaxed),
3. stores `seq = 2s+2` (release), then `next = s+1` (release).

A consumer expecting message `e`:
1. loads `seq` (acquire):
   - `< 2e+2`: the message is not published yet (empty, mid-write, or an old lap). Poll again.
   - `> 2e+2`: the slot already holds a later lap. The consumer has been **lapped**.
2. If `seq == 2e+2`, it copies len, type and the payload (relaxed loads), issues an acquire fence
   and re-loads `seq`. If the value is unchanged the copy is consistent. Otherwise the producer
   overwrote the slot mid-copy: lapped.

### Why relaxed atomics for the payload
A classic seqlock copies the payload with plain loads, which is a data race and therefore
undefined behaviour in C++ (and flagged by ThreadSanitizer). Here every payload word is read and
written through `std::atomic_ref<uint64_t>` with relaxed ordering. On arm64 and x86-64 these
compile to ordinary loads and stores, so there is no cost, and the program is race-free. The
fences provide the ordering (Boehm, "Can seqlocks get along with programming language memory
models?", 2012). The tests run clean under TSan.

### Lapping and resynchronisation
A lapped consumer adds the skipped count to `lost()` and resumes **half a ring behind the
producer's head**, far enough back that it is unlikely to be lapped again at once. It never
returns a torn message. The tests check delivered + lost == published exactly.

### Hostile producers
The reader validates the header against its mapping:
- 128-byte alignment;
- magic and version;
- a power-of-two slot count and a valid slot size;
- `nslots × slot_bytes` fitting inside the mapping.

`poll` takes the **caller's buffer capacity**. The slot size comes from the producer's header,
so a hostile producer advertising huge slots could otherwise make a consumer with a small fixed
buffer write past it. A consistent message longer than the buffer is skipped and counted in
`lost()`. A corrupt or malicious region therefore cannot make the reader read out of bounds or
write past the caller's buffer. This is fuzzed under ASan/UBSan (libFuzzer in CI found the
original capacity bug; see PERF_LOG), and the reader can map the region read-only.

## SPSC queue
`head` (consumer) and `tail` (producer) sit on separate 128-byte lines; the Apple M-series line is
128 B. Each side keeps a cached copy of the other's index and re-reads the shared one only when
the cache says full or empty. In steady state a push touches its own index line and the slot.
Payloads are plain memory, ordered by the release store and acquire load of the indices, so
there is no data race.

## Timing
`now_ns()` reads `CLOCK_UPTIME_RAW` (macOS) or `CLOCK_MONOTONIC_RAW` (Linux), the same clock in
every process. On Apple Silicon it comes from the 24 MHz system counter: values are nanoseconds,
but they advance in ~41.7 ns steps. One-way latencies are therefore quantised to 42, 84, 125,
167, 209 ns and so on. Ping-pong means over many round trips are finer-grained than any single
sample.

## Scheduling
macOS has no thread-affinity API on Apple Silicon. The benchmark threads request
`QOS_CLASS_USER_INTERACTIVE`, the supported way to prefer a performance core. With 6 P-cores and
other load on the machine, some consumers may land on efficiency cores. This is visible in the
p99.9 and max columns.

## Benchmarks
- **`pingpong`.** 32-byte round trips between two processes over two broadcast rings, two SPSC
  queues, a pipe pair, a Unix socket pair and TCP loopback (`TCP_NODELAY`). There are 20k warm-up
  iterations, then 200k measured.
- **`fanout`.** One producer and K consumer processes. The paced runs (1M msg/s) measure one-way
  latency from a timestamp inside the message. The unpaced runs publish as fast as possible and
  report the producer rate and each consumer's delivered and lost counts.
- **`shmcast-itch`.** Replays the full NASDAQ ITCH 5.0 sample (368M messages) through a
  1M-slot ring to K consumer processes, plus optionally one deliberately slow one. Every process
  computes FNV-1a over all messages and per-type counts. Fast consumers must match the producer
  exactly; the slow one must satisfy delivered + lost == published.

## Not here
Variable-length or multi-slot messages (ITCH fits 64-byte payloads), multiple producers, a
persistent journal for late joiners (Aeron's archive), network transport, and consumer
liveness detection (a producer cannot tell whether consumers exist, by design).
