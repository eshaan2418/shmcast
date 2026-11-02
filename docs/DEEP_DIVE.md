# shmcast: deep-dive interview questions

### 1. Why is shared memory ~30× faster than a pipe here?
A pipe or socket message costs:
- a `write` system call: mode switch, copy into a kernel buffer, wake the reader;
- a `read` system call: mode switch, copy out;
- usually a scheduler wake-up of the blocked reader.

With shared memory, the producer stores into memory the consumer has mapped, and the consumer's
next load sees it via cache coherence. There is no kernel involvement. Measured round trips:
167 ns for shared memory, 4.6 µs for a pipe, 6.2 µs for a Unix socket and 16.4 µs for TCP
loopback (p50, `bench.json`).

### 2. Explain the seqlock in the broadcast ring.
1. The producer marks the slot odd (`2s+1`), writes the payload, then marks it even (`2s+2`).
2. A reader reads the sequence, copies the payload, and reads the sequence again.
3. If both reads saw `2e+2`, the copy is a consistent snapshot of message `e`. A larger value
   means the slot was reused for a later lap.

Readers never write shared state, so any number of them add no contention on each other's
lines. The only shared lines are those the producer writes.

### 3. A seqlock copies data that may be concurrently written. Isn't that a data race?
With plain loads, yes. That is undefined behaviour in C++, and the compiler may assume it doesn't
happen. shmcast accesses payload words through relaxed `std::atomic_ref`. A relaxed atomic load
racing with a relaxed atomic store is well-defined: you get some value, maybe a torn mixture
across words, which the sequence re-check rejects. A release fence on the writer and an acquire
fence on the reader order the payload against the sequence. On arm64 the relaxed accesses are
plain `ldr`/`str`, so the correctness costs nothing at runtime. TSan reports no races.

### 4. What happens when a consumer is too slow?
- **Detection.** The producer overwrites its next slot one lap later. The consumer sees a
  sequence greater than expected, or a changed sequence after its copy, and reports `Lapped`.
- **Recovery.** It moves to half a ring behind the producer's current head and adds exactly the
  skipped count to `lost()`.
- **No impact on others.** The producer never waits and the other consumers are unaffected. In
  the full-day ITCH replay with 4 normal consumers, the slow consumer was lapped 223 times and
  lost 116.9M of 368.4M messages, with delivered + lost == published exactly. The four normal
  consumers matched the producer's FNV-1a checksum over all 368M messages.

### 5. Why resynchronise half a ring back, not at the head?
At the head, the next message isn't published yet, so the consumer would jump to "now" and
discard everything still in the ring. At the oldest slot it would likely be lapped again
immediately. Half a ring is a compromise: it keeps recent history and leaves headroom of
n/2 messages before the next lap. A market-data consumer would usually jump to a fresh snapshot
of the book at this point anyway.

### 6. Why does the producer get slower as consumers are added?
- **Coherence.** A caught-up consumer spins on the slot that holds the next message. When the
  producer writes it, the line must be invalidated in every spinning reader's cache, and each
  reader then misses and re-fetches it. The producer's store waits for those invalidations.
- **Measured.** In the ITCH replay the producer alone runs at 16–17M msg/s, and with four
  caught-up consumers at about 7–9.5M (`producer_experiments.txt`).
- **Fixes tried.**
  - One slot per 128-byte line: no measurable difference.
  - Readers polling the producer's `next` counter instead of the slot: worse, because that
    single line then bounces every message.
  - Reader backoff after an empty poll: not distinguishable from run-to-run noise on this
    shared machine.

  The cost is inherent to readers busy-waiting at the head, which is also what gives them their
  latency.

### 7. Why fixed-size slots?
- **Indexing** is a mask. The seqlock is per message. A slow reader can find message `e` without
  walking frames. Recovery is trivial.
- **ITCH fits.** Messages are at most 50 bytes, so an 80-byte slot wastes some space but never
  splits a message.
- **Variable-length frames**, as in Aeron's term buffers, pack denser but need frame walking and
  a more complex lap check.

### 8. How do you know the ITCH replay is correct, not just fast?
Every process folds every message into FNV-1a (with a boundary marker) and counts messages per
type. Each normal consumer's hash, total count and all 18 per-type counts must equal the
producer's, which proves nothing was lost, duplicated, reordered or corrupted. The hash over the
full day is `5bf597be45f123c1`, identical across the producer and all normal consumers in every
run.

### 9. What does the 41.7 ns step in the latency numbers mean?
Apple Silicon's system counter runs at 24 MHz. `clock_gettime_nsec_np(CLOCK_UPTIME_RAW)` returns
nanoseconds, but the value advances in 41.67 ns steps. A one-way latency of "84 ns" means two
ticks: the true value lies anywhere from about 42 to 125 ns. Means over many samples are finer.
On x86 Linux the TSC resolves about 0.3 ns.

### 10. Why can't you pin threads on macOS, and what is the impact?
Apple Silicon macOS exposes no thread-affinity API. Only QoS classes steer threads between
performance and efficiency cores. Without pinning, the scheduler may migrate a spinning consumer
or place it on an E-core, and other load on the machine (there was some) competes for the 6
P-cores. The medians are stable, but the p99.9 and max values are dominated by scheduling, not
by the ring. On Linux you would isolate cores and pin.

### 11. How do processes find the ring?
- **Named object.** `shm_open("/name")` plus `ftruncate` plus `mmap(MAP_SHARED)`; consumers open
  the same name. The creator unlinks it on destruction.
- **Anonymous mapping.** For tests and benchmarks, a `MAP_SHARED|MAP_ANON` mapping is created
  before `fork()`.
- **Name limit.** macOS limits names to 31 characters (`PSHMNAMLEN`).

### 12. Can a consumer crash the producer, or vice versa?
- **Consumers.** They map the region and never write the broadcast ring. They can map it
  read-only, so a consumer crash affects no one.
- **Producer crash.** It leaves the ring intact, and the consumers simply see no new messages. A
  restarted producer re-initialises the ring with a new `epoch`, which consumers can compare.
- **Hostile producer.** It could write garbage, so the reader validates the geometry against
  its mapping size and clamps lengths. This is fuzzed under ASan.

### 13. What is the difference from the LMAX Disruptor?
The Disruptor (in-process, Java) usually gates the producer on the slowest consumer's sequence:
it has back-pressure, and consumers can form dependency graphs. shmcast's broadcast deliberately
has no back-pressure (market data). Its SPSC queue is the back-pressured variant. Both share the
key ideas: a power-of-two ring, sequence numbers instead of locks, and padding of hot indices to
cache lines.

### 14. Why 128-byte padding?
Apple M-series cores use 128-byte cache lines. Padding `head` and `tail` to 64 bytes, the x86
convention, would leave them on the same line here, and producer and consumer would false-share
it on every operation. On x86, 128 bytes also covers the adjacent-line prefetcher, which pulls
pairs of 64-byte lines.

### 15. What is `cpu_relax()` and why use it in spin loops?
On arm64 it is `yield`, on x86 `pause`. It tells the core the thread is spinning. On x86 that
avoids a memory-order mis-speculation flush when the awaited line finally changes, and it frees
pipeline resources for the sibling hyperthread. On arm64 the effect is milder. It keeps spinning
polite without adding measurable latency.

### 16. How would you add a late-joiner or replay capability?
Rings forget. A consumer that starts late, or is lapped, needs a snapshot or replay source. The
usual design is a recorder process that consumes the ring into a journal, plus a snapshot service
(the book state at sequence N). The late joiner loads the snapshot, then reads the ring from
N+1, using the epoch and sequence to stitch. This is Aeron's archive, or an exchange's snapshot
channel plus incremental feed.

### 17. How does this connect to the rest of the stack?
- itchbook decodes the feed.
- shmcast fans the messages out to strategy processes. They can be queuesim's live
  counterparts, or riskd-style services.
- The SPSC queue carries orders the other way toward a gateway (matchbox).

The full-day ITCH replay here is exactly that hop, measured.
