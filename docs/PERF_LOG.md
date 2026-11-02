# shmcast performance log

Machine: Apple M5 (6 P-cores, 12 E-cores, 128 B lines), macOS 27, Apple clang -O3. The machine
was shared with an unrelated workload: load averages are recorded in `bench/results/ENV.txt`
and at the top of `producer_experiments.txt`. Latencies are quantised to the 41.7 ns system
counter tick.

## Round trip, 32 B (`bench/results/bench.json`, load average ~6)

| transport | p50 | p99 | p99.9 | mean |
|---|---|---|---|---|
| shm broadcast (2 rings) | 167 ns | 250 ns | 333 ns | 180 ns |
| shm SPSC (2 queues) | 167 ns | 250 ns | 333 ns | 170 ns |
| pipe | 4,625 ns | 9,916 ns | 16,833 ns | 5,582 ns |
| Unix socket | 6,166 ns | 11,166 ns | 28,083 ns | 6,346 ns |
| TCP loopback (NODELAY) | 16,417 ns | 45,375 ns | 65,916 ns | 19,127 ns |

An earlier run at load average ~3 measured a 125 ns p50 for both shared-memory transports and
4.3 µs for the pipe. Its output was not saved, so it is quoted for context only. The committed
numbers are the loaded run above.

## One-way fan-out at 1M msg/s, 2M messages per consumer (`bench.json`)

| consumers | lost | corrupt | worst p50 | worst p99 | worst p99.9 |
|---|---|---|---|---|---|
| 1 | 0 | 0 | 125 ns | 208 ns | 4.8 µs |
| 2 | 0 | 0 | 125 ns | 250 ns | 3.1 µs |
| 4 | 0 | 0 | 250 ns | 292 ns | 12.4 µs |
| 8 | 0 | 0 | 292 ns | 417 ns | 212 µs |

The p99.9 tail with 8 consumers is scheduling: 9 spinning processes against 6 P-cores, with
other load on the machine.

## Unpaced (`bench.json`)

| consumers | producer rate | worst consumer delivered | corrupt |
|---|---|---|---|
| 1 | 51.6M msg/s | 79.5% (rest lapped and counted) | 0 |
| 4 | 34.1M msg/s | 84.8% | 0 |
| 8 | 13.6M msg/s | 95.5% | 0 |

An unthrottled producer outruns a single consumer, and the ring does exactly what it is designed
to do: the consumer is lapped, counts its loss and never sees a torn message. As consumers are
added the producer slows, because of coherence traffic (DEEP_DIVE Q6), so each consumer
keeps a larger share.

## Full-day ITCH replay (`research/results/itch_fullday_*.json`)
368,366,634 messages (10.5 GB of message bytes) from the gzip file, through a 1M-slot ring of
80-byte slots.

| run | producer | normal consumers | slow consumer |
|---|---|---|---|
| producer only | 16.0M msg/s (gzip + framing + FNV) | | |
| 4 + 1 slow | 8.80M msg/s | 4 × exact FNV `5bf597be45f123c1`, 0 lost | lapped 223×, lost 116,918,683, delivered + lost = published |
| 7 + 1 slow | 7.15M msg/s | 7 × exact, 0 lost | lapped 122×, lost 63,964,201, accounted exactly |

## Producer-throughput experiments (first 50M messages, 4 consumers; `producer_experiments.txt`)

| change | result | verdict |
|---|---|---|
| hypothesis: 80 B slots share cache lines with neighbours (false sharing); 128 B slots | 9.36M / 7.60M (80 B) vs 9.52M / 7.07M (128 B) over two passes | no effect beyond noise; kept 80 B |
| readers poll the producer's `next` counter instead of the slot | measured once and clearly worse: the single counter line then bounced every message; reverted, output not retained | rejected |
| reader backoff of 0/16/64/256 `yield`s after an empty poll | 7.29M / 7.36M / 7.36M / 7.60M | within run-to-run noise (identical configs ranged 7.1–9.5M); not adopted |
| producer alone | 17.2M (80 B) / 16.0M (128 B) | the ceiling is gzip inflate plus framing |

**Conclusion.** The producer's slowdown comes from readers that are caught up and spinning at
the head; it is the cost of their latency. A latency-insensitive consumer, such as a recorder,
could block or sleep instead of spinning and would not tax the producer.

## Bugs found by the fuzzer (CI, libFuzzer + ASan/UBSan)
- **Buffer overflow in `poll`.** The reader copied up to `max_payload()`, which is derived from
  the producer-controlled header. A header advertising large slots overflowed a caller's 512-byte
  stack buffer. Fix: `poll(buf, cap, …)` bounds the copy by the caller's capacity and skips
  (counts as lost) any consistent message longer than it. Regression test:
  `OversizedMessageNeverOverflowsCallerBuffer`. The local fuzz driver had missed it because it
  only generated small slot sizes.
- **Misaligned header access.** The header type is 128-byte aligned, and the harness handed the
  reader 16-byte-aligned memory. Real mmap'd regions are page-aligned, but the reader now
  rejects misaligned memory explicitly (`RejectsMisalignedMemory`), and the harness allocates
  aligned memory.

## Correctness checks behind the numbers
- TSan: all tests clean, including 4 concurrent readers, one slow, over 2M messages.
- ASan/UBSan: all tests clean. The fuzz target ran on 300k random regions locally (a driver
  without libFuzzer), and libFuzzer runs in CI.
- Every benchmark consumer validates message content (`corrupt` column): 0 corrupt messages in
  every run.
