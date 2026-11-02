# shmcast

**Header-only C++20 shared-memory transport for market data between processes.** It provides a
wait-free single-producer broadcast ring with per-slot seqlocks: slow consumers are lapped and
know exactly what they lost, and readers never see a torn message. It also provides a lossless
SPSC queue for order flow. It is benchmarked against pipes, Unix sockets and TCP, and it
replays a full NASDAQ ITCH day to 8 processes with bit-exact checksums.

[![CI](https://github.com/eshaan2418/shmcast/actions/workflows/ci.yml/badge.svg)](https://github.com/eshaan2418/shmcast/actions/workflows/ci.yml)

> Educational/research project. Not investment advice. No live trading.

## Headline results

Apple M5, macOS, Apple clang -O3. The machine was shared with another workload (load average
~6, recorded in [`ENV.txt`](bench/results/ENV.txt)). Numbers come from
[`bench.json`](bench/results/bench.json) and [`research/results`](research/results).

| what | result |
|---|---|
| Cross-process round trip, 32 B, p50 | **167 ns** shared memory vs 4.6 µs pipe, 6.2 µs Unix socket, 16.4 µs TCP loopback |
| One-way latency at 1M msg/s, p50 / p99 | 125 / 208 ns (1 consumer), 250 / 292 ns (4), 292 / 417 ns (8); **0 lost, 0 corrupt** |
| Full NASDAQ ITCH day (368,366,634 msgs, 10.5 GB) → 4 consumers + 1 slow | every normal consumer **matches the producer's FNV-1a exactly** (`5bf597be45f123c1`); the slow one is lapped 223×, loses 116.9M, **delivered + lost = published** |
| Same → 7 consumers + 1 slow | 7 × exact match; slow consumer's loss accounted exactly |
| Producer rate, ITCH replay | 16.0M msg/s alone (gzip-bound), 8.8M with 5 readers, 7.2M with 8 (cache-coherence cost of spinning readers) |
| Unpaced synthetic producer | 51.6M msg/s with 1 consumer; lapped consumers lose messages by design, 0 corrupt |
| Memory-model correctness | payload via relaxed `atomic_ref`: **TSan-clean** seqlock; ASan/UBSan clean; hostile-region fuzzing |

## Design in one picture

```
                producer (feed handler)                   consumers (strategies, risk, recorder)
   slot s:  seq=2s+1 ─► payload ─► seq=2s+2      read seq ─► copy payload ─► re-read seq
            (relaxed)   (relaxed    (release)     (acquire)   (relaxed       (fence; equal ⇒ intact,
                         atomic_ref)                           atomic_ref)    larger ⇒ lapped: count loss, resync)
   ring:  [hdr|next] [slot 0][slot 1] ... [slot 2^k − 1]       in a POSIX shm object or a fork-shared mapping
```

- **Broadcast (no back-pressure).** The producer never waits. Any number of consumer processes
  read independently. A lapped consumer learns exactly how many messages it missed and resumes
  half a ring behind the head. This suits market data, where one slow strategy must not delay
  everyone's view of the market.
- **SPSC (back-pressure).** Head and tail sit on separate 128-byte lines with cached peer
  indices. The producer waits when the ring is full, which suits order flow, where nothing may
  be dropped.
- **Safe against bad producers.** The reader validates the ring geometry against its own mapping
  and bounds every copy by the caller's buffer capacity, so a corrupt region can never cause an
  out-of-bounds read or overflow the caller's buffer (fuzzed; libFuzzer found and the tests pin
  the original capacity bug).
  Consumers can map the ring read-only.

## Run it

```bash
make build test        # GoogleTest: ordering, odd lengths, lapping, 4-reader torn-read stress, cross-process (named shm + fork), SPSC
make tsan sanitize     # ThreadSanitizer, ASan + UBSan
make bench             # ping-pong (5 transports) + fan-out -> bench/results/bench.json (needs an idle machine; spins up to 9 cores)
make data demo         # NASDAQ ITCH sample, then a full-day replay to 4 consumers + 1 slow consumer
make experiments       # producer-throughput experiments (slot size, reader backoff)
make fuzz              # libFuzzer (needs clang with the fuzzer runtime; runs in CI)
```

Real output of `make demo` (abridged from
[`itch_fullday_4plus1slow.json`](research/results/itch_fullday_4plus1slow.json)):

```
"messages": 368366634, "bytes": 10509149824,
"producer_seconds": 41.875, "producer_msgs_per_sec": 8796877, "producer_fnv": "5bf597be45f123c1",
{"slow": false, "messages": 368366634, "lost": 0, "fnv": "5bf597be45f123c1", "exact_match": true, ...}   x4
{"slow": true,  "messages": 251447951, "lost": 116918683, "lapped_events": 223,
                "delivered_plus_lost_eq_published": true, ...}
"ok": true
```

Use it as a library:

```cpp
#include "shmcast/broadcast.hpp"
#include "shmcast/region.hpp"
using namespace shmcast;
// producer
Region r = Region::create("/md.itch", broadcast_bytes(1 << 20, 80));
BroadcastWriter w(r.data(), 1 << 20, 80);
w.publish(msg, len, msg[0]);
// consumer (another process)
Region ro = Region::open("/md.itch");
BroadcastReader rd(ro.data(), ro.size());
unsigned char buf[64]; std::uint32_t len, type;
switch (rd.poll(buf, sizeof buf, len, type)) { case Poll::Message: /* use */ break;
                                   case Poll::Lapped:  /* rd.lost() grew: resync book */ break;
                                   case Poll::Empty:   break; }
```

## Caveats, measured
- **Coarse clock.** Apple Silicon's clock ticks every 41.7 ns, so latencies are multiples of it.
- **No core pinning on macOS.** Threads request the `USER_INTERACTIVE` QoS class. The tails
  (p99.9, max) are scheduling-dominated, especially with 8 spinning consumers on 6 P-cores.
- **Spinning readers cost the producer.** Caught-up consumers cost the producer throughput
  through coherence traffic. Three mitigations were tried, and none helped beyond noise on this
  shared machine ([PERF_LOG](docs/PERF_LOG.md)).

## Documentation
- [docs/DESIGN.md](docs/DESIGN.md): layout, the seqlock and the memory model, lapping, SPSC,
  timing
- [docs/MICROSTRUCTURE.md](docs/MICROSTRUCTURE.md): why trading systems fan out market data
  through shared memory, and lossy vs lossless
- [docs/DEEP_DIVE.md](docs/DEEP_DIVE.md): 17 interview-style questions and answers
- [docs/PERF_LOG.md](docs/PERF_LOG.md): every benchmark and experiment, including the rejected
  hypotheses

## Where it fits
shmcast is the market-data bus of a small research trading stack. It sits between
[itchbook](https://github.com/eshaan2418/itchbook) (feed handler) and strategy or risk processes
such as [riskd](https://github.com/eshaan2418/riskd), with an SPSC lane toward an order gateway
like [matchbox](https://github.com/eshaan2418/matchbox). Other parts of the stack:
[marketmux](https://github.com/eshaan2418/marketmux),
[raftex](https://github.com/eshaan2418/raftex),
[queuesim](https://github.com/eshaan2418/queuesim),
[tickstore](https://github.com/eshaan2418/tickstore) and
[volsurf](https://github.com/eshaan2418/volsurf).

## Data
NASDAQ's public TotalView-ITCH 5.0 sample `01302019.NASDAQ_ITCH50.gz` (SHA-256 `8c97b5b1…0be`),
fetched by `make data`. It is not redistributed here.

## License
MIT.
