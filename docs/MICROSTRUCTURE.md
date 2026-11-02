# Why trading systems move market data through shared memory

## The fan-out problem
A feed handler (itchbook, in this stack) decodes the exchange feed once. Several processes need
the result: strategies, a risk engine, a recorder, a TCA collector. The options:
- **One process with threads.** Fastest, but a crash in any strategy takes the feed handler
  down, and every strategy shares one release cycle.
- **Kernel IPC** (pipes, Unix sockets, TCP loopback). Isolated, but every message pays a system
  call on each side, plus a copy into and out of the kernel and often a context switch. The
  benchmark measures 4.6–16 µs per round trip on this machine.
- **Shared memory.** The producer writes into a ring that the consumers map. There is no system
  call and no kernel copy; the data moves through the CPU's cache-coherence protocol. The
  benchmark measures a ~170 ns round trip.

Production examples: Aeron's IPC transport, LMAX Disruptor-style rings, and most proprietary
"market data bus" designs at HFT firms.

## Lossy versus lossless
Market data and order flow have opposite requirements.
- **Market data, lossy (broadcast).** A strategy that falls behind must not slow the feed for
  everyone else: one slow reader would otherwise delay every other strategy's view of the
  market. A slow reader that has fallen behind needs to know **exactly** what it missed, so it
  can resynchronise its book, for example from a snapshot. That is the gap-fill / snapshot
  recovery pattern of exchange feeds (ITCH over MoldUDP64 has the same shape). shmcast's
  broadcast ring gives the producer wait-freedom and the consumer exact loss accounting. In the
  full-day replay a deliberately slow consumer lost 116.9M of 368.4M messages, every loss
  accounted for, while four normal consumers received every message bit-exact.
- **Orders, lossless (SPSC with back-pressure).** Dropping an order or a cancel is never
  acceptable. The producer must wait, or fail fast, when the consumer is behind.

## Latency at this scale
- **Coherence is the cost.** At ~100 ns per hop, the cost is mostly coherence traffic: the cache
  line holding a message moves from the producer's core to each consumer's.
  - Readers that are caught up spin on the line the producer is about to write. Every message
    then involves invalidating that line in every reader's cache.
  - This is why the producer's rate falls as consumers are added (16–17M msg/s alone versus about
    7–9.5M with four caught-up consumers in the ITCH replay). It is also why one-way latency
    rises from ~125 ns with 1–2 consumers to ~290 ns with 8 in the paced run.
- **Core placement matters as much as code.**
  - Linux deployments pin producer and consumers to isolated cores (`isolcpus`, `taskset`) on the
    same socket, ideally sharing an L2 or L3. Cross-socket hops roughly double the latency.
  - macOS cannot pin. That is one reason the p99.9 here is in microseconds while the median is
    in nanoseconds.
- **Clock quantisation.** Apple Silicon's 24 MHz counter quantises measurements to ~42 ns. On
  Linux/x86 the TSC gives about 0.3 ns resolution.

## Message sizing
ITCH 5.0 messages are 11–50 bytes. A fixed 80-byte slot (64-byte payload) holds any of them in
two cache lines at most. Fixed slots make indexing a mask and keep the seqlock per message.
Variable-length framing, as in Aeron's term buffers, packs denser at the cost of more complex
recovery.

## Conduct
The ITCH file is NASDAQ's public sample data. This project only moves bytes between local
processes.
