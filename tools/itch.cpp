// shmcast-itch: replay a NASDAQ TotalView-ITCH 5.0 file (gzip, 2-byte big-endian length framing)
// through a shared-memory broadcast ring to K consumer processes.
//
// Every process folds each message into FNV-1a and per-type counts. A fast consumer must end
// with exactly the producer's checksum and counts (nothing lost, reordered or corrupted). An
// optional deliberately slow consumer is lapped by the producer; it must report its exact loss,
// deliver only intact messages, and must not slow the producer or the other consumers.
//
// usage: shmcast-itch FILE.gz [--consumers K] [--slow 0|1] [--limit N] [--slot BYTES] [--backoff N]
#include <sys/wait.h>
#include <unistd.h>
#include <zlib.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "shmcast/broadcast.hpp"
#include "shmcast/clock.hpp"
#include "shmcast/region.hpp"

using namespace shmcast;

namespace {

constexpr std::uint64_t kFnvOff = 0xcbf29ce484222325ull, kFnvPrime = 0x100000001b3ull;

struct Digest {
  std::uint64_t msgs = 0, bytes = 0, fnv = kFnvOff;
  std::uint64_t by_type[256] = {};
  void add(const unsigned char* p, std::uint32_t n) {
    ++msgs;
    bytes += n;
    ++by_type[n ? p[0] : 0];
    std::uint64_t h = fnv;
    for (std::uint32_t i = 0; i < n; ++i) h = (h ^ p[i]) * kFnvPrime;
    fnv = (h ^ 0xff) * kFnvPrime;  // message boundary
  }
};

struct Out {
  Digest d;
  std::uint64_t lost, lapped_events;
  double seconds;
  int slow;
};

struct Shared {
  std::atomic<int> ready;
  std::atomic<int> done;  // producer finished: total messages published is in `total`
  std::uint64_t total;
  Out out[16];
};

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: shmcast-itch FILE.gz [--consumers K] [--slow 0|1] [--limit N]\n");
    return 2;
  }
  int consumers = 4, slow = 1;
  std::uint64_t limit = ~0ull;
  std::size_t slot_bytes = 80;
  int backoff = 0;  // extra cpu_relax() iterations after an empty poll
  for (int i = 2; i + 1 < argc; i += 2) {
    const std::string a = argv[i];
    if (a == "--consumers") consumers = std::atoi(argv[i + 1]);
    else if (a == "--slow") slow = std::atoi(argv[i + 1]);
    else if (a == "--limit") limit = std::strtoull(argv[i + 1], nullptr, 10);
    else if (a == "--slot") slot_bytes = std::strtoull(argv[i + 1], nullptr, 10);
    else if (a == "--backoff") backoff = std::atoi(argv[i + 1]);
  }
  if (consumers < 0 || consumers > 14) return 2;
  // 1M slots of 80 B (64 B payload; the longest ITCH 5.0 message is 50 B). One slot per 128 B
  // cache line was measured and did not help (PERF_LOG).
  const std::size_t n = 1 << 20, sb = slot_bytes;
  Region ring = Region::anonymous(broadcast_bytes(n, sb));
  Region shr = Region::anonymous(sizeof(Shared));
  auto* sh = new (shr.data()) Shared{};
  BroadcastWriter w(ring.data(), n, sb);
  const int total_procs = consumers + slow;
  std::vector<pid_t> pids;
  for (int k = 0; k < total_procs; ++k) {
    const bool is_slow = k == consumers;
    const pid_t pid = fork();
    if (pid == 0) {
      prefer_performance_core();
      BroadcastReader r(ring.data(), ring.size());
      r.seek(0);
      Out o{};
      o.slow = is_slow;
      sh->ready.fetch_add(1);
      unsigned char buf[64];
      std::uint32_t len = 0, type = 0;
      const std::uint64_t t0 = now_ns();
      for (;;) {
        const Poll p = r.poll(buf, len, type);
        if (p == Poll::Message) {
          o.d.add(buf, len);
          // The slow consumer does ~1 us of extra "work" every 8 messages (~125 ns/message, a
          // budget of ~8 M msg/s): slower than the replay, so the producer laps it.
          if (is_slow && (o.d.msgs & 7) == 0) {
            const std::uint64_t until = now_ns() + 1000;
            while (now_ns() < until) {
            }
          }
        } else if (p == Poll::Lapped) {
          ++o.lapped_events;
        } else if (sh->done.load(std::memory_order_acquire) && r.position() >= sh->total) {
          break;
        } else {
          for (int b = 0; b <= backoff; ++b) cpu_relax();
        }
      }
      o.lost = r.lost();
      o.seconds = static_cast<double>(now_ns() - t0) / 1e9;
      sh->out[k] = o;
      _exit(0);
    }
    pids.push_back(pid);
  }
  while (sh->ready.load() < total_procs) cpu_relax();
  prefer_performance_core();
  gzFile gz = gzopen(argv[1], "rb");
  if (!gz) {
    std::perror("gzopen");
    return 1;
  }
  gzbuffer(gz, 1 << 20);
  Digest pd;
  std::vector<unsigned char> buf(1 << 22);
  std::size_t have = 0, off = 0;
  bool eof = false;
  const std::uint64_t t0 = now_ns();
  std::uint64_t oversize = 0;
  while (pd.msgs < limit) {
    if (have - off < 2 + 64 && !eof) {
      std::memmove(buf.data(), buf.data() + off, have - off);
      have -= off;
      off = 0;
      const int r = gzread(gz, buf.data() + have, static_cast<unsigned>(buf.size() - have));
      if (r <= 0) eof = true;
      else have += static_cast<std::size_t>(r);
    }
    if (have - off < 2) break;
    const std::uint32_t len = (static_cast<std::uint32_t>(buf[off]) << 8) | buf[off + 1];
    if (have - off < 2 + len) break;
    const unsigned char* m = buf.data() + off + 2;
    if (len > w.max_payload()) {
      ++oversize;  // not expected for ITCH 5.0 (longest message is 50 bytes)
    } else {
      w.publish(m, len, m[0]);
      pd.add(m, len);
    }
    off += 2 + len;
  }
  const double secs = static_cast<double>(now_ns() - t0) / 1e9;
  gzclose(gz);
  sh->total = w.published();
  sh->done.store(1, std::memory_order_release);
  for (pid_t p : pids) waitpid(p, nullptr, 0);
  std::printf("{\n  \"file\": \"%s\",\n  \"messages\": %llu, \"bytes\": %llu, \"oversize_skipped\": %llu,\n", std::strrchr(argv[1], '/') ? std::strrchr(argv[1], '/') + 1 : argv[1],
              static_cast<unsigned long long>(pd.msgs), static_cast<unsigned long long>(pd.bytes), static_cast<unsigned long long>(oversize));
  std::printf("  \"producer_seconds\": %.3f, \"producer_msgs_per_sec\": %.0f, \"producer_fnv\": \"%016llx\",\n  \"ring\": {\"slots\": %zu, \"slot_bytes\": %zu},\n", secs,
              static_cast<double>(pd.msgs) / secs, static_cast<unsigned long long>(pd.fnv), n, sb);
  std::printf("  \"consumers\": [\n");
  bool all_ok = true;
  for (int k = 0; k < total_procs; ++k) {
    const Out& o = sh->out[k];
    bool types_eq = true;
    for (int t = 0; t < 256; ++t) types_eq &= o.d.by_type[t] == pd.by_type[t];
    const bool exact = o.d.msgs == pd.msgs && o.d.fnv == pd.fnv && types_eq && o.lost == 0;
    const bool accounted = o.d.msgs + o.lost == pd.msgs;
    if (!o.slow) all_ok &= exact;
    else all_ok &= accounted && o.d.fnv != 0;
    std::printf("    {\"slow\": %s, \"messages\": %llu, \"lost\": %llu, \"lapped_events\": %llu, \"fnv\": \"%016llx\", \"exact_match\": %s, \"delivered_plus_lost_eq_published\": %s, \"seconds\": %.3f}%s\n",
                o.slow ? "true" : "false", static_cast<unsigned long long>(o.d.msgs), static_cast<unsigned long long>(o.lost),
                static_cast<unsigned long long>(o.lapped_events), static_cast<unsigned long long>(o.d.fnv), exact ? "true" : "false",
                accounted ? "true" : "false", o.seconds, k + 1 < total_procs ? "," : "");
  }
  std::printf("  ],\n  \"message_types\": {");
  bool first = true;
  for (int t = 0; t < 256; ++t)
    if (pd.by_type[t]) {
      std::printf("%s\"%c\": %llu", first ? "" : ", ", t, static_cast<unsigned long long>(pd.by_type[t]));
      first = false;
    }
  std::printf("},\n  \"ok\": %s\n}\n", all_ok ? "true" : "false");
  return all_ok ? 0 : 1;
}
