// shmcast-bench: cross-process latency and throughput.
//   pingpong  round trips between two processes over shm (broadcast, spsc), pipe, UDS, TCP
//   fanout    one producer, K consumer processes: one-way latency at a paced rate, plus an
//             unpaced run measuring producer rate and per-consumer delivered/lost
// Output: JSON on stdout.
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "shmcast/baselines.hpp"
#include "shmcast/broadcast.hpp"
#include "shmcast/clock.hpp"
#include "shmcast/region.hpp"
#include "shmcast/spsc.hpp"

using namespace shmcast;

namespace {

struct Pct {
  double p50, p90, p99, p999, max, mean;
};

Pct pct(std::vector<std::uint64_t>& v) {
  std::sort(v.begin(), v.end());
  auto q = [&](double p) { return static_cast<double>(v[std::min(v.size() - 1, static_cast<std::size_t>(p * static_cast<double>(v.size())))]); };
  double s = 0;
  for (auto x : v) s += static_cast<double>(x);
  return {q(0.5), q(0.9), q(0.99), q(0.999), static_cast<double>(v.back()), s / static_cast<double>(v.size())};
}

void print_pct(const char* name, const Pct& p, bool comma) {
  std::printf("    \"%s\": {\"p50_ns\": %.0f, \"p90_ns\": %.0f, \"p99_ns\": %.0f, \"p999_ns\": %.0f, \"max_ns\": %.0f, \"mean_ns\": %.1f}%s\n", name, p.p50,
              p.p90, p.p99, p.p999, p.max, p.mean, comma ? "," : "");
}

constexpr int kWarm = 20000;

// Round trips: parent sends a 32-byte message, child echoes it. RTT measured per iteration.
template <class Send, class Recv>
std::vector<std::uint64_t> rtt_loop(int iters, Send send, Recv recv) {
  std::vector<std::uint64_t> v;
  v.reserve(static_cast<std::size_t>(iters));
  std::uint64_t msg[4] = {};
  for (int i = 0; i < kWarm + iters; ++i) {
    msg[0] = static_cast<std::uint64_t>(i);
    const std::uint64_t t0 = now_ns();
    send(msg);
    recv(msg);
    const std::uint64_t t1 = now_ns();
    if (i >= kWarm) v.push_back(t1 - t0);
  }
  return v;
}

std::vector<std::uint64_t> pingpong_broadcast(int iters) {
  const std::size_t n = 1024, sb = 64;
  Region ab = Region::anonymous(broadcast_bytes(n, sb)), ba = Region::anonymous(broadcast_bytes(n, sb));
  BroadcastWriter wab(ab.data(), n, sb), wba(ba.data(), n, sb);
  const pid_t pid = fork();
  if (pid == 0) {
    prefer_performance_core();
    BroadcastReader r(ab.data(), ab.size());
    r.seek(0);
    BroadcastWriter& w = wba;  // child's view of the b->a ring (initialised before fork)
    std::uint64_t buf[8] = {};
    std::uint32_t len = 0, type = 0;
    for (int i = 0; i < kWarm + iters; ++i) {
      while (r.poll(buf, len, type) != Poll::Message) cpu_relax();
      w.publish(buf, len);
    }
    _exit(0);
  }
  prefer_performance_core();
  BroadcastReader r(ba.data(), ba.size());
  r.seek(0);
  std::uint64_t buf[8] = {};
  std::uint32_t len = 0, type = 0;
  auto v = rtt_loop(
      iters, [&](const std::uint64_t* m) { wab.publish(m, 32); },
      [&](std::uint64_t*) {
        while (r.poll(buf, len, type) != Poll::Message) cpu_relax();
      });
  waitpid(pid, nullptr, 0);
  return v;
}

std::vector<std::uint64_t> pingpong_spsc(int iters) {
  const std::size_t n = 1024, sb = 64;
  Region ab = Region::anonymous(spsc_bytes(n, sb)), ba = Region::anonymous(spsc_bytes(n, sb));
  spsc_init(ab.data(), n, sb);
  spsc_init(ba.data(), n, sb);
  const pid_t pid = fork();
  if (pid == 0) {
    prefer_performance_core();
    SpscConsumer c(ab.data());
    SpscProducer p(ba.data());
    std::uint64_t buf[8] = {};
    for (int i = 0; i < kWarm + iters; ++i) {
      int l;
      while ((l = c.try_pop(buf)) < 0) cpu_relax();
      while (!p.try_push(buf, static_cast<std::uint32_t>(l))) cpu_relax();
    }
    _exit(0);
  }
  prefer_performance_core();
  SpscProducer p(ab.data());
  SpscConsumer c(ba.data());
  std::uint64_t buf[8] = {};
  auto v = rtt_loop(
      iters,
      [&](const std::uint64_t* m) {
        while (!p.try_push(m, 32)) cpu_relax();
      },
      [&](std::uint64_t*) {
        while (c.try_pop(buf) < 0) cpu_relax();
      });
  waitpid(pid, nullptr, 0);
  return v;
}

std::vector<std::uint64_t> pingpong_fd(Channel ch, int iters) {
  const pid_t pid = fork();
  if (pid == 0) {
    std::uint64_t buf[4];
    for (int i = 0; i < kWarm + iters; ++i) {
      if (!read_all(ch.b_rd, buf, 32)) _exit(1);
      write_all(ch.b_wr, buf, 32);
    }
    _exit(0);
  }
  auto v = rtt_loop(
      iters, [&](const std::uint64_t* m) { write_all(ch.a_wr, m, 32); }, [&](std::uint64_t* m) { read_all(ch.a_rd, m, 32); });
  waitpid(pid, nullptr, 0);
  return v;
}

// Results a consumer process reports back through shared memory.
struct ConsumerOut {
  std::uint64_t got, lost, bad;
  double p50, p90, p99, p999, max;
};

void fanout(int consumers, std::uint64_t msgs, double rate_per_s, bool comma) {
  const std::size_t n = 1 << 16, sb = 64;
  Region ring = Region::anonymous(broadcast_bytes(n, sb));
  Region outs = Region::anonymous(sizeof(ConsumerOut) * 16 + sizeof(std::atomic<int>));
  auto* ready = new (outs.data()) std::atomic<int>(0);
  auto* out = reinterpret_cast<ConsumerOut*>(static_cast<char*>(outs.data()) + 64);
  BroadcastWriter w(ring.data(), n, sb);
  std::vector<pid_t> pids;
  for (int k = 0; k < consumers; ++k) {
    const pid_t pid = fork();
    if (pid == 0) {
      prefer_performance_core();
      BroadcastReader r(ring.data(), ring.size());
      r.seek(0);
      std::vector<std::uint64_t> lat;
      lat.reserve(msgs);
      ready->fetch_add(1);
      std::uint64_t buf[8], got = 0, bad = 0;
      std::uint32_t len = 0, type = 0;
      while (r.position() < msgs) {
        const Poll p = r.poll(buf, len, type);
        if (p == Poll::Message) {
          const std::uint64_t t = now_ns();
          if (buf[1] != buf[0] * 3 + 1) ++bad;
          lat.push_back(t - buf[2]);
          ++got;
        } else if (p == Poll::Empty) {
          cpu_relax();
        }
      }
      Pct pc = lat.empty() ? Pct{} : pct(lat);
      out[k] = {got, r.lost(), bad, pc.p50, pc.p90, pc.p99, pc.p999, pc.max};
      _exit(0);
    }
    pids.push_back(pid);
  }
  while (ready->load() < consumers) cpu_relax();
  prefer_performance_core();
  const double gap = rate_per_s > 0 ? 1e9 / rate_per_s : 0;
  const std::uint64_t t0 = now_ns();
  std::uint64_t msg[4] = {};
  for (std::uint64_t s = 0; s < msgs; ++s) {
    if (gap > 0) {
      const std::uint64_t due = t0 + static_cast<std::uint64_t>(gap * static_cast<double>(s));
      while (now_ns() < due) cpu_relax();
    }
    msg[0] = s;
    msg[1] = s * 3 + 1;
    msg[2] = now_ns();
    w.publish(msg, 32);
  }
  const double secs = static_cast<double>(now_ns() - t0) / 1e9;
  for (pid_t p : pids) waitpid(p, nullptr, 0);
  std::printf("    {\"consumers\": %d, \"messages\": %llu, \"target_rate\": %.0f, \"producer_rate\": %.0f, \"per_consumer\": [", consumers,
              static_cast<unsigned long long>(msgs), rate_per_s, static_cast<double>(msgs) / secs);
  for (int k = 0; k < consumers; ++k) {
    const ConsumerOut& o = out[k];
    std::printf("%s{\"got\": %llu, \"lost\": %llu, \"corrupt\": %llu, \"p50_ns\": %.0f, \"p90_ns\": %.0f, \"p99_ns\": %.0f, \"p999_ns\": %.0f, \"max_ns\": %.0f}",
                k ? ", " : "", static_cast<unsigned long long>(o.got), static_cast<unsigned long long>(o.lost), static_cast<unsigned long long>(o.bad), o.p50,
                o.p90, o.p99, o.p999, o.max);
  }
  std::printf("]}%s\n", comma ? "," : "");
}

}  // namespace

int main(int argc, char** argv) {
  const std::string mode = argc > 1 ? argv[1] : "all";
  const int iters = argc > 2 ? std::atoi(argv[2]) : 200000;
  std::printf("{\n");
  if (mode == "all" || mode == "pingpong") {
    std::printf("  \"pingpong_rtt_32B\": {\n    \"iterations\": %d,\n", iters);
    auto a = pingpong_broadcast(iters);
    print_pct("shm_broadcast", pct(a), true);
    auto b = pingpong_spsc(iters);
    print_pct("shm_spsc", pct(b), true);
    auto c = pingpong_fd(make_pipe_channel(), iters);
    print_pct("pipe", pct(c), true);
    auto d = pingpong_fd(make_uds_channel(), iters);
    print_pct("unix_socket", pct(d), true);
    auto e = pingpong_fd(make_tcp_channel(), iters);
    print_pct("tcp_loopback", pct(e), false);
    std::printf("  }%s\n", mode == "all" ? "," : "");
  }
  if (mode == "all" || mode == "fanout") {
    std::printf("  \"fanout_oneway_32B\": [\n");
    for (int k : {1, 2, 4, 8}) fanout(k, 2'000'000, 1e6, true);
    for (int k : {1, 4, 8}) fanout(k, 50'000'000, 0, k != 8);
    std::printf("  ]\n");
  }
  std::printf("}\n");
}
