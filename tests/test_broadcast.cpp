#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include "shmcast/broadcast.hpp"
#include "shmcast/region.hpp"

using namespace shmcast;

namespace {

// Self-checking payload: word 0 is the sequence, every later word is a function of it, and the
// length varies with the sequence. A torn read (words from two different messages) fails check.
std::uint32_t make_msg(std::uint64_t s, std::uint64_t* w, std::size_t maxw) {
  const std::uint32_t nw = 1 + static_cast<std::uint32_t>(s % maxw);
  w[0] = s;
  for (std::uint32_t i = 1; i < nw; ++i) w[i] = s * 0x9e3779b97f4a7c15ull + i;
  return nw * 8;
}

bool check_msg(const std::uint64_t* w, std::uint32_t len, std::size_t maxw) {
  const std::uint64_t s = w[0];
  if (len != 8 * (1 + s % maxw)) return false;
  for (std::uint32_t i = 1; i < len / 8; ++i)
    if (w[i] != s * 0x9e3779b97f4a7c15ull + i) return false;
  return true;
}

struct Ring {
  std::vector<std::uint64_t> mem;
  std::size_t bytes;
  Ring(std::size_t n, std::size_t sb) : mem(broadcast_bytes(n, sb) / 8 + 16), bytes(broadcast_bytes(n, sb)) {}
  void* data() { return mem.data(); }
};

}  // namespace

TEST(Broadcast, PublishPollInOrder) {
  Ring r(64, 64);
  BroadcastWriter w(r.data(), 64, 64);
  BroadcastReader rd(r.data(), r.bytes);
  std::uint64_t buf[8], msg[8];
  std::uint32_t len, type;
  EXPECT_EQ(rd.poll(buf, len, type), Poll::Empty);
  for (std::uint64_t s = 0; s < 40; ++s) {
    const std::uint32_t l = make_msg(s, msg, 6);
    w.publish(msg, l, static_cast<std::uint32_t>(s % 3));
  }
  for (std::uint64_t s = 0; s < 40; ++s) {
    ASSERT_EQ(rd.poll(buf, len, type), Poll::Message);
    EXPECT_EQ(buf[0], s);
    EXPECT_EQ(type, s % 3);
    EXPECT_TRUE(check_msg(buf, len, 6));
  }
  EXPECT_EQ(rd.poll(buf, len, type), Poll::Empty);
  EXPECT_EQ(rd.lost(), 0u);
}

TEST(Broadcast, OddLengthsRoundTrip) {
  Ring r(16, 64);
  BroadcastWriter w(r.data(), 16, 64);
  BroadcastReader rd(r.data(), r.bytes);
  unsigned char in[48], out[48];
  for (std::uint32_t l = 0; l <= 48; ++l) {
    for (std::uint32_t i = 0; i < l; ++i) in[i] = static_cast<unsigned char>(l * 31 + i);
    w.publish(in, l);
    std::uint32_t len, type;
    ASSERT_EQ(rd.poll(out, len, type), Poll::Message);
    ASSERT_EQ(len, l);
    EXPECT_EQ(std::memcmp(in, out, l), 0) << l;
  }
}

TEST(Broadcast, LappedReaderCountsLossAndResyncs) {
  const std::size_t n = 64;
  Ring r(n, 64);
  BroadcastWriter w(r.data(), n, 64);
  BroadcastReader rd(r.data(), r.bytes);
  std::uint64_t msg[6], buf[6];
  for (std::uint64_t s = 0; s < 3 * n; ++s) w.publish(msg, make_msg(s, msg, 6));
  std::uint32_t len, type;
  ASSERT_EQ(rd.poll(buf, len, type), Poll::Lapped);
  EXPECT_EQ(rd.lost(), 3 * n - n / 2);  // resumed half a ring behind the head
  std::uint64_t got = 0, expect = rd.position();
  while (rd.poll(buf, len, type) == Poll::Message) {
    ASSERT_EQ(buf[0], expect++);
    ASSERT_TRUE(check_msg(buf, len, 6));
    ++got;
  }
  EXPECT_EQ(got + rd.lost(), 3 * n);
}

// Concurrent producer and readers, one of them deliberately slow so it gets lapped: every
// delivered message is intact, sequences strictly increase, and delivered + lost == published.
TEST(Broadcast, ConcurrentNoTornReads) {
  const std::size_t n = 1024, total = 2'000'000, maxw = 6;
  Ring r(n, 64);
  BroadcastWriter w(r.data(), n, 64);
  std::atomic<bool> start{false};
  std::atomic<int> ready{0};
  struct Result {
    std::uint64_t got = 0, lost = 0, torn = 0, disorder = 0;
  };
  std::vector<Result> res(4);
  std::vector<std::thread> ts;
  for (int k = 0; k < 4; ++k)
    ts.emplace_back([&, k] {
      BroadcastReader rd(r.data(), r.bytes);
      rd.seek(0);
      ready.fetch_add(1);
      while (!start.load()) {
      }
      std::uint64_t buf[8], last = 0;
      bool first = true;
      std::uint32_t len, type;
      for (;;) {
        const Poll p = rd.poll(buf, len, type);
        if (p == Poll::Message) {
          if (!check_msg(buf, len, maxw)) ++res[k].torn;
          if (!first && buf[0] <= last) ++res[k].disorder;
          last = buf[0];
          first = false;
          ++res[k].got;
          if (k == 3 && res[k].got % 64 == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        if (rd.position() >= total) break;
      }
      res[k].lost = rd.lost();
    });
  while (ready.load() < 4) {
  }
  start.store(true);
  std::uint64_t msg[8];
  for (std::uint64_t s = 0; s < total; ++s) w.publish(msg, make_msg(s, msg, maxw));
  for (auto& t : ts) t.join();
  for (int k = 0; k < 4; ++k) {
    EXPECT_EQ(res[k].torn, 0u) << k;
    EXPECT_EQ(res[k].disorder, 0u) << k;
    EXPECT_EQ(res[k].got + res[k].lost, total) << k;
  }
  EXPECT_GT(res[3].lost, 0u) << "slow reader was expected to be lapped";
}

// Producer and consumer in different processes over a named POSIX shared-memory region.
TEST(Broadcast, CrossProcessNamedRegion) {
  const std::size_t n = 1 << 16, total = 1'000'000;
  const std::string name = "/shmcast-test-" + std::to_string(getpid());
  Region reg = Region::create(name, broadcast_bytes(n, 64));
  BroadcastWriter w(reg.data(), n, 64);
  int go[2];
  ASSERT_EQ(pipe(go), 0);
  const pid_t pid = fork();
  if (pid == 0) {
    Region ro = Region::open(name);
    BroadcastReader rd(ro.data(), ro.size());
    rd.seek(0);
    char c = 1;
    (void)!write(go[1], &c, 1);
    std::uint64_t buf[8], expect = 0;
    std::uint32_t len, type;
    while (expect < total) {
      const Poll p = rd.poll(buf, len, type);
      if (p == Poll::Message) {
        if (buf[0] != expect || !check_msg(buf, len, 6)) _exit(2);
        ++expect;
      } else if (p == Poll::Lapped) {
        _exit(3);
      }
    }
    _exit(0);
  }
  char c;
  ASSERT_EQ(read(go[0], &c, 1), 1);
  std::uint64_t msg[8];
  for (std::uint64_t s = 0; s < total; ++s) {
    // Paced well below the reader's speed so a correct reader is never lapped (a 65,536-slot ring
    // gives the child tens of milliseconds of slack even on a loaded machine).
    w.publish(msg, make_msg(s, msg, 6));
    if ((s & 255) == 0) std::this_thread::sleep_for(std::chrono::microseconds(20));
  }
  int st = 0;
  waitpid(pid, &st, 0);
  ASSERT_TRUE(WIFEXITED(st));
  EXPECT_EQ(WEXITSTATUS(st), 0) << "2 = corrupt message, 3 = lapped";
}

TEST(Broadcast, RejectsBadHeaders) {
  Ring r(64, 64);
  BroadcastWriter w(r.data(), 64, 64);
  EXPECT_NO_THROW(BroadcastReader(r.data(), r.bytes));
  EXPECT_THROW(BroadcastReader(r.data(), r.bytes - 64), std::runtime_error);  // nslots beyond mapping
  auto* h = static_cast<BroadcastHeader*>(r.data());
  h->nslots = 63;
  EXPECT_THROW(BroadcastReader(r.data(), r.bytes), std::runtime_error);
  h->nslots = 64;
  h->magic ^= 1;
  EXPECT_THROW(BroadcastReader(r.data(), r.bytes), std::runtime_error);
  EXPECT_THROW(BroadcastWriter(r.data(), 63, 64), std::invalid_argument);
}
