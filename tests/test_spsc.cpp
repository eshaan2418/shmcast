#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <thread>
#include <vector>

#include "shmcast/baselines.hpp"
#include "shmcast/region.hpp"
#include "shmcast/spsc.hpp"

using namespace shmcast;

TEST(Spsc, FullAndEmpty) {
  std::vector<std::uint64_t> mem(spsc_bytes(8, 32) / 8);
  spsc_init(mem.data(), 8, 32);
  SpscProducer p(mem.data());
  SpscConsumer c(mem.data());
  char buf[32];
  EXPECT_EQ(c.try_pop(buf), -1);
  for (int i = 0; i < 8; ++i) EXPECT_TRUE(p.try_push(&i, sizeof i));
  int x = 99;
  EXPECT_FALSE(p.try_push(&x, sizeof x));
  for (int i = 0; i < 8; ++i) {
    ASSERT_EQ(c.try_pop(buf), 4);
    int v;
    std::memcpy(&v, buf, 4);
    EXPECT_EQ(v, i);
  }
  EXPECT_THROW(p.try_push(buf, 25), std::invalid_argument);
}

TEST(Spsc, ThreadsLosslessInOrder) {
  const std::uint64_t total = 5'000'000;
  std::vector<std::uint64_t> mem(spsc_bytes(1024, 32) / 8);
  spsc_init(mem.data(), 1024, 32);
  std::thread prod([&] {
    SpscProducer p(mem.data());
    for (std::uint64_t i = 0; i < total; ++i) {
      const std::uint64_t m[2] = {i, ~i};
      while (!p.try_push(m, 16)) {
      }
    }
  });
  SpscConsumer c(mem.data());
  std::uint64_t buf[3], expect = 0;
  while (expect < total) {
    if (c.try_pop(buf) == 16) {
      ASSERT_EQ(buf[0], expect);
      ASSERT_EQ(buf[1], ~expect);
      ++expect;
    }
  }
  prod.join();
}

TEST(Spsc, CrossProcessLossless) {
  const std::uint64_t total = 2'000'000;
  Region reg = Region::anonymous(spsc_bytes(4096, 32));
  spsc_init(reg.data(), 4096, 32);
  const pid_t pid = fork();
  if (pid == 0) {
    SpscConsumer c(reg.data());
    std::uint64_t buf[3], expect = 0;
    while (expect < total) {
      if (c.try_pop(buf) == 16) {
        if (buf[0] != expect || buf[1] != expect * 7) _exit(2);
        ++expect;
      }
    }
    _exit(0);
  }
  SpscProducer p(reg.data());
  for (std::uint64_t i = 0; i < total; ++i) {
    const std::uint64_t m[2] = {i, i * 7};
    while (!p.try_push(m, 16)) {
    }
  }
  int st = 0;
  waitpid(pid, &st, 0);
  ASSERT_TRUE(WIFEXITED(st));
  EXPECT_EQ(WEXITSTATUS(st), 0);
}

TEST(Baselines, ChannelsRoundTrip) {
  for (auto mk : {make_pipe_channel, make_uds_channel, make_tcp_channel}) {
    Channel ch = mk();
    const char msg[5] = "ping";
    char got[5] = {};
    write_all(ch.a_wr, msg, 5);
    ASSERT_TRUE(read_all(ch.b_rd, got, 5));
    EXPECT_STREQ(got, "ping");
    write_all(ch.b_wr, "pong", 5);
    ASSERT_TRUE(read_all(ch.a_rd, got, 5));
    EXPECT_STREQ(got, "pong");
    std::vector<int> fds{ch.a_rd, ch.a_wr, ch.b_rd, ch.b_wr};
    std::sort(fds.begin(), fds.end());
    fds.erase(std::unique(fds.begin(), fds.end()), fds.end());
    for (int fd : fds) close(fd);
  }
}
