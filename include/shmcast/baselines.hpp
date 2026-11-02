// Kernel transports used as baselines: pipe, Unix-domain stream socket, TCP loopback
// (TCP_NODELAY). Each is a bidirectional byte channel between a parent and a forked child.
#pragma once

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>

namespace shmcast {

struct Channel {
  int a_rd = -1, a_wr = -1;  // parent side
  int b_rd = -1, b_wr = -1;  // child side
};

inline Channel make_pipe_channel() {
  int p1[2], p2[2];
  if (pipe(p1) || pipe(p2)) throw std::runtime_error("pipe");
  return {p2[0], p1[1], p1[0], p2[1]};
}

inline Channel make_uds_channel() {
  int sv[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv)) throw std::runtime_error("socketpair");
  return {sv[0], sv[0], sv[1], sv[1]};
}

inline Channel make_tcp_channel() {
  const int ls = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  a.sin_port = 0;
  socklen_t al = sizeof a;
  if (bind(ls, reinterpret_cast<sockaddr*>(&a), sizeof a) || listen(ls, 1) || getsockname(ls, reinterpret_cast<sockaddr*>(&a), &al))
    throw std::runtime_error("tcp listen");
  const int c = socket(AF_INET, SOCK_STREAM, 0);
  if (connect(c, reinterpret_cast<sockaddr*>(&a), sizeof a)) throw std::runtime_error("tcp connect");
  const int s = accept(ls, nullptr, nullptr);
  close(ls);
  int one = 1;
  setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  return {c, c, s, s};
}

inline void write_all(int fd, const void* p, std::size_t n) {
  auto* b = static_cast<const char*>(p);
  while (n) {
    const ssize_t w = write(fd, b, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error(std::string("write: ") + std::strerror(errno));
    }
    b += w;
    n -= static_cast<std::size_t>(w);
  }
}

inline bool read_all(int fd, void* p, std::size_t n) {
  auto* b = static_cast<char*>(p);
  while (n) {
    const ssize_t r = read(fd, b, n);
    if (r == 0) return false;
    if (r < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error(std::string("read: ") + std::strerror(errno));
    }
    b += r;
    n -= static_cast<std::size_t>(r);
  }
  return true;
}

}  // namespace shmcast
