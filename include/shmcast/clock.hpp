// Timestamps that are comparable across processes on one machine.
#pragma once

#include <cstdint>
#include <ctime>

#if defined(__APPLE__)
#include <pthread.h>
#include <sys/qos.h>
#endif

namespace shmcast {

// Nanoseconds on a monotonic clock shared by all processes (CLOCK_MONOTONIC_RAW / UPTIME_RAW).
// On Apple Silicon this is derived from the 24 MHz system counter: it reports nanoseconds but
// advances in ~41.7 ns steps, which bounds the resolution of one-way latency histograms.
inline std::uint64_t now_ns() {
#if defined(__APPLE__)
  return clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
#else
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return static_cast<std::uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<std::uint64_t>(ts.tv_nsec);
#endif
}

// Ask the scheduler for a performance core. macOS has no thread-affinity API on Apple Silicon;
// the highest QoS class is the supported way to steer a thread onto a P-core.
inline void prefer_performance_core() {
#if defined(__APPLE__)
  pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#endif
}

inline void cpu_relax() {
#if defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#elif defined(__x86_64__)
  __builtin_ia32_pause();
#endif
}

}  // namespace shmcast
