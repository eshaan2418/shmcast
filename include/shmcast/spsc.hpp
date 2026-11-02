// Lossless single-producer single-consumer queue in shared memory (order-flow style): the
// producer waits (try_push fails) when the consumer is a full ring behind.
//
// head (consumer) and tail (producer) live on separate 128-byte lines; each side caches the
// other's index and re-reads it only when the cached value says the ring is full/empty, so in
// steady state a push or pop touches only its own line plus the slot.
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>

#include "shmcast/broadcast.hpp"

namespace shmcast {

struct SpscHeader {
  std::uint64_t magic;
  std::uint32_t slot_bytes;
  std::uint32_t pad0;
  std::uint64_t nslots;
  alignas(kCacheLine) std::atomic<std::uint64_t> tail;  // written by producer
  alignas(kCacheLine) std::atomic<std::uint64_t> head;  // written by consumer
  alignas(kCacheLine) char pad1[kCacheLine];
};

inline constexpr std::uint64_t kSpscMagic = 0x31435350534d4853ull;  // "SHMSPSC1"

inline std::size_t spsc_bytes(std::size_t nslots, std::size_t slot_bytes) { return sizeof(SpscHeader) + nslots * slot_bytes; }

inline void spsc_init(void* mem, std::size_t nslots, std::size_t slot_bytes) {
  if (!std::has_single_bit(nslots) || slot_bytes < 16 || slot_bytes % 8 != 0) throw std::invalid_argument("bad spsc geometry");
  std::memset(mem, 0, spsc_bytes(nslots, slot_bytes));
  auto* h = static_cast<SpscHeader*>(mem);
  new (&h->tail) std::atomic<std::uint64_t>(0);
  new (&h->head) std::atomic<std::uint64_t>(0);
  h->slot_bytes = static_cast<std::uint32_t>(slot_bytes);
  h->nslots = nslots;
  h->magic = kSpscMagic;
  std::atomic_thread_fence(std::memory_order_release);
}

class SpscEnd {
 protected:
  explicit SpscEnd(void* mem) : h_(static_cast<SpscHeader*>(mem)) {
    if (h_->magic != kSpscMagic) throw std::runtime_error("shmcast: not an spsc queue");
    mask_ = h_->nslots - 1;
    slot_bytes_ = h_->slot_bytes;
    base_ = reinterpret_cast<std::byte*>(h_ + 1);
  }
  std::byte* slot(std::uint64_t i) const { return base_ + (i & mask_) * slot_bytes_; }
  SpscHeader* h_;
  std::byte* base_;
  std::uint64_t mask_;
  std::size_t slot_bytes_;

 public:
  std::size_t max_payload() const { return slot_bytes_ - 8; }
};

class SpscProducer : public SpscEnd {
 public:
  explicit SpscProducer(void* mem) : SpscEnd(mem), tail_(h_->tail.load(std::memory_order_relaxed)), head_cache_(h_->head.load(std::memory_order_acquire)) {}
  bool try_push(const void* data, std::uint32_t len) {
    if (len > max_payload()) throw std::invalid_argument("message too large");
    if (tail_ - head_cache_ > mask_) {
      head_cache_ = h_->head.load(std::memory_order_acquire);
      if (tail_ - head_cache_ > mask_) return false;
    }
    std::byte* s = slot(tail_);
    std::memcpy(s, &len, 4);
    std::memcpy(s + 8, data, len);
    h_->tail.store(++tail_, std::memory_order_release);
    return true;
  }

 private:
  std::uint64_t tail_, head_cache_;
};

class SpscConsumer : public SpscEnd {
 public:
  explicit SpscConsumer(void* mem) : SpscEnd(mem), head_(h_->head.load(std::memory_order_relaxed)), tail_cache_(h_->tail.load(std::memory_order_acquire)) {}
  // Returns the payload length, or -1 if empty. buf must hold max_payload() bytes.
  int try_pop(void* buf) {
    if (head_ == tail_cache_) {
      tail_cache_ = h_->tail.load(std::memory_order_acquire);
      if (head_ == tail_cache_) return -1;
    }
    const std::byte* s = slot(head_);
    std::uint32_t len;
    std::memcpy(&len, s, 4);
    if (len > max_payload()) len = static_cast<std::uint32_t>(max_payload());
    std::memcpy(buf, s + 8, len);
    h_->head.store(++head_, std::memory_order_release);
    return static_cast<int>(len);
  }

 private:
  std::uint64_t head_, tail_cache_;
};

}  // namespace shmcast
