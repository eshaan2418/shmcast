// Single-producer, many-consumer broadcast ring in shared memory (market-data style).
//
// The producer never waits for consumers. Each slot carries a sequence word used as a seqlock:
// for message s the producer stores 2s+1 (writing), the payload, then 2s+2 (published). A
// consumer expecting message e reads the sequence, copies the payload, and re-reads the
// sequence:
//   seq <  2e+1          not published yet (or an older lap): poll again
//   seq == 2e+1          being written: poll again
//   seq == 2e+2 twice    a consistent copy of message e
//   seq >  2e+2          the producer has lapped this consumer: messages were overwritten
// A lapped consumer learns exactly how many messages it lost and resynchronises; it can never
// return a torn message. Payload words are accessed through relaxed std::atomic_ref, so the
// seqlock is race-free under the C++ memory model (and clean under ThreadSanitizer); on arm64
// and x86-64 those are plain loads and stores.
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>

namespace shmcast {

inline constexpr std::size_t kCacheLine = 128;  // Apple M-series; also covers x86 pair prefetch
inline constexpr std::uint64_t kMagic = 0x31545341434d4853ull;  // "SHMCAST1"

struct BroadcastHeader {
  std::uint64_t magic;
  std::uint32_t version;
  std::uint32_t slot_bytes;  // total bytes per slot (header + payload), multiple of 16
  std::uint64_t nslots;      // power of two
  std::uint64_t epoch;       // changes when a producer re-initialises the ring
  alignas(kCacheLine) std::atomic<std::uint64_t> next;  // messages published so far
  alignas(kCacheLine) char pad[kCacheLine];
};

struct SlotHeader {
  std::atomic<std::uint64_t> seq;
  std::uint32_t len;
  std::uint32_t type;
};
static_assert(sizeof(SlotHeader) == 16);

inline std::size_t broadcast_bytes(std::size_t nslots, std::size_t slot_bytes) {
  return sizeof(BroadcastHeader) + nslots * slot_bytes;
}

class BroadcastWriter {
 public:
  // Initialise a ring in [mem, mem + broadcast_bytes(nslots, slot_bytes)).
  BroadcastWriter(void* mem, std::size_t nslots, std::size_t slot_bytes, std::uint64_t epoch = 1)
      : h_(static_cast<BroadcastHeader*>(mem)), mask_(nslots - 1), slot_bytes_(slot_bytes) {
    if (!std::has_single_bit(nslots) || slot_bytes < 32 || slot_bytes % 16 != 0)
      throw std::invalid_argument("nslots must be a power of two, slot_bytes >= 32 and a multiple of 16");
    std::memset(mem, 0, broadcast_bytes(nslots, slot_bytes));
    new (&h_->next) std::atomic<std::uint64_t>(0);
    base_ = reinterpret_cast<std::byte*>(h_ + 1);
    for (std::size_t i = 0; i < nslots; ++i) new (&slot(i)->seq) std::atomic<std::uint64_t>(0);
    h_->magic = kMagic;
    h_->version = 1;
    h_->slot_bytes = static_cast<std::uint32_t>(slot_bytes);
    h_->nslots = nslots;
    h_->epoch = epoch;
    std::atomic_thread_fence(std::memory_order_release);
  }

  std::size_t max_payload() const { return slot_bytes_ - sizeof(SlotHeader); }

  // Publish one message (len <= max_payload()). Wait-free.
  void publish(const void* data, std::uint32_t len, std::uint32_t type = 0) {
    const std::uint64_t s = seq_++;
    SlotHeader* sh = slot(s & mask_);
    sh->seq.store(2 * s + 1, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);  // "writing" visible before the payload
    std::atomic_ref<std::uint32_t>(sh->len).store(len, std::memory_order_relaxed);
    std::atomic_ref<std::uint32_t>(sh->type).store(type, std::memory_order_relaxed);
    auto* w = reinterpret_cast<std::uint64_t*>(sh + 1);
    const auto* src = static_cast<const unsigned char*>(data);
    const std::uint32_t nw = (len + 7) / 8;
    for (std::uint32_t i = 0; i < nw; ++i) {
      std::uint64_t v = 0;
      std::memcpy(&v, src + 8 * i, i + 1 < nw ? 8 : len - 8 * i);
      std::atomic_ref<std::uint64_t>(w[i]).store(v, std::memory_order_relaxed);
    }
    sh->seq.store(2 * s + 2, std::memory_order_release);
    h_->next.store(s + 1, std::memory_order_release);
  }

  std::uint64_t published() const { return seq_; }

 private:
  SlotHeader* slot(std::size_t i) const { return reinterpret_cast<SlotHeader*>(base_ + i * slot_bytes_); }
  BroadcastHeader* h_;
  std::byte* base_ = nullptr;
  std::uint64_t mask_;
  std::size_t slot_bytes_;
  std::uint64_t seq_ = 0;
};

enum class Poll { Message, Empty, Lapped };

class BroadcastReader {
 public:
  // Attach to an initialised ring of `bytes` bytes. Validates the header against the mapping
  // size so a corrupt or hostile header cannot cause out-of-bounds reads.
  BroadcastReader(const void* mem, std::size_t bytes) : h_(static_cast<const BroadcastHeader*>(mem)) {
    // mmap'd regions are page-aligned; anything less aligned than the header type is not a ring.
    if (reinterpret_cast<std::uintptr_t>(mem) % alignof(BroadcastHeader) != 0)
      throw std::runtime_error("shmcast: ring memory is not 128-byte aligned");
    if (bytes < sizeof(BroadcastHeader) || h_->magic != kMagic || h_->version != 1)
      throw std::runtime_error("shmcast: not a broadcast ring");
    const std::uint64_t n = h_->nslots, sb = h_->slot_bytes;
    if (!std::has_single_bit(n) || sb < 32 || sb % 16 != 0 || n > (bytes - sizeof(BroadcastHeader)) / sb)
      throw std::runtime_error("shmcast: inconsistent ring header");
    mask_ = n - 1;
    slot_bytes_ = sb;
    epoch_ = h_->epoch;
    base_ = reinterpret_cast<const std::byte*>(h_ + 1);
    next_ = const_cast<BroadcastHeader*>(h_)->next.load(std::memory_order_acquire);  // join at the head
  }

  // Start reading from sequence s (e.g. 0 to read the ring's history if it has not wrapped).
  void seek(std::uint64_t s) { next_ = s; }
  std::uint64_t position() const { return next_; }
  std::uint64_t lost() const { return lost_; }
  // Convenience overload: the caller guarantees buf holds max_payload() bytes (check it once,
  // after attaching, before relying on this).
  Poll poll(void* buf, std::uint32_t& len, std::uint32_t& type) { return poll(buf, max_payload(), len, type); }
  std::size_t max_payload() const { return slot_bytes_ - sizeof(SlotHeader); }
  std::uint64_t epoch() const { return epoch_; }

  // Try to read the next message into buf, which holds `cap` bytes. A message longer than cap is
  // never copied: the slot geometry comes from the producer's header, so a corrupt or hostile
  // producer could otherwise advertise huge slots and overflow the caller's buffer. Such a
  // message is skipped and counted as lost. On Lapped, the reader has been moved to the oldest
  // message still safely in the ring and lost() was increased.
  Poll poll(void* buf, std::size_t cap, std::uint32_t& len, std::uint32_t& type) {
    const std::uint64_t e = next_;
    const SlotHeader* sh = slot(e & mask_);
    auto& seqw = const_cast<SlotHeader*>(sh)->seq;
    const std::uint64_t q1 = seqw.load(std::memory_order_acquire);
    if (q1 < 2 * e + 2) return Poll::Empty;
    if (q1 == 2 * e + 2) {
      auto* hs = const_cast<SlotHeader*>(sh);
      const std::uint32_t l = std::atomic_ref<std::uint32_t>(hs->len).load(std::memory_order_relaxed);
      const std::uint32_t t = std::atomic_ref<std::uint32_t>(hs->type).load(std::memory_order_relaxed);
      const std::size_t lim = cap < max_payload() ? cap : max_payload();
      const std::uint32_t cl = l <= lim ? l : static_cast<std::uint32_t>(lim);
      auto* w = reinterpret_cast<std::uint64_t*>(hs + 1);
      auto* dst = static_cast<unsigned char*>(buf);
      const std::uint32_t nw = (cl + 7) / 8;
      for (std::uint32_t i = 0; i < nw; ++i) {
        const std::uint64_t v = std::atomic_ref<std::uint64_t>(w[i]).load(std::memory_order_relaxed);
        std::memcpy(dst + 8 * i, &v, i + 1 < nw ? 8 : cl - 8 * i);
      }
      std::atomic_thread_fence(std::memory_order_acquire);  // payload loads before the re-check
      if (seqw.load(std::memory_order_relaxed) == q1) {
        if (l <= lim) {
          len = l;
          type = t;
          ++next_;
          return Poll::Message;
        }
        // Consistent but larger than the caller's buffer: skip exactly this message.
        ++next_;
        ++lost_;
        return Poll::Lapped;
      }
    }
    // Lapped (seq moved past our message before or during the copy), or a length that cannot
    // have come from a correct producer. Resynchronise to half a ring behind the head: far
    // enough from the producer that the next reads are unlikely to be lapped again.
    const std::uint64_t head = const_cast<BroadcastHeader*>(h_)->next.load(std::memory_order_acquire);
    const std::uint64_t n = mask_ + 1;
    const std::uint64_t target = head > n / 2 ? head - n / 2 : 0;
    const std::uint64_t resume = target > e ? target : e + 1;
    lost_ += resume - e;
    next_ = resume;
    return Poll::Lapped;
  }

 private:
  const SlotHeader* slot(std::size_t i) const { return reinterpret_cast<const SlotHeader*>(base_ + i * slot_bytes_); }
  const BroadcastHeader* h_;
  const std::byte* base_ = nullptr;
  std::uint64_t mask_ = 0;
  std::size_t slot_bytes_ = 0;
  std::uint64_t epoch_ = 0;
  std::uint64_t next_ = 0;
  std::uint64_t lost_ = 0;
};

}  // namespace shmcast
