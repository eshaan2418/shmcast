// libFuzzer target: a reader attached to arbitrary bytes (a corrupt or hostile producer) must
// either reject the header or poll without reading outside the mapping. ASan checks the bounds.
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "shmcast/broadcast.hpp"

using namespace shmcast;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size < sizeof(BroadcastHeader)) return 0;
  // 128-byte-aligned copy (as mmap'd memory would be), sized exactly so ASan flags any read
  // past the end of the mapping.
  const std::size_t alloc = (size + 127) / 128 * 128;
  void* raw = std::aligned_alloc(128, alloc);
  std::memset(raw, 0, alloc);
  std::memcpy(raw, data, size);
  auto* h = static_cast<BroadcastHeader*>(raw);
  h->magic = kMagic;  // get past the magic check so the fuzzer explores geometry and slots
  h->version = 1;
  try {
    BroadcastReader r(raw, size);
    std::uint64_t buf[64];
    std::uint32_t len = 0, type = 0;
    for (int i = 0; i < 256; ++i) {
      if (r.poll(buf, sizeof buf, len, type) == Poll::Message && len > sizeof buf) __builtin_trap();
    }
  } catch (const std::runtime_error&) {
  }
  std::free(raw);
  return 0;
}
