// libFuzzer target: a reader attached to arbitrary bytes (a corrupt or hostile producer) must
// either reject the header or poll without reading outside the mapping. ASan checks the bounds.
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "shmcast/broadcast.hpp"

using namespace shmcast;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size < sizeof(BroadcastHeader)) return 0;
  // Exact-size, suitably aligned copy so ASan flags any read past the end.
  std::vector<std::uint64_t> mem((size + 7) / 8);
  std::memcpy(mem.data(), data, size);
  auto* h = reinterpret_cast<BroadcastHeader*>(mem.data());
  h->magic = kMagic;  // get past the magic check so the fuzzer explores geometry and slots
  h->version = 1;
  try {
    BroadcastReader r(mem.data(), size);
    std::uint64_t buf[64];
    std::uint32_t len = 0, type = 0;
    for (int i = 0; i < 256; ++i) {
      if (r.poll(buf, len, type) == Poll::Message && len > r.max_payload()) __builtin_trap();
    }
  } catch (const std::runtime_error&) {
  }
  return 0;
}
