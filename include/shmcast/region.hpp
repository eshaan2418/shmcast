// A named POSIX shared-memory region (shm_open + mmap) or an anonymous shared mapping that
// survives fork. RAII: unmaps on destruction; the creator unlinks the name.
#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

namespace shmcast {

class Region {
 public:
  Region() = default;
  // Create (and size) a named region; fails if it exists unless replace=true.
  static Region create(const std::string& name, std::size_t bytes, bool replace = true) {
    if (replace) shm_unlink(name.c_str());
    const int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) throw std::runtime_error("shm_open(create " + name + "): " + std::strerror(errno));
    if (ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
      const int e = errno;
      close(fd);
      shm_unlink(name.c_str());
      throw std::runtime_error("ftruncate: " + std::string(std::strerror(e)));
    }
    Region r = map(fd, bytes, true);
    r.name_ = name;
    r.owner_ = true;
    return r;
  }
  // Open an existing named region; its size is taken from the object.
  static Region open(const std::string& name, bool writable = false) {
    const int fd = shm_open(name.c_str(), writable ? O_RDWR : O_RDONLY, 0600);
    if (fd < 0) throw std::runtime_error("shm_open(open " + name + "): " + std::strerror(errno));
    struct stat st {};
    if (fstat(fd, &st) != 0) {
      close(fd);
      throw std::runtime_error("fstat failed");
    }
    Region r = map(fd, static_cast<std::size_t>(st.st_size), writable);
    r.name_ = name;
    return r;
  }
  // Anonymous MAP_SHARED mapping: shared with children created by fork() afterwards.
  static Region anonymous(std::size_t bytes) {
    void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    if (p == MAP_FAILED) throw std::runtime_error("mmap(anon): " + std::string(std::strerror(errno)));
    Region r;
    r.base_ = p;
    r.size_ = bytes;
    return r;
  }

  Region(Region&& o) noexcept { *this = std::move(o); }
  Region& operator=(Region&& o) noexcept {
    if (this != &o) {
      reset();
      base_ = std::exchange(o.base_, nullptr);
      size_ = std::exchange(o.size_, 0);
      name_ = std::move(o.name_);
      owner_ = std::exchange(o.owner_, false);
    }
    return *this;
  }
  Region(const Region&) = delete;
  Region& operator=(const Region&) = delete;
  ~Region() { reset(); }

  void* data() const { return base_; }
  std::size_t size() const { return size_; }
  // Give up ownership of the name (it will not be unlinked by this object).
  void release_name() { owner_ = false; }

 private:
  static Region map(int fd, std::size_t bytes, bool writable) {
    void* p = mmap(nullptr, bytes, writable ? PROT_READ | PROT_WRITE : PROT_READ, MAP_SHARED, fd, 0);
    const int e = errno;
    close(fd);
    if (p == MAP_FAILED) throw std::runtime_error("mmap: " + std::string(std::strerror(e)));
    Region r;
    r.base_ = p;
    r.size_ = bytes;
    return r;
  }
  void reset() {
    if (base_) munmap(base_, size_);
    if (owner_ && !name_.empty()) shm_unlink(name_.c_str());
    base_ = nullptr;
    size_ = 0;
    owner_ = false;
  }
  void* base_ = nullptr;
  std::size_t size_ = 0;
  std::string name_;
  bool owner_ = false;
};

}  // namespace shmcast
