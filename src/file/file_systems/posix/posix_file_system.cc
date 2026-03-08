// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "src/file/file_systems/posix/posix_file_system.h"

#include <bit>
#include <algorithm>
#include <fcntl.h>
#include <glob.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/cleanup/cleanup.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "src/file/file_system/file_system.h"
#include "src/file/file_system/pread_file.h"
#include "src/file/file_system/pread_open_options.h"
#include "src/file/file_system/shard_spec.h"
#include "src/file/file_system/write_file.h"

#if defined(__linux__)
#include <linux/stat.h>
#endif

namespace sackli {
namespace {

class ScopedFd {
 public:
  ScopedFd() = default;
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  ScopedFd(ScopedFd&& other) noexcept : fd_(other.release()) {}
  ScopedFd& operator=(ScopedFd&& other) noexcept {
    if (this != &other) {
      if (fd_ >= 0) {
        close(fd_);
      }
      fd_ = other.release();
    }
    return *this;
  }

  int get() const { return fd_; }
  int release() {
    int fd = fd_;
    fd_ = -1;
    return fd;
  }

 private:
  int fd_ = -1;
};

class MmapDeleter {
 public:
  explicit MmapDeleter(size_t mmap_size) : mmap_size_(mmap_size) {}
  void operator()(const void* mmap) const {
    munmap(const_cast<void*>(mmap), mmap_size_);
  }
  size_t mmap_size() const { return mmap_size_; }

 private:
  size_t mmap_size_;
};

using MmapUniquePtr = std::unique_ptr<void, MmapDeleter>;

constexpr size_t kPReadChunkSize = 1 << 20;
constexpr size_t kMinDirectIoAlignment = 512;
enum class PosixReadBackend {
  kMmap,
  kPRead,
  kDirect,
};

PosixReadBackend SelectReadBackend(const PReadOpenOptions& options) {
  switch (options.cache_policy) {
    case CachePolicy::kDirectIo:
      return PosixReadBackend::kDirect;
    case CachePolicy::kDropAfterRead:
      return PosixReadBackend::kPRead;
    case CachePolicy::kSystem:
      return options.prefer_streaming ? PosixReadBackend::kPRead
                                      : PosixReadBackend::kMmap;
  }
  return options.prefer_streaming ? PosixReadBackend::kPRead
                                  : PosixReadBackend::kMmap;
}

absl::StatusOr<ScopedFd> OpenReadOnlyFd(const std::string& filename,
                                        int extra_flags = 0) {
  int fd = open(filename.c_str(), O_RDONLY | extra_flags);
  if (fd < 0) {
    return absl::ErrnoToStatus(errno, "open");
  }
  return ScopedFd(fd);
}

absl::StatusOr<size_t> FileSize(int fd) {
  struct stat stat;
  if (fstat(fd, &stat) < 0) {
    return absl::ErrnoToStatus(errno, "fstat");
  }
  return stat.st_size;
}

absl::StatusOr<MmapUniquePtr> MmapFile(int fd, size_t size) {
  // We cannot mmap an empty file but we can use an empty string_view.
  if (size == 0) {
    return MmapUniquePtr(nullptr, MmapDeleter(0));
  }

  void* records_mmap = mmap(/*addr=*/nullptr, /*length=*/size,
                            /*prot=*/PROT_READ, /*flags=*/MAP_SHARED,
                            /*fd=*/fd, /*offset=*/0);
  if (records_mmap == MAP_FAILED) {
    // kNotFound is confusing for user for ENODEV.
    if (errno == ENODEV) {
      return absl::PermissionDeniedError("mmap");
    } else {
      return absl::ErrnoToStatus(errno, "mmap");
    }
  }
  return MmapUniquePtr(records_mmap, MmapDeleter(size));
}

absl::Status BufferedPReadFully(int fd, size_t offset,
                                absl::Span<char> buffer) {
  size_t total_read = 0;
  while (total_read < buffer.size()) {
    ssize_t bytes_read;
    do {
      bytes_read =
          pread(fd, buffer.data() + total_read, buffer.size() - total_read,
                static_cast<off_t>(offset + total_read));
    } while (bytes_read < 0 && errno == EINTR);
    if (bytes_read < 0) {
      return absl::ErrnoToStatus(errno, "pread");
    }
    if (bytes_read == 0) {
      return absl::OutOfRangeError("Invalid read");
    }
    total_read += static_cast<size_t>(bytes_read);
  }
  return absl::OkStatus();
}

absl::Status DirectPReadExact(int fd, size_t offset, absl::Span<char> buffer) {
  while (true) {
    errno = 0;
    const ssize_t bytes_read =
        pread(fd, buffer.data(), buffer.size(), static_cast<off_t>(offset));
    if (bytes_read < 0 && errno == EINTR) {
      continue;
    }
    if (bytes_read < 0) {
      return absl::ErrnoToStatus(errno, "pread");
    }
    if (bytes_read == 0) {
      return absl::OutOfRangeError("Invalid read");
    }
    if (static_cast<size_t>(bytes_read) != buffer.size()) {
      return absl::FailedPreconditionError(
          absl::StrCat("Direct I/O short read at offset ", offset,
                       ": expected ", buffer.size(), " bytes but got ",
                       bytes_read, "."));
    }
    return absl::OkStatus();
  }
}

int MadviseAccessPattern(AccessPattern access_pattern) {
  switch (access_pattern) {
    case AccessPattern::kRandom:
      return MADV_RANDOM;
    case AccessPattern::kSequential:
      return MADV_SEQUENTIAL;
    case AccessPattern::kSystem:
      return -1;
  }
  return -1;
}

int FadviseAccessPattern(AccessPattern access_pattern) {
  switch (access_pattern) {
    case AccessPattern::kRandom:
      return POSIX_FADV_RANDOM;
    case AccessPattern::kSequential:
      return POSIX_FADV_SEQUENTIAL;
    case AccessPattern::kSystem:
      return -1;
  }
  return -1;
}

void ApplyMmapAccessPattern(void* data, size_t size,
                            AccessPattern access_pattern) {
  int advice = MadviseAccessPattern(access_pattern);
  if (data == nullptr || size == 0 || advice < 0) {
    return;
  }
  (void)madvise(data, size, advice);
}

void ApplyFdAccessPattern(int fd, AccessPattern access_pattern) {
  int advice = FadviseAccessPattern(access_pattern);
  if (advice < 0) {
    return;
  }
  (void)posix_fadvise(fd, 0, 0, advice);
}

size_t PageSize() {
  static const size_t page_size = []() {
    long size = sysconf(_SC_PAGESIZE);
    return size > 0 ? static_cast<size_t>(size) : size_t{4096};
  }();
  return page_size;
}

size_t AlignDown(size_t value, size_t alignment) {
  return value - value % alignment;
}

size_t AlignUp(size_t value, size_t alignment) {
  size_t remainder = value % alignment;
  if (remainder == 0) {
    return value;
  }
  return value + (alignment - remainder);
}

void DropFileCache(int fd, size_t file_size, size_t offset, size_t num_bytes) {
  if (num_bytes == 0) {
    return;
  }
  size_t page_size = PageSize();
  size_t begin = AlignDown(offset, page_size);
  size_t end = std::min(file_size, AlignUp(offset + num_bytes, page_size));
  if (end <= begin) {
    return;
  }
  (void)posix_fadvise(fd, static_cast<off_t>(begin),
                      static_cast<off_t>(end - begin), POSIX_FADV_DONTNEED);
}

class PosixMmapPReadFile : public PReadFile {
 public:
  explicit PosixMmapPReadFile(MmapUniquePtr mmap) : mmap_(std::move(mmap)) {}
  size_t size() const override { return mmap_.get_deleter().mmap_size(); }

  absl::Status PRead(
      size_t offset, size_t num_bytes,
      absl::FunctionRef<bool(absl::string_view)> callback) const override {
    size_t mmap_size = size();
    if (num_bytes > mmap_size || offset > mmap_size - num_bytes) {
      return absl::OutOfRangeError("Invalid read");
    }
    if (num_bytes == 0) {
      return absl::OkStatus();
    }
    callback(absl::string_view(static_cast<const char*>(mmap_.get()) + offset,
                               num_bytes));
    return absl::OkStatus();
  }

 private:
  MmapUniquePtr mmap_;
};

class PosixBufferedFdPReadFile : public PReadFile {
 public:
  PosixBufferedFdPReadFile(ScopedFd fd, size_t size, bool drop_after_read)
      : fd_(std::move(fd)),
        size_(size),
        drop_after_read_(drop_after_read) {}

  size_t size() const override { return size_; }

  absl::Status PRead(
      size_t offset, size_t num_bytes,
      absl::FunctionRef<bool(absl::string_view)> callback) const override {
    if (num_bytes > size_ || offset > size_ - num_bytes) {
      return absl::OutOfRangeError("Invalid read");
    }
    if (num_bytes == 0) {
      return absl::OkStatus();
    }

    const size_t buffer_size =
        std::min({num_bytes, kPReadChunkSize,
                  static_cast<size_t>(std::numeric_limits<ssize_t>::max())});
    std::string buffer(buffer_size, '\0');
    size_t total_read = 0;
    while (total_read < num_bytes) {
      size_t remaining = num_bytes - total_read;
      size_t chunk_offset = offset + total_read;
      size_t chunk_size = std::min(remaining, buffer.size());
      ssize_t bytes_read;
      do {
        bytes_read = pread(fd_.get(), buffer.data(), chunk_size,
                           static_cast<off_t>(chunk_offset));
      } while (bytes_read < 0 && errno == EINTR);
      if (bytes_read < 0) {
        return absl::ErrnoToStatus(errno, "pread");
      }
      if (bytes_read == 0) {
        return absl::OutOfRangeError("Invalid read");
      }

      const size_t chunk_bytes = static_cast<size_t>(bytes_read);
      total_read += chunk_bytes;
      const bool continue_reading =
          callback(absl::string_view(buffer.data(), chunk_bytes));
      if (drop_after_read_) {
        DropFileCache(fd_.get(), size_, chunk_offset, chunk_bytes);
      }
      if (!continue_reading) {
        return absl::OkStatus();
      }
    }
    return absl::OkStatus();
  }

 private:
  ScopedFd fd_;
  size_t size_;
  bool drop_after_read_;
};

class OwnedAlignedBuffer {
 public:
  OwnedAlignedBuffer() = default;

  static absl::StatusOr<OwnedAlignedBuffer> Allocate(size_t size,
                                                     size_t alignment) {
    void* data = nullptr;
    const size_t required_alignment = std::max(alignment, sizeof(void*));
    const int err = posix_memalign(&data, required_alignment, size);
    if (err != 0) {
      return absl::ErrnoToStatus(err, "posix_memalign");
    }
    return OwnedAlignedBuffer(static_cast<char*>(data), size);
  }

  ~OwnedAlignedBuffer() { std::free(data_); }

  OwnedAlignedBuffer(const OwnedAlignedBuffer&) = delete;
  OwnedAlignedBuffer& operator=(const OwnedAlignedBuffer&) = delete;

  OwnedAlignedBuffer(OwnedAlignedBuffer&& other) noexcept
      : data_(std::exchange(other.data_, nullptr)), size_(other.size_) {}

  OwnedAlignedBuffer& operator=(OwnedAlignedBuffer&& other) noexcept {
    if (this != &other) {
      std::free(data_);
      data_ = std::exchange(other.data_, nullptr);
      size_ = other.size_;
    }
    return *this;
  }

  char* data() const { return data_; }
  size_t size() const { return size_; }

 private:
  OwnedAlignedBuffer(char* data, size_t size) : data_(data), size_(size) {}

  char* data_ = nullptr;
  size_t size_ = 0;
};

#if defined(__linux__) && defined(O_DIRECT)

struct DirectIoAlignments {
  size_t mem_align;
  size_t read_align;
};

#if defined(STATX_DIOALIGN)
absl::StatusOr<DirectIoAlignments> QueryDirectIoAlignments(int fd) {
  struct statx statx_buffer = {};
  if (statx(fd, "", AT_EMPTY_PATH, STATX_DIOALIGN, &statx_buffer) != 0) {
    return absl::ErrnoToStatus(errno, "statx(STATX_DIOALIGN)");
  }
  if ((statx_buffer.stx_mask & STATX_DIOALIGN) == 0 ||
      statx_buffer.stx_dio_mem_align == 0 ||
      statx_buffer.stx_dio_offset_align == 0) {
    return absl::FailedPreconditionError(
        "Direct I/O alignment info unavailable for this file.");
  }

  // Some filesystems accept smaller alignments but still appear to service
  // O_DIRECT into page-aligned DMA units. Keep user buffers page-aligned even
  // when the reported minimum memory alignment is smaller.
  const size_t mem_align =
      std::max(static_cast<size_t>(statx_buffer.stx_dio_mem_align),
               PageSize());
  const size_t read_align =
      statx_buffer.stx_dio_read_offset_align != 0
          ? statx_buffer.stx_dio_read_offset_align
          : statx_buffer.stx_dio_offset_align;
  if (!std::has_single_bit(mem_align) || !std::has_single_bit(read_align)) {
    return absl::FailedPreconditionError(
        "Direct I/O alignments must be powers of two.");
  }
  return DirectIoAlignments{.mem_align = mem_align, .read_align = read_align};
}
#endif

absl::StatusOr<DirectIoAlignments> ConservativeDirectIoAlignments(int fd) {
  // Do not use st_blksize here: it is an efficiency hint and can be much
  // larger than the strict O_DIRECT alignment requirement (for example on
  // striped filesystems such as Lustre).
  size_t alignment = std::max(kMinDirectIoAlignment, PageSize());

  struct statvfs statvfs_buffer = {};
  // If statvfs fails, keep the page-sized fallback and let the safe probe
  // below validate or reject it during OpenPRead().
  if (fstatvfs(fd, &statvfs_buffer) == 0) {
    if (statvfs_buffer.f_bsize > 0) {
      alignment =
          std::max(alignment, static_cast<size_t>(statvfs_buffer.f_bsize));
    }
    if (statvfs_buffer.f_frsize > 0) {
      alignment =
          std::max(alignment, static_cast<size_t>(statvfs_buffer.f_frsize));
    }
  }

  if (!std::has_single_bit(alignment)) {
    alignment = std::bit_ceil(alignment);
  }
  return DirectIoAlignments{
      .mem_align = alignment,
      .read_align = alignment,
  };
}

absl::Status DirectIoProbeFailureStatus(const absl::Status& status,
                                        size_t alignment, size_t offset) {
  return absl::Status(
      status.code(),
      absl::StrCat("Direct I/O probe failed for alignment ", alignment,
                   " at offset ", offset, ": ", status.message()));
}

absl::StatusOr<DirectIoAlignments> ProbeDirectIoAlignments(
    int fd, size_t file_size, size_t start_alignment) {
  if (start_alignment == 0 || !std::has_single_bit(start_alignment)) {
    return absl::FailedPreconditionError(
        "Direct I/O probe requires a power-of-two start alignment.");
  }

  // Files smaller than the first aligned chunk never issue direct reads at a
  // nonzero offset because the whole file becomes tail-cached.
  if (file_size < start_alignment) {
    return DirectIoAlignments{
        .mem_align = start_alignment,
        .read_align = start_alignment,
    };
  }

  // This loop grows geometrically, so even very large files only require a
  // small number of probe attempts.
  for (size_t alignment = start_alignment; alignment <= file_size;) {
    bool try_next_alignment = false;
    absl::StatusOr<OwnedAlignedBuffer> buffer =
        OwnedAlignedBuffer::Allocate(alignment, alignment);
    if (!buffer.ok()) {
      return buffer.status();
    }

    absl::Status zero_offset_status =
        DirectPReadExact(fd, 0, absl::Span<char>(buffer->data(), alignment));
    if (!zero_offset_status.ok()) {
      if (zero_offset_status.code() == absl::StatusCode::kInvalidArgument) {
        try_next_alignment = true;
      } else {
        return DirectIoProbeFailureStatus(zero_offset_status, alignment, 0);
      }
    }
    if (try_next_alignment) {
      if (alignment > std::numeric_limits<size_t>::max() / 2) {
        break;
      }
      alignment <<= 1;
      continue;
    }

    // Once the remaining non-tail range is smaller than one aligned chunk, all
    // direct reads come from offset 0 and this alignment is safe even if a
    // larger nonzero offset would require stricter alignment.
    if (alignment > file_size / 2) {
      return DirectIoAlignments{
          .mem_align = alignment,
          .read_align = alignment,
      };
    }

    absl::Status nonzero_offset_status =
        DirectPReadExact(fd, alignment,
                         absl::Span<char>(buffer->data(), alignment));
    if (!nonzero_offset_status.ok()) {
      if (nonzero_offset_status.code() == absl::StatusCode::kInvalidArgument) {
        try_next_alignment = true;
      } else {
        return DirectIoProbeFailureStatus(nonzero_offset_status, alignment,
                                          alignment);
      }
    }
    if (try_next_alignment) {
      if (alignment > std::numeric_limits<size_t>::max() / 2) {
        break;
      }
      alignment <<= 1;
      continue;
    }
    return DirectIoAlignments{
        .mem_align = alignment,
        .read_align = alignment,
    };
  }

  return absl::FailedPreconditionError(
      "Direct I/O alignment probing did not find a usable alignment.");
}

absl::StatusOr<DirectIoAlignments> ResolveDirectIoAlignments(int fd,
                                                             size_t file_size) {
#if defined(STATX_DIOALIGN)
  absl::StatusOr<DirectIoAlignments> statx_alignments =
      QueryDirectIoAlignments(fd);
  if (statx_alignments.ok()) {
    return statx_alignments;
  }
#endif
  // Probe from a conservative page-aligned starting point. This preserves
  // open-time validation while avoiding the unsafe sub-page-aligned buffers
  // that some filesystems mishandle during O_DIRECT reads.
  absl::StatusOr<DirectIoAlignments> initial_alignment =
      ConservativeDirectIoAlignments(fd);
  if (!initial_alignment.ok()) {
    return initial_alignment.status();
  }
  return ProbeDirectIoAlignments(fd, file_size, initial_alignment->read_align);
}

absl::StatusOr<std::string> ReadTailWithoutCaching(const std::string& filename,
                                                   size_t file_size,
                                                   size_t tail_start) {
  if (tail_start >= file_size) {
    return std::string{};
  }

  absl::StatusOr<ScopedFd> buffered_fd = OpenReadOnlyFd(filename);
  if (!buffered_fd.ok()) {
    return buffered_fd.status();
  }
  ApplyFdAccessPattern(buffered_fd->get(), AccessPattern::kRandom);

  std::string tail_cache(file_size - tail_start, '\0');
  if (absl::Status status = BufferedPReadFully(
          buffered_fd->get(), tail_start,
          absl::Span<char>(tail_cache.data(), tail_cache.size()));
      !status.ok()) {
    return status;
  }
  DropFileCache(buffered_fd->get(), file_size, tail_start, tail_cache.size());
  return tail_cache;
}

class PosixDirectPReadFile : public PReadFile {
 public:
  PosixDirectPReadFile(ScopedFd fd, size_t size, size_t mem_align,
                       size_t read_align, std::string tail_cache)
      : fd_(std::move(fd)),
        size_(size),
        mem_align_(mem_align),
        read_align_(read_align),
        tail_start_(AlignDown(size, read_align)),
        tail_cache_(std::move(tail_cache)),
        max_chunk_size_(
            AlignUp(std::max(read_align, kPReadChunkSize), read_align)) {}

  size_t size() const override { return size_; }

  absl::Status PRead(
      size_t offset, size_t num_bytes,
      absl::FunctionRef<bool(absl::string_view)> callback) const override {
    if (num_bytes > size_ || offset > size_ - num_bytes) {
      return absl::OutOfRangeError("Invalid read");
    }
    if (num_bytes == 0) {
      return absl::OkStatus();
    }

    const size_t request_end = offset + num_bytes;
    const size_t direct_data_end = std::min(request_end, tail_start_);
    if (offset < direct_data_end) {
      const size_t aligned_begin = AlignDown(offset, read_align_);
      const size_t aligned_end = AlignUp(direct_data_end, read_align_);
      for (size_t chunk_begin = aligned_begin; chunk_begin < aligned_end;
           chunk_begin += max_chunk_size_) {
        const size_t chunk_end =
            std::min(aligned_end, chunk_begin + max_chunk_size_);
        const size_t chunk_size = chunk_end - chunk_begin;
        absl::StatusOr<OwnedAlignedBuffer> buffer =
            OwnedAlignedBuffer::Allocate(chunk_size, mem_align_);
        if (!buffer.ok()) {
          return buffer.status();
        }
        if (absl::Status status =
                DirectPReadExact(fd_.get(), chunk_begin,
                                 absl::Span<char>(buffer->data(),
                                                  buffer->size()));
            !status.ok()) {
          return status;
        }

        const size_t piece_begin = std::max(offset, chunk_begin);
        const size_t piece_end = std::min(direct_data_end, chunk_end);
        if (piece_begin < piece_end &&
            !callback(absl::string_view(buffer->data(), buffer->size())
                          .substr(piece_begin - chunk_begin,
                                  piece_end - piece_begin))) {
          return absl::OkStatus();
        }
      }
    }

    if (request_end > tail_start_) {
      const size_t tail_piece_begin = std::max(offset, tail_start_);
      const size_t tail_piece_end = request_end;
      if (tail_piece_begin < tail_piece_end &&
          !callback(absl::string_view(tail_cache_).substr(
              tail_piece_begin - tail_start_,
              tail_piece_end - tail_piece_begin))) {
        return absl::OkStatus();
      }
    }
    return absl::OkStatus();
  }

 private:
  ScopedFd fd_;
  size_t size_;
  size_t mem_align_;
  size_t read_align_;
  size_t tail_start_;
  std::string tail_cache_;
  size_t max_chunk_size_;
};

#endif

class PosixWriteFile : public WriteFile {
 public:
  using File =
      std::unique_ptr<std::FILE,
                      decltype([](std::FILE* file) { std::fclose(file); })>;
  explicit PosixWriteFile(File file) : file_(std::move(file)) {}
  absl::Status Write(absl::string_view data) override {
    size_t written = std::fwrite(data.data(), 1, data.size(), file_.get());
    if (written != data.size()) {
      return absl::ErrnoToStatus(errno, "Failed to write to file");
    }
    return absl::OkStatus();
  }
  absl::Status Flush() override {
    if (std::fflush(file_.get()) != 0) {
      return absl::ErrnoToStatus(errno, "Failed to flush file");
    }
    return absl::OkStatus();
  }
  absl::Status Close() override {
    if (file_ == nullptr) {
      return absl::OkStatus();
    }
    absl::Status status = absl::OkStatus();
    if (std::fclose(file_.release()) != 0) {
      status = absl::ErrnoToStatus(errno, "Failed to close file");
    }
    return status;
  }

 private:
  File file_;
};

}  // namespace

absl::StatusOr<absl_nonnull std::unique_ptr<WriteFile>>
PosixFileSystem::OpenWrite(absl::string_view filename, uint64_t offset,
                           absl::string_view options) const {
  std::string filename_str(filename);
  PosixWriteFile::File file;
  if (offset > 0) {
    file.reset(std::fopen(filename_str.c_str(), "ab"));
    if (file == nullptr) {
      return absl::ErrnoToStatus(errno, "Failed to open file");
    }
    if (std::fseek(file.get(), 0, SEEK_END) != 0) {
      return absl::ErrnoToStatus(errno, "Failed to seek to end of file");
    }
    if (size_t file_size = std::ftell(file.get()); file_size < offset) {
      return absl::OutOfRangeError(
          absl::StrCat("Invalid offset: ", offset, " > ", file_size));
    }
    if (ftruncate(fileno(file.get()), offset) != 0) {
      return absl::ErrnoToStatus(errno, "Failed to truncate file");
    }
    if (std::fseek(file.get(), offset, SEEK_SET) != 0) {
      return absl::ErrnoToStatus(errno, "Failed to seek file");
    }
  } else {
    file.reset(std::fopen(filename_str.c_str(), "wb"));
    if (file == nullptr) {
      return absl::ErrnoToStatus(errno, "Failed to open file");
    }
  }
  return std::make_unique<PosixWriteFile>(std::move(file));
}

absl::StatusOr<absl_nonnull std::unique_ptr<PReadFile>>
PosixFileSystem::OpenPRead(absl::string_view filename,
                           const PReadOpenOptions& options) const {
  std::string filename_str(filename);
  const PosixReadBackend backend = SelectReadBackend(options);
  if (backend == PosixReadBackend::kDirect) {
#if defined(__linux__) && defined(O_DIRECT)
    absl::StatusOr<ScopedFd> direct_fd = OpenReadOnlyFd(filename_str, O_DIRECT);
    if (!direct_fd.ok()) {
      return absl::Status(
          direct_fd.status().code(),
          absl::StrCat("Direct I/O open failed: ",
                       direct_fd.status().message()));
    }
    absl::StatusOr<size_t> size = FileSize(direct_fd->get());
    if (!size.ok()) {
      return size.status();
    }
    absl::StatusOr<DirectIoAlignments> alignments =
        ResolveDirectIoAlignments(direct_fd->get(), *size);
    if (!alignments.ok()) {
      return alignments.status();
    }
    ApplyFdAccessPattern(direct_fd->get(), options.access_pattern);
    const size_t tail_start = AlignDown(*size, alignments->read_align);
    absl::StatusOr<std::string> tail_cache =
        ReadTailWithoutCaching(filename_str, *size, tail_start);
    if (!tail_cache.ok()) {
      return tail_cache.status();
    }
    return std::make_unique<PosixDirectPReadFile>(
        *std::move(direct_fd), *size, alignments->mem_align,
        alignments->read_align, *std::move(tail_cache));
#else
    return absl::UnimplementedError(
        "Direct I/O backend requires Linux O_DIRECT support.");
#endif
  }

  absl::StatusOr<ScopedFd> fd = OpenReadOnlyFd(filename_str);
  if (!fd.ok()) {
    return fd.status();
  }
  absl::StatusOr<size_t> size = FileSize(fd->get());
  if (!size.ok()) {
    return size.status();
  }

  const bool drop_after_read =
      options.cache_policy == CachePolicy::kDropAfterRead;
  if (backend == PosixReadBackend::kPRead) {
    ApplyFdAccessPattern(fd->get(), options.access_pattern);
    return std::make_unique<PosixBufferedFdPReadFile>(
        *std::move(fd), *size, drop_after_read);
  }

  absl::StatusOr<MmapUniquePtr> mmap = MmapFile(fd->get(), *size);
  if (!mmap.ok()) {
    return mmap.status();
  }
  ApplyMmapAccessPattern(mmap->get(), *size, options.access_pattern);
  return std::make_unique<PosixMmapPReadFile>(*std::move(mmap));
}

absl::Status PosixFileSystem::Delete(absl::string_view filename,
                                     absl::string_view options) const {
  std::string filename_str(filename);
  if (std::remove(filename_str.c_str()) != 0) {
    return absl::ErrnoToStatus(errno, "Failed to delete file");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<absl_nonnull std::unique_ptr<PReadFile>>>
PosixFileSystem::BulkOpenPRead(absl::string_view filespec_without_prefix,
                               const PReadOpenOptions& options) const {
  std::string filespec = CanonicaliseShardSpec(
      filespec_without_prefix, [](const std::string& pattern) {
        glob_t glob_result;
        absl::Cleanup cleanup = [&glob_result] { globfree(&glob_result); };
        int return_value =
            glob(pattern.c_str(), GLOB_NOESCAPE, nullptr, &glob_result);
        if (return_value != 0 || glob_result.gl_pathc == 0) {
          return std::string{};
        }
        return std::string(glob_result.gl_pathv[glob_result.gl_pathc - 1]);
      });

  return FileSystem::BulkOpenPRead(filespec, options);
}

bool PosixFileSystem::NeedsDistinctPReadHandles(
    const PReadOpenOptions& first, const PReadOpenOptions& second) const {
  return SelectReadBackend(first) != SelectReadBackend(second) ||
         first.access_pattern != second.access_pattern ||
         first.cache_policy != second.cache_policy;
}

}  // namespace sackli
