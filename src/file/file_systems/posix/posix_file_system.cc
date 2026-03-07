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

#include <algorithm>
#include <fcntl.h>
#include <glob.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
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
#include "src/file/file_system/file_system.h"
#include "src/file/file_system/pread_file.h"
#include "src/file/file_system/pread_open_options.h"
#include "src/file/file_system/shard_spec.h"
#include "src/file/file_system/write_file.h"

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

enum class PosixReadBackend {
  kMmap,
  kPRead,
};

PosixReadBackend SelectReadBackend(const PReadOpenOptions& options) {
  return options.prefer_streaming ||
                 options.cache_policy == CachePolicy::kDropAfterRead
             ? PosixReadBackend::kPRead
             : PosixReadBackend::kMmap;
}

absl::StatusOr<ScopedFd> OpenReadOnlyFd(const std::string& filename) {
  int fd = open(filename.c_str(), O_RDONLY);
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

class PosixFdPReadFile : public PReadFile {
 public:
  PosixFdPReadFile(ScopedFd fd, size_t size, bool drop_after_read)
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
  if (SelectReadBackend(options) == PosixReadBackend::kPRead) {
    ApplyFdAccessPattern(fd->get(), options.access_pattern);
    return std::make_unique<PosixFdPReadFile>(
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
