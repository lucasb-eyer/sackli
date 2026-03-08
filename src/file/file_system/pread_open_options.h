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

#ifndef SACKLI_SRC_FILE_FILE_SYSTEM_PREAD_OPEN_OPTIONS_H_
#define SACKLI_SRC_FILE_FILE_SYSTEM_PREAD_OPEN_OPTIONS_H_

namespace sackli {

// Hint for how records are expected to be read from files.
enum class AccessPattern {
  kSystem,      // Use the system default access pattern.
  kRandom,      // Reads are expected to be random.
  kSequential,  // Reads are expected to be sequential.
};

// Policy for how aggressively to retain or avoid read data in caches.
enum class CachePolicy {
  kSystem,         // Use the system default caching behavior.
  kDropAfterRead,  // Best-effort hint to avoid retaining data after reads.
  kDirectIo,       // Use direct I/O reads and avoid the page cache when
                   // possible.
};

// Generic hints for opening a file for parallel reads. Filesystems may ignore
// hints that they do not support.
struct PReadOpenOptions {
  AccessPattern access_pattern = AccessPattern::kSystem;
  CachePolicy cache_policy = CachePolicy::kSystem;

  // Best-effort hint to prefer on-demand streaming reads over whole-file
  // mappings or eager buffering.
  bool prefer_streaming = false;

  bool operator==(const PReadOpenOptions&) const = default;
};

}  // namespace sackli

#endif  // SACKLI_SRC_FILE_FILE_SYSTEM_PREAD_OPEN_OPTIONS_H_
