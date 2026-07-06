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

#include "src/sackli_multi_index.h"

#include <cstddef>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/sackli_reader.h"
#include "src/sackli_record_scan.h"

namespace sackli {

absl::StatusOr<SackliMultiIndex> SackliMultiIndex::Create(
    const SackliReader& reader) {
  absl::flat_hash_map<std::string, absl::InlinedVector<size_t, 1>> index;
  index.reserve(reader.size());
  if (absl::Status status = internal::ScanRecords(
          reader, internal::kRecordScanBatchBytes,
          [&index](size_t start_index, absl::Span<std::string> batch) {
            for (size_t i = 0; i < batch.size(); ++i) {
              index[std::move(batch[i])].push_back(start_index + i);
            }
          });
      !status.ok()) {
    return status;
  }
  return SackliMultiIndex(std::move(index));
}

}  // namespace sackli
