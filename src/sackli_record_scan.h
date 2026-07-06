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

#ifndef SACKLI_SRC_SACKLI_RECORD_SCAN_H_
#define SACKLI_SRC_SACKLI_RECORD_SCAN_H_

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "src/sackli_reader.h"

namespace sackli::internal {

// Reads all records of `reader` in order, in batches of roughly
// `batch_bytes` on-disk bytes, and invokes `callback(start_index, batch)` for
// each batch, where `batch[i]` is the record at `start_index + i`. For
// compressed files, the budget counts compressed bytes; decompressed records
// can require substantially more RAM. The callback owns the batch's records
// (it may move them out). Each batch is read with the reader's full
// parallelism, so peak memory stays at one batch plus whatever the callback
// retains, without serializing the scan.
inline absl::Status ScanRecords(
    const SackliReader& reader, size_t batch_bytes,
    absl::FunctionRef<void(size_t start_index, absl::Span<std::string> batch)>
        callback) {
  if (reader.IsClosed()) {
    return absl::FailedPreconditionError("Reader is closed.");
  }
  const size_t num_records = reader.size();
  // Estimate on-disk bytes per record. Each batch entry also pays a
  // std::string shell, which dominates for small uncompressed records; for
  // compressed files this remains only a sizing heuristic for RAM.
  const double estimated_bytes_per_record =
      std::max(reader.ApproximateNumBytesPerRecord(), 1.0) +
      sizeof(std::string);
  const size_t estimated_batch_records =
      std::max<size_t>(
          static_cast<size_t>(batch_bytes / estimated_bytes_per_record), 1);
  const size_t batch_records =
      std::min(estimated_batch_records, std::max<size_t>(num_records, 1));

  std::vector<std::string> batch;
  for (size_t start = 0; start < num_records; start += batch_records) {
    const size_t count = std::min(batch_records, num_records - start);
    batch.resize(count);
    if (absl::Status status = reader.ReadRangeWithAllocator(
            start, count,
            [&batch](size_t result_index, size_t record_size) {
              batch[result_index].resize(record_size);
              return absl::MakeSpan(batch[result_index]);
            });
        !status.ok()) {
      return status;
    }
    callback(start, absl::MakeSpan(batch));
  }
  return absl::OkStatus();
}

// Default batch size for index-building scans.
inline constexpr size_t kRecordScanBatchBytes = 64 * 1024 * 1024;  // 64 MiB

}  // namespace sackli::internal

#endif  // SACKLI_SRC_SACKLI_RECORD_SCAN_H_
