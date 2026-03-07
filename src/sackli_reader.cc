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

#include "src/sackli_reader.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/call_once.h"
#include "absl/functional/any_invocable.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "src/file/file.h"
#include "src/file/file_system/pread_open_options.h"
#include "src/file/file_system/pread_file.h"
#include "src/file/registry/file_system_registry.h"
#include "src/internal/sackli_shard_reader.h"
#include "src/internal/limits_name.h"
#include "src/internal/parallel_do.h"
#include "src/internal/records_limits.h"
#include "src/internal/recycling_pool.h"
#include "src/internal/shard_indexing.h"
#include "src/internal/zstd_decompressor.h"
#include "zstd.h"

namespace sackli {
namespace {

struct ReaderOpenHandles {
  std::unique_ptr<PReadFile> records;
  // For kSeparate this is the limits file; for kTail this may be an alternate
  // view of the record file used to derive the limits view.
  std::unique_ptr<PReadFile> limits_source;
};

using ReaderHandleBatch = std::vector<ReaderOpenHandles>;
using ReaderFilePair =
    std::pair<std::unique_ptr<PReadFile>, std::unique_ptr<PReadFile>>;

PReadOpenOptions RecordFileOpenOptions(const SackliReader::Options& options) {
  return PReadOpenOptions{
      .access_pattern = options.access_pattern,
      .cache_policy = options.cache_policy,
      .prefer_streaming =
          options.cache_policy == CachePolicy::kDropAfterRead,
  };
}

PReadOpenOptions TailLimitSourceOpenOptions(
    const SackliReader::Options& options) {
  return PReadOpenOptions{
      .access_pattern = options.access_pattern,
  };
}

absl::StatusOr<bool> FileSpecNeedsSeparateTailLimitSourceHandle(
    absl::string_view file_spec, const SackliReader::Options& options) {
  const PReadOpenOptions record_options = RecordFileOpenOptions(options);
  const PReadOpenOptions tail_limit_source_options =
      TailLimitSourceOpenOptions(options);
  if (record_options == tail_limit_source_options) {
    return false;
  }
  absl::StatusOr<ResolvedFile> resolved_file =
      FileSystemRegistry::Instance().Resolve(file_spec);
  if (!resolved_file.ok()) {
    return resolved_file.status();
  }
  return resolved_file->file_system->NeedsDistinctPReadHandles(
      record_options, tail_limit_source_options);
}

std::string LimitsFileSpec(absl::string_view file_spec) {
  const auto [prefix, filename] = SplitPrefixAndFilename(file_spec);
  const std::string limits_name = internal::LimitsName(filename);
  if (prefix.empty()) {
    return limits_name;
  }
  return absl::StrCat(prefix, limits_name);
}

std::string LimitsFileSpecList(absl::Span<const std::string> file_specs) {
  std::vector<std::string> limits_file_specs;
  limits_file_specs.reserve(file_specs.size());
  for (absl::string_view file_spec : file_specs) {
    limits_file_specs.push_back(LimitsFileSpec(file_spec));
  }
  return absl::StrJoin(limits_file_specs, ",");
}

void AppendReaderHandleBatch(ReaderHandleBatch& destination,
                             ReaderHandleBatch source) {
  destination.reserve(destination.size() + source.size());
  destination.insert(destination.end(), std::make_move_iterator(source.begin()),
                     std::make_move_iterator(source.end()));
}

absl::StatusOr<ReaderHandleBatch> MakeReaderOpenHandles(
    absl::Span<std::unique_ptr<PReadFile>> record_files,
    absl::Span<std::unique_ptr<PReadFile>> limits_source_files) {
  if (record_files.size() != limits_source_files.size()) {
    return absl::InvalidArgumentError(
        "record_files.size() must equal limits_source_files.size().");
  }

  ReaderHandleBatch handle_batch;
  handle_batch.reserve(record_files.size());
  for (size_t i = 0; i < record_files.size(); ++i) {
    if (record_files[i] == nullptr) {
      return absl::InternalError(
          absl::StrCat("Record handle must be non-null at file ", i, "."));
    }
    handle_batch.push_back(ReaderOpenHandles{
        .records = std::move(record_files[i]),
        .limits_source = std::move(limits_source_files[i]),
    });
  }
  return handle_batch;
}

absl::StatusOr<ReaderHandleBatch> OpenFilesForFileSpecBatch(
    absl::Span<const std::string> file_specs,
    bool use_separate_tail_limit_source_handles,
    const SackliReader::Options& options) {
  const std::string batch_file_spec = absl::StrJoin(file_specs, ",");
  absl::StatusOr<std::vector<std::unique_ptr<PReadFile>>> record_files =
      file::BulkOpenPRead(batch_file_spec, RecordFileOpenOptions(options));
  if (!record_files.ok()) {
    return record_files.status();
  }

  if (options.limits_placement == LimitsPlacement::kSeparate) {
    absl::StatusOr<std::vector<std::unique_ptr<PReadFile>>> limits_files =
        file::BulkOpenPRead(LimitsFileSpecList(file_specs));
    if (!limits_files.ok()) {
      return limits_files.status();
    }
    return MakeReaderOpenHandles(absl::MakeSpan(*record_files),
                                 absl::MakeSpan(*limits_files));
  }

  std::vector<std::unique_ptr<PReadFile>> limits_source_files(
      record_files->size());
  if (use_separate_tail_limit_source_handles) {
    absl::StatusOr<std::vector<std::unique_ptr<PReadFile>>>
        opened_tail_limit_source_files = file::BulkOpenPRead(
            batch_file_spec, TailLimitSourceOpenOptions(options));
    if (!opened_tail_limit_source_files.ok()) {
      return opened_tail_limit_source_files.status();
    }
    if (opened_tail_limit_source_files->size() != record_files->size()) {
      return absl::InternalError(
          "Tail limits source handles must match record handles.");
    }
    limits_source_files = std::move(*opened_tail_limit_source_files);
  }
  return MakeReaderOpenHandles(absl::MakeSpan(*record_files),
                               absl::MakeSpan(limits_source_files));
}

absl::StatusOr<ReaderHandleBatch> OpenFilesForFileSpecList(
    absl::string_view filespec, const SackliReader::Options& options) {
  std::vector<std::string> file_specs;
  for (absl::string_view file_spec :
       absl::StrSplit(filespec, ',', absl::SkipEmpty())) {
    file_specs.push_back(std::string(file_spec));
  }

  if (options.limits_placement != LimitsPlacement::kTail) {
    return OpenFilesForFileSpecBatch(absl::MakeConstSpan(file_specs),
                                     /*use_separate_tail_limit_source_handles=*/
                                     false, options);
  }

  ReaderHandleBatch handle_batch;
  std::vector<std::string> batch_file_specs;
  bool batch_uses_separate_tail_limit_source_handles = false;

  auto flush_batch = [&](bool use_separate_tail_limit_source_handles)
      -> absl::StatusOr<ReaderHandleBatch> {
    if (batch_file_specs.empty()) {
      return ReaderHandleBatch{};
    }
    absl::StatusOr<ReaderHandleBatch> batch_handles = OpenFilesForFileSpecBatch(
        absl::MakeConstSpan(batch_file_specs),
        use_separate_tail_limit_source_handles, options);
    batch_file_specs.clear();
    return batch_handles;
  };

  for (const std::string& file_spec : file_specs) {
    absl::StatusOr<bool> use_separate_tail_limit_source_handles =
        FileSpecNeedsSeparateTailLimitSourceHandle(file_spec, options);
    if (!use_separate_tail_limit_source_handles.ok()) {
      return use_separate_tail_limit_source_handles.status();
    }

    if (!batch_file_specs.empty() &&
        *use_separate_tail_limit_source_handles !=
            batch_uses_separate_tail_limit_source_handles) {
      absl::StatusOr<ReaderHandleBatch> batch_handles = flush_batch(
          batch_uses_separate_tail_limit_source_handles);
      if (!batch_handles.ok()) {
        return batch_handles.status();
      }
      AppendReaderHandleBatch(handle_batch, std::move(*batch_handles));
    }
    if (batch_file_specs.empty()) {
      batch_uses_separate_tail_limit_source_handles =
          *use_separate_tail_limit_source_handles;
    }
    batch_file_specs.push_back(std::string(file_spec));
  }

  absl::StatusOr<ReaderHandleBatch> batch_handles =
      flush_batch(batch_uses_separate_tail_limit_source_handles);
  if (!batch_handles.ok()) {
    return batch_handles.status();
  }
  AppendReaderHandleBatch(handle_batch, std::move(*batch_handles));
  return handle_batch;
}

absl::StatusOr<std::vector<ReaderFilePair>> ResolveFilePairs(
    ReaderHandleBatch open_handles, const SackliReader::Options& options) {
  std::vector<ReaderFilePair> file_pairs;
  if (options.limits_placement == LimitsPlacement::kSeparate) {
    file_pairs.reserve(open_handles.size());
    for (size_t i = 0; i < open_handles.size(); ++i) {
      ReaderOpenHandles& handles = open_handles[i];
      if (handles.records == nullptr || handles.limits_source == nullptr) {
        return absl::InternalError(
            absl::StrCat("Record and limits handles must be non-null at file ",
                         i, "."));
      }
      file_pairs.push_back(
          {std::move(handles.records), std::move(handles.limits_source)});
    }
    return file_pairs;
  }

  file_pairs.resize(open_handles.size());
  if (absl::Status status = internal::ParallelDo(
          open_handles.size(),
          [&](size_t i) -> absl::Status {
            ReaderOpenHandles& handles = open_handles[i];
            if (handles.records == nullptr) {
              return absl::InternalError(
                  absl::StrCat("Record handle must be non-null at file ", i,
                               "."));
            }
            absl::StatusOr<internal::RecordsLimits> split =
                handles.limits_source != nullptr
                    ? internal::SplitRecordsAndLimits(std::move(handles.records),
                                                      std::move(handles.limits_source))
                    : internal::SplitRecordsAndLimits(std::move(handles.records));
            if (!split.ok()) {
              return absl::Status(
                  split.status().code(),
                  absl::StrCat(split.status().message(), "; at file ", i));
            }
            file_pairs[i] = {std::move(split->records),
                             std::move(split->limits)};
            return absl::OkStatus();
          },
          options.max_parallelism, /*cpu_bound=*/false);
      !status.ok()) {
    return status;
  }
  return file_pairs;
}

// Reads the entire file into memory on first use the provided PReadFile
// interface.
class InMemoryPReadFile : public PReadFile {
 public:
  explicit InMemoryPReadFile(std::unique_ptr<PReadFile> pread_file)
      : pread_file_(std::move(pread_file)), data_size_(pread_file_->size()) {}

  void InitOnce() {
    data_ = std::make_unique_for_overwrite<char[]>(data_size_);
    char* data = data_.get();
    status_ =
        pread_file_->PRead(0, data_size_, [&data](absl::string_view piece) {
          std::memcpy(data, piece.data(), piece.size());
          data += piece.size();
          return true;
        });
    pread_file_.reset();
    if (!status_.ok()) {
      data_ = nullptr;
    }
  }

  size_t size() const override { return data_size_; }

  absl::Status PRead(size_t offset, size_t num_bytes,
                     absl::FunctionRef<bool(absl::string_view piece)> callback)
      const override {
    absl::call_once(once_, &InMemoryPReadFile::InitOnce,
                    const_cast<InMemoryPReadFile*>(this));
    if (!status_.ok()) {
      return status_;
    }
    callback(
        absl::string_view(data_.get(), data_size_).substr(offset, num_bytes));
    return absl::OkStatus();
  }

 private:
  std::unique_ptr<PReadFile> pread_file_;
  std::unique_ptr<char[]> data_;
  size_t data_size_;
  absl::Status status_;
  mutable absl::once_flag once_;
};

}  // namespace

struct SackliReader::State {
  State(SackliReader::Options options,
        std::vector<internal::SackliShardReader> shards,
        std::vector<size_t> accumulated_count)
      : options_(std::move(options)),
        shards_(std::move(shards)),
        shard_indexing_(
            std::move(accumulated_count),
            options_.sharding_layout == ShardingLayout::kInterleaved) {
    if (std::holds_alternative<CompressionZstd>(options_.compression)) {
      absl::string_view dictionary =
          std::get<CompressionZstd>(options_.compression).dictionary;
      decompressor_factory_ = [dictionary] {
        return internal::ZstdDecompressor(dictionary);
      };
    }
  }

  [[nodiscard]] size_t size() const { return shard_indexing_.size(); }

  absl::Status ReadWithAllocator(
      size_t index,
      absl::FunctionRef<absl::Span<char>(size_t record_size)> allocate) const {
    if (index >= size()) {
      return absl::OutOfRangeError(
          absl::StrCat("Index ", index, " out of range [0, ", size(), ")"));
    }
    auto decompressor = decompressor_factory_ != nullptr
                            ? decompressor_pool_.Get(decompressor_factory_)
                            : nullptr;
    internal::ShardIndex shard_index = shard_indexing_.index(index);
    absl::Status compress_status = absl::OkStatus();

    absl::Status status = shards_[shard_index.shard].Read(
        shard_index.shard_index, [&](absl::string_view compressed) {
          compress_status =
              DecompressInto(decompressor.get(), compressed, allocate);
        });
    status.Update(compress_status);
    return status;
  }

  absl::StatusOr<SackliReader::Handle> ReadHandle(size_t index) const {
    internal::ShardIndex shard_index = shard_indexing_.index(index);
    absl::StatusOr<internal::SackliShardReader::ByteRange> byte_range =
        shards_[shard_index.shard].ReadByteRange(shard_index.shard_index);
    if (!byte_range.ok()) {
      return byte_range.status();
    }
    return SackliReader::Handle{.shard = shard_index.shard,
                              .offset = byte_range->offset,
                              .num_bytes = byte_range->length};
  }

  absl::Status ReadFromHandleWithAllocator(
      SackliReader::Handle handle,
      absl::FunctionRef<absl::Span<char>(size_t record_size)> allocate) const {
    auto decompressor = decompressor_factory_ != nullptr
                            ? decompressor_pool_.Get(decompressor_factory_)
                            : nullptr;
    absl::Status compress_status = absl::OkStatus();

    absl::Status status = shards_[handle.shard].ReadFromByteRange(
        internal::SackliShardReader::ByteRange{.offset = handle.offset,
                                             .length = handle.num_bytes},
        [&](absl::string_view compressed) {
          compress_status =
              DecompressInto(decompressor.get(), compressed, allocate);
        });
    status.Update(compress_status);
    return status;
  }

  absl::Status ReadRangeWithAllocator(
      size_t start, size_t num_records,
      absl::FunctionRef<absl::Span<char>(size_t result_index,
                                         size_t record_size)>
          allocate_for_index) const {
    if (!shard_indexing_.valid_range(start, num_records)) {
      return absl::OutOfRangeError(
          absl::StrCat("Range [", start, ", ", start + num_records,
                       ") out of range [0, ", size(), "]"));
    }

    return ReadShardRanges(shard_indexing_.range(start, num_records),
                           allocate_for_index);
  }

  absl::Status ReadIndicesWithAllocator(
      absl::Span<const size_t> indices,
      absl::FunctionRef<absl::Span<char>(size_t result_index,
                                         size_t record_size)>
          allocate_for_index,
      absl::FunctionRef<void(size_t from_index, size_t to_index)> copy_result)
      const {
    struct Result {
      internal::ShardIndex record;
      size_t result_index;
    };
    std::vector<Result> result_map;
    for (size_t i = 0; i < indices.size(); ++i) {
      size_t index = indices[i];
      if (index >= size()) {
        return absl::OutOfRangeError(absl::StrCat(
            "indices[", i, "] = ", index, " out of range [0, ", size(), ")"));
      }

      result_map.push_back(
          Result{.record = shard_indexing_.index(index), .result_index = i});
    }

    // Sort by record, result_index.
    absl::c_sort(result_map, [](const Result& a, const Result& b) {
      return std::tie(a.record.shard, a.record.shard_index, a.result_index) <
             std::tie(b.record.shard, b.record.shard_index, b.result_index);
    });

    std::vector<absl::Span<const Result>> grouped_by_record_index;
    grouped_by_record_index.reserve(result_map.size());
    // Remove duplicates.
    absl::Span<const Result> result_map_span = absl::MakeConstSpan(result_map);
    for (size_t i = 0; i < result_map.size();) {
      size_t record_shard = result_map[i].record.shard;
      size_t record_index = result_map[i].record.shard_index;
      size_t start_index = i++;
      while (i < result_map.size() &&
             result_map[i].record.shard_index == record_index &&
             result_map[i].record.shard == record_shard) {
        ++i;
      }
      grouped_by_record_index.push_back(
          result_map_span.subspan(start_index, i - start_index));
    }

    std::vector<internal::ShardRange> shard_ranges;

    // Upper bound on the number of shard ranges.

    // Group by contiguous ranges.
    shard_ranges.reserve(grouped_by_record_index.size());
    for (size_t i = 0; i < grouped_by_record_index.size();) {
      const Result& first = grouped_by_record_index[i].front();
      size_t record_shard = first.record.shard;
      size_t record_index = first.record.shard_index;
      size_t start_index = i++;
      for (; i < grouped_by_record_index.size(); ++i) {
        const Result& next = grouped_by_record_index[i].front();
        if (next.record.shard_index != record_index + i - start_index ||
            next.record.shard != record_shard) {
          break;
        }
      }
      shard_ranges.push_back(internal::ShardRange{.shard = record_shard,
                                                  .shard_start = record_index,
                                                  .count = i - start_index,
                                                  .result_offset = start_index,
                                                  .result_stride = 1});
    }
    if (absl::Status status = ReadShardRanges(
            shard_ranges,
            [&](size_t result_index, size_t record_size) {
              return allocate_for_index(
                  grouped_by_record_index[result_index].front().result_index,
                  record_size);
            });
        !status.ok()) {
      return status;
    }
    // Copy duplicates.
    if (grouped_by_record_index.size() < indices.size()) {
      for (absl::Span<const Result> group : grouped_by_record_index) {
        if (group.size() > 1) {
          size_t from_index = group.front().result_index;
          for (size_t i = 1; i < group.size(); ++i) {
            copy_result(from_index, group[i].result_index);
          }
        }
      }
    }
    return absl::OkStatus();
  }

  double ApproximateNumBytesPerRecord() const {
    size_t num_records = size();
    if (num_records == 0) {
      return 0.0;
    }
    size_t num_bytes = 0;
    for (const internal::SackliShardReader& shard : shards_) {
      num_bytes += shard.num_bytes();
    }
    return static_cast<double>(num_bytes) / num_records;
  }

  const SackliReader::Options& options() const { return options_; }

 private:
  static absl::Status DecompressInto(
      internal::ZstdDecompressor* decompressor, absl::string_view compressed,
      absl::FunctionRef<absl::Span<char>(size_t num_bytes)> allocate) {
    if (decompressor == nullptr || compressed.empty()) {
      auto span = allocate(compressed.size());
      if (span.size() != compressed.size()) {
        return absl::InternalError(
            absl::StrCat("Failed to allocate ", compressed.size(), " bytes"));
      }
      std::memcpy(span.data(), compressed.data(), compressed.size());
      return absl::OkStatus();
    } else {
      return decompressor->Decompress(compressed, allocate);
    }
  }

  absl::Status ReadShardRanges(
      absl::Span<const internal::ShardRange> shard_ranges,
      absl::FunctionRef<absl::Span<char>(size_t result_index,
                                         size_t record_size)>
          allocate_for_index) const {
    return internal::ParallelDo(
        shard_ranges.size(),
        [&](size_t shard_index) -> absl::Status {
          const internal::ShardRange& shard_range = shard_ranges[shard_index];
          absl::Status compress_status = absl::OkStatus();
          auto status = shards_[shard_range.shard].ReadRange(
              shard_range.shard_start, shard_range.count,
              [&](size_t index, absl::string_view compressed) {
                size_t result_index = shard_range.result_stride * index +
                                      shard_range.result_offset;
                auto decompressor =
                    decompressor_factory_ != nullptr
                        ? decompressor_pool_.Get(decompressor_factory_)
                        : nullptr;
                compress_status = DecompressInto(
                    decompressor.get(), compressed,
                    [result_index, &allocate_for_index](size_t num_bytes) {
                      return allocate_for_index(result_index, num_bytes);
                    });
                return compress_status.ok();
              });
          status.Update(compress_status);
          return status;
        },
        options_.max_parallelism,
        /*cpu_bound=*/decompressor_factory_ != nullptr);
  }

  Options options_;
  std::vector<internal::SackliShardReader> shards_;
  internal::ShardIndexing shard_indexing_;
  mutable internal::RecyclingPool<internal::ZstdDecompressor>
      decompressor_pool_;
  absl::AnyInvocable<internal::ZstdDecompressor() const> decompressor_factory_;
};

absl::StatusOr<SackliReader> SackliReader::BuildFromFilePairs(
    std::vector<FilePair> file_pairs, SackliReader::Options options) {
  std::vector<internal::SackliShardReader> shards;
  shards.reserve(file_pairs.size());
  size_t total_count = 0;
  std::vector<size_t> accumulated_count;
  accumulated_count.reserve(file_pairs.size());
  for (auto& [record_file, limits_file] : file_pairs) {
    if (options.limits_storage == LimitsStorage::kInMemory) {
      limits_file = std::make_unique<InMemoryPReadFile>(std::move(limits_file));
    }

    internal::SackliShardReader& shard = shards.emplace_back(
        std::move(record_file), std::move(limits_file));
    total_count += shard.size();
    accumulated_count.push_back(total_count);
  }
  if (options.sharding_layout == ShardingLayout::kInterleaved) {
    for (size_t i = 1; i < shards.size(); ++i) {
      if (shards[i].size() > shards[i - 1].size()) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Interleaved error - shard sizes must be non-increasing. Size at ",
            i, " : ", shards[i].size(), " size at", i - 1, " : ",
            shards[i - 1].size()));
      }
      if (shards.front().size() - shards[i].size() > 1) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Interleaved error - shard sizes must be within 1 of each other. "
            "First size: ",
            shards.front().size(), " size at ", i, " : ", shards[i].size()));
      }
    }
  }
  return SackliReader(
      std::make_shared<SackliReader::State>(
          std::move(options), std::move(shards), std::move(accumulated_count)),
      /*slice_start=*/0, /*slice_step=*/1, /*slice_length=*/total_count);
}

size_t SackliReader::size() const { return slice_length_; }

double SackliReader::ApproximateNumBytesPerRecord() const {
  return state_->ApproximateNumBytesPerRecord();
}

absl::StatusOr<std::string> SackliReader::operator[](size_t index) const {
  if (index >= slice_length_) {
    return absl::OutOfRangeError(absl::StrCat(
        "index ", index, " out of range [0, ", slice_length_, ")"));
  }
  absl::StatusOr<SackliReader::Handle> handle =
      state_->ReadHandle(index * slice_step_ + slice_start_);
  if (!handle.ok()) {
    return handle.status();
  }
  return ReadFromHandle(*handle);
}

absl::StatusOr<std::vector<std::string>> SackliReader::ReadRange(
    size_t start, size_t num_records) const {
  if (start + num_records > slice_length_) {
    return absl::OutOfRangeError(
        absl::StrCat("Range [", start, ", ", start + num_records,
                     ") out of range [0, ", slice_length_, "]"));
  }
  std::vector<std::string> result_vector(num_records);
  if (slice_step_ != 1) {
    std::vector<size_t> indices;
    indices.reserve(num_records);
    for (size_t i = 0; i < num_records; ++i) {
      indices.push_back(slice_start_ + (start + i) * slice_step_);
    }
    if (absl::Status status = state_->ReadIndicesWithAllocator(
            indices,
            [&result_vector](size_t result_index, size_t record_size) {
              result_vector[result_index].resize(record_size);
              return absl::MakeSpan(result_vector[result_index]);
            },
            [&result_vector](size_t from_index, size_t to_index) {
              result_vector[to_index] = result_vector[from_index];
            });
        !status.ok()) {
      return status;
    } else {
      return result_vector;
    }
  }
  if (absl::Status status = state_->ReadRangeWithAllocator(
          start + slice_start_, num_records,
          [&result_vector](size_t result_index, size_t record_size) {
            result_vector[result_index].resize(record_size);
            return absl::MakeSpan(result_vector[result_index]);
          });
      !status.ok()) {
    return status;
  } else {
    return result_vector;
  }
}

absl::StatusOr<std::vector<std::string>> SackliReader::ReadIndices(
    absl::Span<const size_t> indices) const {
  std::vector<std::size_t> indices_vector;
  if (slice_step_ != 1 || slice_start_ != 0) {
    indices_vector.reserve(indices.size());
    size_t position = 0;
    for (size_t index : indices) {
      if (index >= slice_length_) {
        return absl::OutOfRangeError(
            absl::StrCat("indices[", position, "] = ", index,
                         " out of range [0, ", slice_length_, ")"));
      }
      ++position;
      indices_vector.push_back(index * slice_step_ + slice_start_);
    }
    indices = absl::MakeSpan(indices_vector);
  }

  std::vector<std::string> result_vector(indices.size());
  if (absl::Status status = state_->ReadIndicesWithAllocator(
          indices,
          [&result_vector](size_t result_index, size_t record_size) {
            result_vector[result_index].resize(record_size);
            return absl::MakeSpan(result_vector[result_index]);
          },
          [&result_vector](size_t from_index, size_t to_index) {
            result_vector[to_index] = result_vector[from_index];
          });
      !status.ok()) {
    return status;
  } else {
    return result_vector;
  }
}

absl::Status SackliReader::ReadWithAllocator(
    size_t index,
    absl::FunctionRef<absl::Span<char>(size_t record_size)> allocate) const {
  if (index >= slice_length_) {
    return absl::OutOfRangeError(absl::StrCat(
        "index ", index, " out of range [0, ", slice_length_, ")"));
  }
  return state_->ReadWithAllocator(index * slice_step_ + slice_start_,
                                   allocate);
}

absl::Status SackliReader::ReadRangeWithAllocator(
    size_t start, size_t num_records,
    absl::FunctionRef<absl::Span<char>(size_t result_index, size_t record_size)>
        allocate_for_index) const {
  if (start + num_records > slice_length_) {
    return absl::OutOfRangeError(
        absl::StrCat("Range ", start, ", ", start + num_records,
                     " out of range [0, ", slice_length_, "]"));
  }
  if (slice_step_ != 1) {
    if (slice_step_ == 0) {
      return absl::InvalidArgumentError(
          "slice_step_ must not be 0 when calling ReadRangeWithAllocator().");
    }
    std::vector<size_t> indices;
    indices.reserve(num_records);
    for (size_t i = 0; i < num_records; ++i) {
      indices.push_back(slice_start_ + (start + i) * slice_step_);
    }
    return state_->ReadIndicesWithAllocator(indices, allocate_for_index,
                                            [](size_t, size_t) {});
  }

  return state_->ReadRangeWithAllocator(start + slice_start_, num_records,
                                        allocate_for_index);
}

absl::Status SackliReader::ReadIndicesWithAllocator(
    absl::Span<const size_t> indices,
    absl::FunctionRef<absl::Span<char>(size_t result_index, size_t record_size)>
        allocate_for_index,
    absl::FunctionRef<void(size_t from_index, size_t to_index)> copy_result)
    const {
  std::vector<size_t> indices_vector;
  if (slice_step_ != 1 || slice_start_ != 0) {
    indices_vector.reserve(indices.size());
    size_t position = 0;
    for (size_t index : indices) {
      if (index >= slice_length_) {
        return absl::OutOfRangeError(
            absl::StrCat("indices[", position, "] = ", index,
                         " out of range [0, ", slice_length_, ")"));
      }
      ++position;
      indices_vector.push_back(index * slice_step_ + slice_start_);
    }
    indices = absl::MakeSpan(indices_vector);
  }
  return state_->ReadIndicesWithAllocator(indices, allocate_for_index,
                                          copy_result);
}

absl::StatusOr<SackliReader> SackliReader::Open(absl::string_view filespec,
                                            SackliReader::Options options) {
  if (std::holds_alternative<CompressionAutoDetect>(options.compression)) {
    if (filespec.ends_with(".bagz")) {
      options.compression.emplace<CompressionZstd>();
    } else {
      options.compression.emplace<CompressionNone>();
    }
  }

  absl::StatusOr<ReaderHandleBatch> open_handles =
      OpenFilesForFileSpecList(filespec, options);
  if (!open_handles.ok()) {
    return open_handles.status();
  }
  absl::StatusOr<std::vector<ReaderFilePair>> file_pairs =
      ResolveFilePairs(std::move(*open_handles), options);
  if (!file_pairs.ok()) {
    return file_pairs.status();
  }
  return BuildFromFilePairs(std::move(*file_pairs), std::move(options));
}

absl::StatusOr<SackliReader::Handle> SackliReader::ReadHandle(size_t index) const {
  if (index >= slice_length_) {
    return absl::OutOfRangeError(absl::StrCat(
        "index ", index, " out of range [0, ", slice_length_, ")"));
  }
  return state_->ReadHandle(index * slice_step_ + slice_start_);
}

absl::StatusOr<std::string> SackliReader::ReadFromHandle(Handle handle) const {
  std::string result;
  if (absl::Status status =
          state_->ReadFromHandleWithAllocator(handle,
                                              [&result](size_t record_size) {
                                                result.resize(record_size);
                                                return absl::MakeSpan(result);
                                              });
      !status.ok()) {
    return status;
  } else {
    return result;
  }
}

absl::Status SackliReader::ReadFromHandleWithAllocator(
    Handle handle,
    absl::FunctionRef<absl::Span<char>(size_t record_size)> allocate) const {
  return state_->ReadFromHandleWithAllocator(handle, allocate);
}

absl::StatusOr<SackliReader> SackliReader::Slice(size_t start, int64_t step,
                                             size_t length) const {
  size_t num_records = size();
  if (step == 0) {
    return absl::OutOfRangeError("step must be non-zero");
  }
  if (length > 0) {
    if (start >= num_records) {
      return absl::OutOfRangeError(absl::StrCat(
          "start (", start, ") must be in range [0, ", num_records, ")"));
    }
    if (step > 0) {
      if (start + step * (length - 1) + 1 > num_records) {
        return absl::OutOfRangeError(absl::StrCat(
            "(start: ", start, " + step: ", step, " * length: ", length,
            ") must be less than size: ", num_records));
      }
    } else {
      // step < 0.
      if (start < -step * (length - 1)) {
        return absl::OutOfRangeError(absl::StrCat(
            "(start: ", start, ", step: ", step, ", length: ", length,
            ") doesn't fit in [0, ", num_records, ")"));
      }
    }
  }
  start = slice_start_ + start * slice_step_;
  step *= slice_step_;
  return SackliReader(state_, length != 0 ? start : 0, step, length);
}

const SackliReader::Options& SackliReader::options() const {
  return state_->options();
}

}  // namespace sackli
