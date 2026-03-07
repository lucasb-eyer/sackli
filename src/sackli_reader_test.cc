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

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "src/file/file.h"
#include "src/file/file_system/file_system.h"
#include "src/file/file_system/pread_file.h"
#include "src/file/file_system/string_pread_file.h"
#include "src/file/file_system/write_file.h"
#include "src/file/file_systems/posix/posix_file_system.h"
#include "src/file/registry/file_system_registry.h"

namespace sackli {
namespace {

struct Call {
  std::string filespec;
  PReadOpenOptions options;
};

struct TempFile {
  ~TempFile() { file::Delete(path).IgnoreError(); }

  std::string path;
};

TempFile MakeTempFile(absl::string_view stem) {
  static int next_id = 0;
  return TempFile{
      .path = absl::StrCat("/tmp/", stem, "-", getpid(), "-", next_id++)};
}

absl::StatusOr<std::vector<absl_nonnull std::unique_ptr<PReadFile>>>
MakeEmptyFiles(size_t count) {
  std::vector<absl_nonnull std::unique_ptr<PReadFile>> files;
  files.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    files.push_back(std::make_unique<StringPReadFile>(""));
  }
  return files;
}

class RecordingFileSystem : public FileSystem {
 public:
  RecordingFileSystem() {
    FileSystemRegistry::Instance().Register("record_fs:", *this).IgnoreError();
  }

  ~RecordingFileSystem() override {
    FileSystemRegistry::Instance().Unregister("record_fs:").IgnoreError();
  }

  absl::StatusOr<absl_nonnull std::unique_ptr<PReadFile>> OpenPRead(
      absl::string_view filename_without_prefix,
      const PReadOpenOptions& options) const override {
    (void)options;
    return absl::UnimplementedError("OpenPRead should not be called");
  }

  absl::StatusOr<absl_nonnull std::unique_ptr<WriteFile>> OpenWrite(
      absl::string_view filename_without_prefix, uint64_t offset,
      absl::string_view options) const override {
    return absl::UnimplementedError("OpenWrite should not be called");
  }

  absl::Status Delete(absl::string_view filename_without_prefix,
                      absl::string_view options) const override {
    return absl::UnimplementedError("Delete should not be called");
  }

  absl::StatusOr<std::vector<absl_nonnull std::unique_ptr<PReadFile>>>
  BulkOpenPRead(absl::string_view filespec_without_prefix,
                const PReadOpenOptions& options) const override {
    calls.push_back(
        Call{std::string(filespec_without_prefix), options});
    const size_t count =
        filespec_without_prefix.empty()
            ? 0
            : 1 + static_cast<size_t>(std::count(filespec_without_prefix.begin(),
                                                 filespec_without_prefix.end(),
                                                 ','));
    return MakeEmptyFiles(count);
  }

  mutable std::vector<Call> calls;
};

class RecordingPosixFileSystem : public PosixFileSystem {
 public:
  RecordingPosixFileSystem() {
    FileSystemRegistry::Instance()
        .Register("record_posix:", *this)
        .IgnoreError();
  }

  ~RecordingPosixFileSystem() override {
    FileSystemRegistry::Instance().Unregister("record_posix:").IgnoreError();
  }

  absl::StatusOr<std::vector<absl_nonnull std::unique_ptr<PReadFile>>>
  BulkOpenPRead(absl::string_view filespec_without_prefix,
                const PReadOpenOptions& options) const override {
    calls.push_back(
        Call{std::string(filespec_without_prefix), options});
    const size_t count =
        filespec_without_prefix.empty()
            ? 0
            : 1 + static_cast<size_t>(std::count(filespec_without_prefix.begin(),
                                                 filespec_without_prefix.end(),
                                                 ','));
    return MakeEmptyFiles(count);
  }

  mutable std::vector<Call> calls;
};

bool Expect(bool condition, absl::string_view message) {
  if (!condition) {
    std::cerr << message << '\n';
    return false;
  }
  return true;
}

bool ExpectCalls(absl::Span<const Call> actual, absl::Span<const Call> expected,
                 absl::string_view test_name) {
  if (!Expect(actual.size() == expected.size(),
              absl::StrCat(test_name,
                           ": wrong number of BulkOpenPRead calls"))) {
    return false;
  }
  for (size_t i = 0; i < actual.size(); ++i) {
    if (!Expect(actual[i].filespec == expected[i].filespec,
                absl::StrCat(test_name, ": wrong filespec in call ", i))) {
      return false;
    }
    if (!Expect(actual[i].options == expected[i].options,
                absl::StrCat(test_name, ": wrong options in call ", i))) {
      return false;
    }
  }
  return true;
}

bool TestTailUsesSeparateLimitHandlesForPosixFiles() {
  RecordingPosixFileSystem file_system;
  SackliReader::Options options{
      .limits_placement = LimitsPlacement::kTail,
      .compression = CompressionNone{},
      .access_pattern = AccessPattern::kRandom,
      .cache_policy = CachePolicy::kDropAfterRead,
  };
  absl::StatusOr<SackliReader> reader =
      SackliReader::Open("record_posix:data.bagz", std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("tail-posix: ", reader.status().message()))) {
    return false;
  }
  const Call expected[] = {
      {.filespec = "data.bagz",
       .options =
           {.access_pattern = AccessPattern::kRandom,
            .cache_policy = CachePolicy::kDropAfterRead,
            .prefer_streaming = true}},
      {.filespec = "data.bagz",
       .options = {.access_pattern = AccessPattern::kRandom}},
  };
  return ExpectCalls(file_system.calls, expected, "tail-posix");
}

bool TestTailReusesRecordHandleForAccessPatternOnly() {
  RecordingPosixFileSystem file_system;
  SackliReader::Options options{
      .limits_placement = LimitsPlacement::kTail,
      .compression = CompressionNone{},
      .access_pattern = AccessPattern::kSequential,
  };
  absl::StatusOr<SackliReader> reader =
      SackliReader::Open("record_posix:data.bagz", std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("tail-posix-access-pattern: ",
                           reader.status().message()))) {
    return false;
  }
  const Call expected[] = {
      {.filespec = "data.bagz",
       .options = {.access_pattern = AccessPattern::kSequential}},
  };
  return ExpectCalls(file_system.calls, expected,
                     "tail-posix-access-pattern");
}

bool TestSeparateLimitsKeepDefaultReadOptions() {
  RecordingPosixFileSystem file_system;
  SackliReader::Options options{
      .limits_placement = LimitsPlacement::kSeparate,
      .compression = CompressionNone{},
      .access_pattern = AccessPattern::kSequential,
  };
  absl::StatusOr<SackliReader> reader =
      SackliReader::Open("record_posix:a.bag,record_posix:b.bag",
                         std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("separate-limits: ", reader.status().message()))) {
    return false;
  }
  const Call expected[] = {
      {.filespec = "a.bag,b.bag",
       .options = {.access_pattern = AccessPattern::kSequential}},
      {.filespec = "limits.a.bag,limits.b.bag", .options = {}},
  };
  return ExpectCalls(file_system.calls, expected, "separate-limits");
}

bool TestTailBatchesAdjacentPosixFilesWithSameReadOptions() {
  RecordingPosixFileSystem file_system;
  SackliReader::Options options{
      .limits_placement = LimitsPlacement::kTail,
      .compression = CompressionNone{},
      .access_pattern = AccessPattern::kRandom,
      .cache_policy = CachePolicy::kDropAfterRead,
  };
  absl::StatusOr<SackliReader> reader =
      SackliReader::Open("record_posix:a.bag,record_posix:b.bag",
                         std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("tail-batched-posix: ",
                           reader.status().message()))) {
    return false;
  }
  const Call expected[] = {
      {.filespec = "a.bag,b.bag",
       .options =
           {.access_pattern = AccessPattern::kRandom,
            .cache_policy = CachePolicy::kDropAfterRead,
            .prefer_streaming = true}},
      {.filespec = "a.bag,b.bag",
       .options = {.access_pattern = AccessPattern::kRandom}},
  };
  return ExpectCalls(file_system.calls, expected, "tail-batched-posix");
}

bool TestTailIgnoresPosixReadOptionsForNonPosixFiles() {
  RecordingFileSystem file_system;
  SackliReader::Options options{
      .limits_placement = LimitsPlacement::kTail,
      .compression = CompressionNone{},
      .access_pattern = AccessPattern::kRandom,
      .cache_policy = CachePolicy::kDropAfterRead,
  };
  absl::StatusOr<SackliReader> reader =
      SackliReader::Open("record_fs:data.bagz", std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("tail-non-posix: ", reader.status().message()))) {
    return false;
  }
  const Call expected[] = {
      {.filespec = "data.bagz",
       .options =
           {.access_pattern = AccessPattern::kRandom,
            .cache_policy = CachePolicy::kDropAfterRead,
            .prefer_streaming = true}},
  };
  return ExpectCalls(file_system.calls, expected, "tail-non-posix");
}

bool TestTailPosixUsesSeparateLimitHandlesAtDefaultOptions() {
  RecordingPosixFileSystem file_system;
  SackliReader::Options options{
      .limits_placement = LimitsPlacement::kTail,
      .compression = CompressionNone{},
  };
  absl::StatusOr<SackliReader> reader =
      SackliReader::Open("record_posix:data.bagz", std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("tail-posix-default: ",
                           reader.status().message()))) {
    return false;
  }
  const Call expected[] = {
      {.filespec = "data.bagz", .options = {}},
  };
  return ExpectCalls(file_system.calls, expected, "tail-posix-default");
}

bool TestSeparateLimitsHandleLeadingSlashPrefixes() {
  RecordingPosixFileSystem file_system;
  SackliReader::Options options{
    .limits_placement = LimitsPlacement::kSeparate,
    .compression = CompressionNone{},
  };
  absl::StatusOr<SackliReader> reader =
      SackliReader::Open("/record_posix:data.bag", std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("leading-slash-prefix: ",
                           reader.status().message()))) {
    return false;
  }
  const Call expected[] = {
      {.filespec = "data.bag", .options = {}},
      {.filespec = "limits.data.bag", .options = {}},
  };
  return ExpectCalls(file_system.calls, expected, "leading-slash-prefix");
}

bool TestMixedFilespecsApplyPosixOptionsOnlyToPosixFiles() {
  RecordingPosixFileSystem posix_file_system;
  RecordingFileSystem non_posix_file_system;
  SackliReader::Options options{
      .limits_placement = LimitsPlacement::kTail,
      .compression = CompressionNone{},
      .access_pattern = AccessPattern::kRandom,
      .cache_policy = CachePolicy::kDropAfterRead,
  };
  absl::StatusOr<SackliReader> reader = SackliReader::Open(
      "record_posix:a.bag,record_fs:b.bag,record_posix:c.bag",
      std::move(options));
  if (!Expect(reader.ok(),
              absl::StrCat("mixed-filespecs: ", reader.status().message()))) {
    return false;
  }
  const Call expected_posix[] = {
      {.filespec = "a.bag",
       .options =
           {.access_pattern = AccessPattern::kRandom,
            .cache_policy = CachePolicy::kDropAfterRead,
            .prefer_streaming = true}},
      {.filespec = "a.bag",
       .options = {.access_pattern = AccessPattern::kRandom}},
      {.filespec = "c.bag",
       .options =
           {.access_pattern = AccessPattern::kRandom,
            .cache_policy = CachePolicy::kDropAfterRead,
            .prefer_streaming = true}},
      {.filespec = "c.bag",
       .options = {.access_pattern = AccessPattern::kRandom}},
  };
  if (!ExpectCalls(posix_file_system.calls, expected_posix, "mixed-posix")) {
    return false;
  }
  const Call expected_non_posix[] = {
      {.filespec = "b.bag",
       .options =
           {.access_pattern = AccessPattern::kRandom,
            .cache_policy = CachePolicy::kDropAfterRead,
            .prefer_streaming = true}},
  };
  return ExpectCalls(non_posix_file_system.calls, expected_non_posix,
                     "mixed-non-posix");
}

bool TestPosixPReadBackendStreamsLargeReads() {
  constexpr size_t kPayloadSize = (2 << 20) + 123;
  TempFile temp_file = MakeTempFile("sackli-posix-pread-stream");

  std::string payload;
  payload.reserve(kPayloadSize);
  for (size_t i = 0; i < kPayloadSize; ++i) {
    payload.push_back(static_cast<char>('a' + (i % 26)));
  }

  absl::StatusOr<absl_nonnull std::unique_ptr<WriteFile>> writer =
      file::OpenWrite(temp_file.path);
  if (!Expect(writer.ok(),
              absl::StrCat("pread-stream: failed to open writer: ",
                           writer.status().message()))) {
    return false;
  }
  if (!Expect((*writer)->Write(payload).ok(),
              "pread-stream: failed to write payload")) {
    return false;
  }
  if (!Expect((*writer)->Close().ok(),
              "pread-stream: failed to close writer")) {
    return false;
  }

  absl::StatusOr<absl_nonnull std::unique_ptr<PReadFile>> file =
      file::OpenPRead(temp_file.path, {.prefer_streaming = true});
  if (!Expect(file.ok(),
              absl::StrCat("pread-stream: failed to open reader: ",
                           file.status().message()))) {
    return false;
  }

  std::string reconstructed;
  reconstructed.reserve(payload.size());
  size_t callback_count = 0;
  if (!Expect(
          (*file)
              ->PRead(0, payload.size(), [&](absl::string_view piece) {
                ++callback_count;
                reconstructed.append(piece);
                return true;
              })
              .ok(),
          "pread-stream: PRead failed")) {
    return false;
  }
  if (!Expect(reconstructed == payload,
              "pread-stream: reconstructed payload mismatch")) {
    return false;
  }
  if (!Expect(callback_count > 1,
              "pread-stream: expected split callbacks for large read")) {
    return false;
  }

  size_t first_piece_size = 0;
  if (!Expect(
          (*file)
              ->PRead(0, payload.size(), [&](absl::string_view piece) {
                first_piece_size = piece.size();
                return false;
              })
              .ok(),
          "pread-stream: early-stop PRead failed")) {
    return false;
  }
  return Expect(first_piece_size > 0 && first_piece_size < payload.size(),
                "pread-stream: expected early stop after first chunk");
}

}  // namespace

int RunTests() {
  const bool ok = TestTailUsesSeparateLimitHandlesForPosixFiles() &&
                  TestTailReusesRecordHandleForAccessPatternOnly() &&
                  TestSeparateLimitsKeepDefaultReadOptions() &&
                  TestTailBatchesAdjacentPosixFilesWithSameReadOptions() &&
                  TestTailIgnoresPosixReadOptionsForNonPosixFiles() &&
                  TestTailPosixUsesSeparateLimitHandlesAtDefaultOptions() &&
                  TestSeparateLimitsHandleLeadingSlashPrefixes() &&
                  TestMixedFilespecsApplyPosixOptionsOnlyToPosixFiles() &&
                  TestPosixPReadBackendStreamsLargeReads();
  return ok ? 0 : 1;
}

}  // namespace sackli

int main() { return sackli::RunTests(); }
