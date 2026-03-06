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

#include "absl/base/no_destructor.h"
#include "absl/log/absl_check.h"
#include "src/file/file_systems/posix/posix_file_system.h"
#include "src/file/registry/file_system_registry.h"

#if BAGZ_ENABLE_GCS
#include "src/file/file_systems/gcs/gcs_file_system.h"
#endif

#if BAGZ_ENABLE_S3
#include "src/file/file_systems/s3/s3_file_system.h"
#endif

namespace bagz {

void RegisterFileSystems(FileSystemRegistry& register_fs) {
  static absl::NoDestructor<PosixFileSystem> posix_fs;

#if BAGZ_ENABLE_GCS
  static absl::NoDestructor<bagz::GcsFileSystem> gcs_fs;
#endif

#if BAGZ_ENABLE_S3
  static absl::NoDestructor<bagz::S3FileSystem> s3_fs;
#endif

#if BAGZ_ENABLE_GCS
  ABSL_CHECK_OK(register_fs.Register("gs:", *gcs_fs));
#endif

  ABSL_CHECK_OK(register_fs.Register("posix:", *posix_fs));

#if BAGZ_ENABLE_S3
  ABSL_CHECK_OK(register_fs.Register("s3:", *s3_fs));
#endif

  // Set the default file system.
  ABSL_CHECK_OK(register_fs.Register("", *posix_fs));
}

}  // namespace bagz
