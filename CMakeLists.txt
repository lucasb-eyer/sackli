# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.28)
project(bagz LANGUAGES CXX)

include(FetchContent)

option(BAGZ_ENABLE_GCS "Build Google Cloud Storage filesystem support" ON)
option(BAGZ_ENABLE_S3 "Build Amazon S3 filesystem support" ON)
option(BUILD_SHARED_LIBS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS TRUE)
set(CMAKE_SKIP_INSTALL_RULES ON)

# Temporarily disable warnings for third-party libraries
set(SAVED_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w -Wno-dev")

include(cmake/third_party_absl.cmake)
include(cmake/third_party_pybind11.cmake)
include(cmake/third_party_zstd.cmake)
if(BAGZ_ENABLE_GCS)
  include(cmake/third_party_gcs.cmake)
endif()
if(BAGZ_ENABLE_S3)
  include(cmake/third_party_aws.cmake)
endif()

# Restore original CXX_FLAGS
set(CMAKE_CXX_FLAGS "${SAVED_CMAKE_CXX_FLAGS}")

set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS FALSE)

set(CMAKE_SKIP_INSTALL_RULES OFF)

include(cmake/bagz_cc.cmake)

file(GLOB bagz_internal_sources "src/internal/*.cc")
file(GLOB bagz_internal_headers "src/internal/*.h")

bagz_cc_library(
  bagz_internal
  SOURCES ${bagz_internal_sources}
  HEADERS ${bagz_internal_headers}
  DEPS
    absl::strings
    absl::status
    absl::statusor
    absl::flat_hash_map
    absl::log
    libzstd_static
)

file(GLOB bagz_file_system_sources "src/file/file_system/*.cc")
file(GLOB bagz_file_system_headers "src/file/file_system/*.h")

bagz_cc_library(
  bagz_file_system
  SOURCES ${bagz_file_system_sources}
  HEADERS ${bagz_file_system_headers}
  DEPS
    absl::flat_hash_map
    absl::log
    absl::status
    absl::statusor
    absl::strings
    bagz_internal
)

file(GLOB bagz_file_system_posix_sources "src/file/file_systems/posix/*.cc")
file(GLOB bagz_file_system_posix_headers "src/file/file_systems/posix/*.h")

bagz_cc_library(
  bagz_file_system_posix
  SOURCES ${bagz_file_system_posix_sources}
  HEADERS ${bagz_file_system_posix_headers}
  DEPS
    absl::status
    absl::statusor
    absl::strings
    bagz_file_system
    bagz_internal
)


file(GLOB bagz_file_system_gcs_sources "src/file/file_systems/gcs/*.cc")
file(GLOB bagz_file_system_gcs_headers "src/file/file_systems/gcs/*.h")

if(BAGZ_ENABLE_GCS)
  bagz_cc_library(
    bagz_file_system_gcs
    SOURCES ${bagz_file_system_gcs_sources}
    HEADERS ${bagz_file_system_gcs_headers}
    DEPS
      absl::log
      absl::status
      absl::statusor
      absl::strings
      bagz_file_system
      bagz_internal
      google-cloud-cpp::storage
  )
endif()

file(GLOB bagz_file_system_s3_sources "src/file/file_systems/s3/*.cc")
file(GLOB bagz_file_system_s3_headers "src/file/file_systems/s3/*.h")

if(BAGZ_ENABLE_S3)
  bagz_cc_library(
    bagz_file_system_s3
    SOURCES ${bagz_file_system_s3_sources}
    HEADERS ${bagz_file_system_s3_headers}
    DEPS
      absl::log
      absl::status
      absl::statusor
      absl::strings
      bagz_file_system
      bagz_internal
      aws-cpp-sdk-s3
      aws-cpp-sdk-core
      GTest::gtest
      GTest::gtest_main
      GTest::gmock
  )
endif()

file(GLOB bagz_file_sources "src/file/*.cc"  "src/file/registry/*.cc")
file(GLOB bagz_file_headers "src/file/*.h" "src/file/registry/*.h")

set(
  bagz_file_deps
  absl::log
  absl::status
  absl::statusor
  absl::strings
  bagz_internal
  bagz_file_system
  bagz_file_system_posix
)

if(BAGZ_ENABLE_GCS)
  list(APPEND bagz_file_deps bagz_file_system_gcs google-cloud-cpp::storage)
endif()

if(BAGZ_ENABLE_S3)
  list(APPEND bagz_file_deps bagz_file_system_s3 aws-cpp-sdk-s3 aws-cpp-sdk-core)
endif()

bagz_cc_library(
  bagz_file
  SOURCES ${bagz_file_sources}
  HEADERS ${bagz_file_headers}
  DEPS ${bagz_file_deps}
)

if(BAGZ_ENABLE_GCS)
  target_compile_definitions(bagz_file PRIVATE BAGZ_ENABLE_GCS=1)
else()
  target_compile_definitions(bagz_file PRIVATE BAGZ_ENABLE_GCS=0)
endif()

if(BAGZ_ENABLE_S3)
  target_compile_definitions(bagz_file PRIVATE BAGZ_ENABLE_S3=1)
else()
  target_compile_definitions(bagz_file PRIVATE BAGZ_ENABLE_S3=0)
endif()

file(GLOB bagz_core_sources "src/*.cc")
file(GLOB bagz_core_headers "src/*.h")

bagz_cc_library(
  bagz_core
  SOURCES ${bagz_core_sources}
  HEADERS ${bagz_core_headers}
  DEPS
    absl::flat_hash_map
    absl::log
    absl::status
    absl::statusor
    absl::strings
    bagz_file
    bagz_internal
    libzstd_static
)

file(GLOB bagz_python_sources "src/python/*.cc")

bagz_pybind11_extension(
  bagz
  SOURCES ${bagz_python_sources}
  DEPS
    absl::flat_hash_map
    absl::log
    absl::status
    absl::statusor
    absl::strings
    bagz_core
)

file(WRITE ${CMAKE_BINARY_DIR}/__init__.py [[# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from importlib import metadata as _metadata


def _bagz_has_dist(name: str) -> bool:
    try:
        _metadata.version(name)
        return True
    except _metadata.PackageNotFoundError:
        return False


if _bagz_has_dist("bagz") and _bagz_has_dist("bagz-freethreading"):
    raise RuntimeError(
        "Both 'bagz' and 'bagz-freethreading' are installed. "
        "Uninstall one to avoid a broken setup: "
        "pip uninstall bagz bagz-freethreading"
    )

from bagz.lib.bagz import *
]])

install(
  FILES "${CMAKE_BINARY_DIR}/__init__.py"
  DESTINATION "bagz"
)

install(
  TARGETS bagz
  DESTINATION "bagz/lib"
)

install(
  DIRECTORY "beam/"
  DESTINATION bagz/beam
  FILES_MATCHING
  PATTERN "*.py"
)

install(
  FILES "src/python/bagz.pyi"
  DESTINATION "bagz"
  RENAME "__init__.pyi"
)
