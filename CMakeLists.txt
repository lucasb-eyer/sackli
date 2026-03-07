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
project(sackli LANGUAGES CXX)

include(FetchContent)
option(BUILD_TESTING "Build tests" OFF)
include(CTest)

option(SACKLI_ENABLE_GCS "Build Google Cloud Storage filesystem support" ON)
option(SACKLI_ENABLE_S3 "Build Amazon S3 filesystem support" ON)
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
if(SACKLI_ENABLE_GCS)
  include(cmake/third_party_gcs.cmake)
endif()
if(SACKLI_ENABLE_S3)
  include(cmake/third_party_aws.cmake)
endif()

# Restore original CXX_FLAGS
set(CMAKE_CXX_FLAGS "${SAVED_CMAKE_CXX_FLAGS}")

set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS FALSE)

set(CMAKE_SKIP_INSTALL_RULES OFF)

include(cmake/sackli_cc.cmake)

file(GLOB sackli_internal_sources "src/internal/*.cc")
file(GLOB sackli_internal_headers "src/internal/*.h")

sackli_cc_library(
  sackli_internal
  SOURCES ${sackli_internal_sources}
  HEADERS ${sackli_internal_headers}
  DEPS
    absl::strings
    absl::status
    absl::statusor
    absl::flat_hash_map
    absl::log
    libzstd_static
)

file(GLOB sackli_file_system_sources "src/file/file_system/*.cc")
file(GLOB sackli_file_system_headers "src/file/file_system/*.h")

sackli_cc_library(
  sackli_file_system
  SOURCES ${sackli_file_system_sources}
  HEADERS ${sackli_file_system_headers}
  DEPS
    absl::flat_hash_map
    absl::log
    absl::status
    absl::statusor
    absl::strings
    sackli_internal
)

file(GLOB sackli_file_system_posix_sources "src/file/file_systems/posix/*.cc")
file(GLOB sackli_file_system_posix_headers "src/file/file_systems/posix/*.h")

sackli_cc_library(
  sackli_file_system_posix
  SOURCES ${sackli_file_system_posix_sources}
  HEADERS ${sackli_file_system_posix_headers}
  DEPS
    absl::status
    absl::statusor
    absl::strings
    sackli_file_system
    sackli_internal
)


file(GLOB sackli_file_system_gcs_sources "src/file/file_systems/gcs/*.cc")
file(GLOB sackli_file_system_gcs_headers "src/file/file_systems/gcs/*.h")

if(SACKLI_ENABLE_GCS)
  sackli_cc_library(
    sackli_file_system_gcs
    SOURCES ${sackli_file_system_gcs_sources}
    HEADERS ${sackli_file_system_gcs_headers}
    DEPS
      absl::log
      absl::status
      absl::statusor
      absl::strings
      sackli_file_system
      sackli_internal
      google-cloud-cpp::storage
  )
endif()

file(GLOB sackli_file_system_s3_sources "src/file/file_systems/s3/*.cc")
file(GLOB sackli_file_system_s3_headers "src/file/file_systems/s3/*.h")

if(SACKLI_ENABLE_S3)
  sackli_cc_library(
    sackli_file_system_s3
    SOURCES ${sackli_file_system_s3_sources}
    HEADERS ${sackli_file_system_s3_headers}
    DEPS
      absl::log
      absl::status
      absl::statusor
      absl::strings
      sackli_file_system
      sackli_internal
      aws-cpp-sdk-s3
      aws-cpp-sdk-core
      GTest::gtest
      GTest::gtest_main
      GTest::gmock
  )
endif()

file(GLOB sackli_file_sources "src/file/*.cc"  "src/file/registry/*.cc")
file(GLOB sackli_file_headers "src/file/*.h" "src/file/registry/*.h")

set(
  sackli_file_deps
  absl::log
  absl::status
  absl::statusor
  absl::strings
  sackli_internal
  sackli_file_system
  sackli_file_system_posix
)

if(SACKLI_ENABLE_GCS)
  list(APPEND sackli_file_deps sackli_file_system_gcs google-cloud-cpp::storage)
endif()

if(SACKLI_ENABLE_S3)
  list(APPEND sackli_file_deps sackli_file_system_s3 aws-cpp-sdk-s3 aws-cpp-sdk-core)
endif()

sackli_cc_library(
  sackli_file
  SOURCES ${sackli_file_sources}
  HEADERS ${sackli_file_headers}
  DEPS ${sackli_file_deps}
)

if(SACKLI_ENABLE_GCS)
  target_compile_definitions(sackli_file PRIVATE SACKLI_ENABLE_GCS=1)
else()
  target_compile_definitions(sackli_file PRIVATE SACKLI_ENABLE_GCS=0)
endif()

if(SACKLI_ENABLE_S3)
  target_compile_definitions(sackli_file PRIVATE SACKLI_ENABLE_S3=1)
else()
  target_compile_definitions(sackli_file PRIVATE SACKLI_ENABLE_S3=0)
endif()

file(GLOB sackli_core_sources "src/*.cc")
file(GLOB sackli_core_headers "src/*.h")
list(FILTER sackli_core_sources EXCLUDE REGEX "_test\\.cc$")

sackli_cc_library(
  sackli_core
  SOURCES ${sackli_core_sources}
  HEADERS ${sackli_core_headers}
  DEPS
    absl::flat_hash_map
    absl::log
    absl::status
    absl::statusor
    absl::strings
    sackli_file
    sackli_internal
    libzstd_static
)

file(GLOB sackli_python_sources "src/python/*.cc")

sackli_pybind11_extension(
  sackli
  SOURCES ${sackli_python_sources}
  DEPS
    absl::flat_hash_map
    absl::log
    absl::status
    absl::statusor
    absl::strings
    sackli_core
)

if(BUILD_TESTING)
  add_executable(
    sackli_reader_test
    src/sackli_reader_test.cc
  )
  target_include_directories(
    sackli_reader_test
    PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}"
  )
  target_link_libraries(
    sackli_reader_test
    PRIVATE
      absl::status
      absl::statusor
      absl::strings
      sackli_core
  )
  add_test(NAME sackli_reader_test COMMAND sackli_reader_test)
endif()

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


def _sackli_has_dist(name: str) -> bool:
    try:
        _metadata.version(name)
        return True
    except _metadata.PackageNotFoundError:
        return False


if _sackli_has_dist("sackli") and _sackli_has_dist("sackli-freethreading"):
    raise RuntimeError(
        "Both 'sackli' and 'sackli-freethreading' are installed. "
        "Uninstall one to avoid a broken setup: "
        "pip uninstall sackli sackli-freethreading"
    )

from sackli.lib.sackli import *
]])

install(
  FILES "${CMAKE_BINARY_DIR}/__init__.py"
  DESTINATION "sackli"
)

install(
  TARGETS sackli
  DESTINATION "sackli/lib"
)

install(
  DIRECTORY "beam/"
  DESTINATION sackli/beam
  FILES_MATCHING
  PATTERN "*.py"
)

install(
  FILES "src/python/sackli.pyi"
  DESTINATION "sackli"
  RENAME "__init__.pyi"
)
