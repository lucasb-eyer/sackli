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

# The patch caches pybind11's C++-type -> type_info resolution per template
# instantiation. Without it, every bound-method call resolves the `self` type
# under pybind11's global internals mutex, which serializes all dispatch on
# free-threading Python: with ~200 reader threads, per-record reads collapse
# to ~4x below their 8-thread throughput. See the patch file for details.
FetchContent_Declare(
  pybind11
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG v3.0.0 # v3.0.0
  GIT_SHALLOW TRUE
  PATCH_COMMAND
    ${CMAKE_COMMAND}
    -DPATCH_FILE=${CMAKE_CURRENT_LIST_DIR}/patches/pybind11-cache-type-lookup.patch
    -P ${CMAKE_CURRENT_LIST_DIR}/patches/apply_patch.cmake
  OVERRIDE_FIND_PACKAGE
  EXCLUDE_FROM_ALL
)

FetchContent_MakeAvailable(pybind11)
