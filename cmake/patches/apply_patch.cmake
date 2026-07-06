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

# Applies a git patch idempotently. Usage (run in the source directory):
#   cmake -DPATCH_FILE=<path> -P apply_patch.cmake
# A patch that is already applied is a no-op; anything else that prevents a
# clean application is an error.

execute_process(
  COMMAND git apply --reverse --check "${PATCH_FILE}"
  RESULT_VARIABLE already_applied
  ERROR_QUIET OUTPUT_QUIET
)
if(already_applied EQUAL 0)
  return()
endif()

execute_process(
  COMMAND git apply "${PATCH_FILE}"
  RESULT_VARIABLE apply_result
)
if(NOT apply_result EQUAL 0)
  message(FATAL_ERROR "Failed to apply patch: ${PATCH_FILE}")
endif()
