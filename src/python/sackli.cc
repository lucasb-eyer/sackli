// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Python.h>

#include <memory>

#include "src/internal/thread_pool.h"
#include "src/sackli_reader.h"
#include "src/python/sackli_index.h"
#include "src/python/sackli_multi_index.h"
#include "src/python/sackli_options.h"
#include "src/python/sackli_reader.h"
#include "src/python/sackli_writer.h"
#include "pybind11/gil.h"
#include "pybind11/pybind11.h"

namespace sackli {
namespace {

// Pins a Python thread state for the duration of a thread-pool task stint.
// Without this, every record allocation on a pool worker creates and destroys
// a PyThreadState (the cost of an outermost pybind11 gil_scoped_acquire on a
// non-Python thread), which is prohibitively expensive on free-threading
// builds. With the pin held, per-record acquisitions are plain GIL / thread
// attach round-trips. Member order matters: acquire creates the thread state,
// release detaches so record-level acquisitions work normally; destruction
// re-attaches and then drops the thread state.
struct PinnedPythonThreadState {
  pybind11::gil_scoped_acquire acquire;
  pybind11::gil_scoped_release release;
};

std::shared_ptr<void> MakePinnedPythonThreadState() {
  return std::make_shared<PinnedPythonThreadState>();
}

PYBIND11_MODULE(sackli, m, pybind11::mod_gil_not_used()) {
  m.doc() = "Sackli Python Bindings";
  internal::ThreadPool::SetTaskGuardFactory(&MakePinnedPythonThreadState);
  // With the GIL enabled, per-record allocations serialize worker threads,
  // so contiguous reads must not be split across threads.
  bool callbacks_scale = false;
  try {
    callbacks_scale = !pybind11::module_::import("sys")
                           .attr("_is_gil_enabled")()
                           .cast<bool>();
  } catch (const pybind11::error_already_set&) {
    // No sys._is_gil_enabled (< 3.13): the GIL is always enabled.
  }
  SackliReader::SetCallbackConcurrencyHint(callbacks_scale);
  RegisterSackliIndex(m);
  RegisterSackliMultiIndex(m);
  RegisterSackliOptions(m);
  RegisterSackliReader(m);
  RegisterSackliWriter(m);

  // Shim to allow `from sackli import sackli` for backward compatibility.
  m.attr("sackli") = m;
}

}  // namespace
}  // namespace sackli
