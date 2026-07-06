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

#ifndef SACKLI_SRC_PYTHON_SACKLI_READER_H_
#define SACKLI_SRC_PYTHON_SACKLI_READER_H_


#include "pybind11/pybind11.h"

namespace sackli {

void RegisterSackliReader(pybind11::module& m);

// Selects the buffered-record strategy for batch reads and iteration:
// workers read into plain C++ buffers which are converted to bytes under a
// single GIL hold afterwards (one extra copy per record). Enable on
// GIL-enabled interpreters, where per-record bytes allocation from worker
// threads would serialize on the GIL; keep disabled on free-threading
// builds, which write into bytes objects zero-copy.
void SetUseBufferedRecords(bool use_buffered);

}  // namespace sackli

#endif  // SACKLI_SRC_PYTHON_SACKLI_READER_H_
