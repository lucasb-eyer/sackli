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

#include "src/python/sackli_index.h"
#include "src/python/sackli_multi_index.h"
#include "src/python/sackli_options.h"
#include "src/python/sackli_reader.h"
#include "src/python/sackli_writer.h"
#include "pybind11/pybind11.h"

namespace sackli {
namespace {

PYBIND11_MODULE(sackli, m, pybind11::mod_gil_not_used()) {
  m.doc() = "Sackli Python Bindings";
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
