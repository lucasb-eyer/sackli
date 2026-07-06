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

// Helpers for accepting user-friendly option values in the Python bindings:
// every option enum can be given as its value name string
// (case-insensitive), and compression additionally as "auto"/"none"/"zstd".

#ifndef SACKLI_SRC_PYTHON_OPTION_CONVERSIONS_H_
#define SACKLI_SRC_PYTHON_OPTION_CONVERSIONS_H_

#include <cctype>
#include <string>

#include "absl/strings/str_cat.h"
#include "src/sackli_options.h"
#include "pybind11/cast.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"
#include "pybind11/stl.h"

namespace sackli::internal {

namespace py = pybind11;

// Converts `value` to the option enum `E`, accepting either an `E` instance
// or the case-insensitive name of one of its values (e.g. "drop_after_read").
template <typename E>
E ToOptionEnum(py::handle value) {
  if (!py::isinstance<py::str>(value)) {
    return py::cast<E>(value);
  }
  std::string name = py::cast<std::string>(value);
  for (char& c : name) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  const py::object enum_type = py::type::of(py::cast(E{}));
  if (!py::hasattr(enum_type, name.c_str())) {
    std::string valid_names;
    for (const auto& member : enum_type.attr("__members__")) {
      std::string member_name = py::cast<std::string>(member);
      for (char& c : member_name) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      absl::StrAppend(&valid_names, valid_names.empty() ? "'" : ", '",
                      member_name, "'");
    }
    throw py::value_error(absl::StrCat(
        "invalid ", py::cast<std::string>(enum_type.attr("__name__")), " '",
        py::cast<std::string>(value), "'; expected one of ", valid_names));
  }
  return py::cast<E>(enum_type.attr(name.c_str()));
}

// Converts `value` to a Compression, accepting a Compression* instance or
// one of the strings "auto", "none", "zstd".
inline Compression ToCompression(py::handle value) {
  if (!py::isinstance<py::str>(value)) {
    return py::cast<Compression>(value);
  }
  std::string name = py::cast<std::string>(value);
  for (char& c : name) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  if (name == "auto") {
    return CompressionAutoDetect{};
  }
  if (name == "none") {
    return CompressionNone{};
  }
  if (name == "zstd") {
    return CompressionZstd{};
  }
  throw py::value_error(
      absl::StrCat("invalid compression '", py::cast<std::string>(value),
                   "'; expected 'auto', 'none', 'zstd', or a Compression* "
                   "instance"));
}

}  // namespace sackli::internal

#endif  // SACKLI_SRC_PYTHON_OPTION_CONVERSIONS_H_
