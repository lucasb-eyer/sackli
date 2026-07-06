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

#ifndef SACKLI_SRC_PYTHON_STATUS_TO_EXCEPTION_H_
#define SACKLI_SRC_PYTHON_STATUS_TO_EXCEPTION_H_

#include <Python.h>

#include <string>

#include "absl/status/status.h"
#include "pybind11/pybind11.h"

namespace sackli::internal {

// A pybind-throwable exception raising an arbitrary builtin exception type.
class BuiltinException : public pybind11::builtin_exception {
 public:
  BuiltinException(PyObject* type, const std::string& what)
      : pybind11::builtin_exception(what), type_(type) {}

  void set_error() const override { PyErr_SetString(type_, what()); }

 private:
  PyObject* type_;  // Borrowed reference to a static exception type.
};

// Throws the Python exception that best matches a non-OK status:
// out-of-range indices raise IndexError, missing files FileNotFoundError,
// permission problems PermissionError, bad arguments/state ValueError,
// unsupported features NotImplementedError, library invariant violations
// RuntimeError, and other I/O-ish failures OSError.
[[noreturn]] inline void ThrowStatusAsPyException(const absl::Status& status) {
  switch (status.code()) {
    case absl::StatusCode::kOutOfRange:
      throw pybind11::index_error(std::string(status.message()));
    case absl::StatusCode::kNotFound:
      throw BuiltinException(PyExc_FileNotFoundError, status.ToString());
    case absl::StatusCode::kPermissionDenied:
      throw BuiltinException(PyExc_PermissionError, status.ToString());
    case absl::StatusCode::kUnimplemented:
      throw BuiltinException(PyExc_NotImplementedError, status.ToString());
    case absl::StatusCode::kInvalidArgument:
    case absl::StatusCode::kFailedPrecondition:
      throw pybind11::value_error(status.ToString());
    case absl::StatusCode::kInternal:
      throw BuiltinException(PyExc_RuntimeError, status.ToString());
    default:
      throw BuiltinException(PyExc_OSError, status.ToString());
  }
}

// Throws if `status` is not OK; see ThrowStatusAsPyException.
inline void ThrowIfNotOk(const absl::Status& status) {
  if (!status.ok()) {
    ThrowStatusAsPyException(status);
  }
}

}  // namespace sackli::internal

#endif  // SACKLI_SRC_PYTHON_STATUS_TO_EXCEPTION_H_
