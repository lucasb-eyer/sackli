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

#include "src/sackli_reader.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/no_destructor.h"
#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "src/sackli_iterator.h"
#include "src/sackli_options.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/gil.h"
#include "pybind11/numpy.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"
#include "pybind11/stl.h"

namespace sackli {
namespace {

namespace py = pybind11;

class FileNotFoundError : public py::builtin_exception {
 public:
  using py::builtin_exception::builtin_exception;

  void set_error() const override {
    PyErr_SetString(PyExc_FileNotFoundError, what());
  }
};

// Helper class to allocate results for callback-based reads.
//
// Records are parked in a plain C++ vector rather than a py::list: callbacks
// run concurrently and write to distinct slots, so no Python container is
// mutated from multiple threads (a shared list would serialize free-threading
// builds on its per-object lock). The caller moves the results into a list
// afterwards with MoveIntoList.
class IndexedAllocator {
 public:
  explicit IndexedAllocator(std::vector<py::object>& results)
      : results_(results) {}

  // Called without the GIL held, possibly from multiple threads with
  // distinct `result_index`es. The GIL is released again before returning.
  absl::Span<char> operator()(size_t result_index, ssize_t num_bytes) const {
    py::gil_scoped_acquire acquire;
    py::bytes record(nullptr, num_bytes);
    char* bytes;
    PyBytes_AsStringAndSize(record.ptr(), &bytes, &num_bytes);
    results_[result_index] = std::move(record);
    return absl::Span<char>(bytes, num_bytes);
  }

 private:
  std::vector<py::object>& results_;
};

// Helper to copy a result to the slots of duplicated indices.
class IndexedCopy {
 public:
  explicit IndexedCopy(std::vector<py::object>& results) : results_(results) {}

  // Called without the GIL held. The GIL is released again before returning.
  void operator()(size_t from_index, ssize_t to_index) const {
    py::gil_scoped_acquire acquire;
    results_[to_index] = results_[from_index];
  }

 private:
  std::vector<py::object>& results_;
};

// Moves fully-populated results into a py::list. Must be called with the GIL
// held; every slot of `results` must be non-null.
py::list MoveIntoList(std::vector<py::object>& results) {
  py::list list(results.size());
  for (size_t i = 0; i < results.size(); ++i) {
    // The new list's slots start out empty; SET_ITEM steals the reference.
    PyList_SET_ITEM(list.ptr(), static_cast<ssize_t>(i),
                    results[i].release().ptr());
  }
  return list;
}

void ThrowNonOkStatusAsException(const absl::Status& status) {
  if (!status.ok()) {
    if (absl::IsOutOfRange(status)) {
      throw py::index_error(std::string(status.message()));
    } else if (absl::IsNotFound(status)) {
      throw FileNotFoundError(status.ToString());
    }
    throw std::invalid_argument(status.ToString());
  }
}

constexpr char kOptionsDoc[] = R"(
Options for creating the sackli.Reader.

Args:
  sharding_layout: Specifies how input indexes/ranges are mapped to the
    underlying records within the shards. See README.md#sharding.
  limits_placement: Placement of the limits section on close defaulting to
    TAIL.
  compression: Compression algorithm to use defaulting to auto-detection.
  limits_storage: Whether to read the limits from disk for every read or to
    cache the limits in memory.
  max_parallelism: Maximum number of threads to use for operations that can be
    parallelized.
  access_pattern: Hint for how records are expected to be read from local
    files.
  cache_policy: Policy for how aggressively to retain record data in the local
    page cache or avoid it.
)";

constexpr char kInitDoc[] = R"(
Opens a collection of Sackli-formatted files (shards).

Args:
  file_spec: is either:
    * filename (e.g. "fs:/path/to/foo.bagz").
    * sharded file-spec (e.g. "fs:/path/to/foo@100.bagz").
    * comma-separated list of filenames and sharded file-specs
      (e.g. "fs:/path/to/f@3.bagz,fs:/path/to/bar.bagz").
  options: options to use when reading, see `sackli.Reader.Options`.
)";

SackliReader Init(py::object file_spec_obj, const SackliReader::Options& options) {
  static absl::NoDestructor<py::object> fspath(
      py::module::import("os").attr("fspath"));
  std::string file_spec = py::cast<std::string>((*fspath)(file_spec_obj));
  {
    py::gil_scoped_release release_gil;
    absl::StatusOr<SackliReader> reader = SackliReader::Open(file_spec, options);
    ThrowNonOkStatusAsException(reader.status());
    return *std::move(reader);
  }
}

constexpr char kReadRangeDoc[] = R"(
Returns all the records in the range [start, start + num_records).
)";

py::list ReadRange(const SackliReader& reader, size_t start, size_t num_records) {
  std::vector<py::object> results(num_records);
  {
    py::gil_scoped_release release;
    ThrowNonOkStatusAsException(reader.ReadRangeWithAllocator(
        start, num_records, IndexedAllocator(results)));
  }
  return MoveIntoList(results);
}

constexpr char kReadIndicesDoc[] = R"(
Returns the records at the given indices.
)";

py::list ReadIndicesFromSpan(const SackliReader& reader,
                             absl::Span<const size_t> indices) {
  std::vector<py::object> results(indices.size());
  {
    py::gil_scoped_release release;
    ThrowNonOkStatusAsException(reader.ReadIndicesWithAllocator(
        indices, IndexedAllocator(results), IndexedCopy(results)));
  }
  return MoveIntoList(results);
}

// Resolves negative indices relative to `num_records`, like Python sequences
// do. Indices that remain negative raise IndexError; indices that are too
// large are left for the reader to reject with its own range error.
std::vector<size_t> NormalizeNegativeIndices(absl::Span<const int64_t> indices,
                                             size_t num_records) {
  const int64_t size = static_cast<int64_t>(num_records);
  std::vector<size_t> normalized;
  normalized.reserve(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    int64_t index = indices[i];
    if (index < 0) {
      index += size;
      if (index < 0) {
        throw py::index_error(absl::StrCat("indices[", i, "] = ", indices[i],
                                           " out of range [", -size, ", ",
                                           size, ")"));
      }
    }
    normalized.push_back(static_cast<size_t>(index));
  }
  return normalized;
}

template <typename Int64>
py::list ReadIndicesFromNumpy(const SackliReader& reader,
                              py::array_t<Int64, py::array::c_style> indices) {
  static_assert(sizeof(Int64) == sizeof(size_t),
                "Int64 must be the same size as size_t");
  if (indices.ndim() != 1) {
    throw std::invalid_argument("indices must be a 1D array");
  }
  absl::Span<const Int64> span(indices.data(),
                               static_cast<size_t>(indices.shape()[0]));
  if constexpr (std::is_signed_v<Int64>) {
    if (absl::c_any_of(span, [](Int64 index) { return index < 0; })) {
      return ReadIndicesFromSpan(
          reader, NormalizeNegativeIndices(span, reader.size()));
    }
  }
  return ReadIndicesFromSpan(
      reader, absl::MakeConstSpan(
                  reinterpret_cast<const size_t*>(span.data()), span.size()));
}

py::list ReadIndicesFromIterable(const SackliReader& reader,
                                 std::vector<int64_t> indices) {
  return ReadIndicesFromSpan(reader,
                             NormalizeNegativeIndices(indices, reader.size()));
}

py::list ReadIndicesFromSlice(const SackliReader& reader, py::slice slice) {
  ssize_t start, stop, step, slicelength;
  if (!slice.compute(static_cast<ssize_t>(reader.size()), &start, &stop, &step,
                     &slicelength)) {
    throw py::index_error("Invalid slice");
  }
  if (step == 1) {
    return ReadRange(reader, start, slicelength);
  }
  std::vector<size_t> indices_vector;
  indices_vector.reserve(slicelength);
  ssize_t index = start;
  for (ssize_t i = 0; i < slicelength; ++i) {
    indices_vector.push_back(static_cast<size_t>(index));
    if (i + 1 < slicelength) {
      index += step;
    }
  }
  return ReadIndicesFromSpan(reader, indices_vector);
}

constexpr char kGetItemDoc[] = R"(
Returns the record at the given index.

Negative indices count from the end, like other Python sequences.
)";

py::bytes GetItem(const SackliReader& reader, ssize_t index) {
  const ssize_t size = static_cast<ssize_t>(reader.size());
  const ssize_t adjusted = index < 0 ? index + size : index;
  if (adjusted < 0 || adjusted >= size) {
    throw py::index_error(absl::StrCat("index ", index, " out of range [",
                                       -size, ", ", size, ")"));
  }
  py::bytes result;
  {
    py::gil_scoped_release release;
    ThrowNonOkStatusAsException(reader.ReadWithAllocator(
        static_cast<size_t>(adjusted),
        [&result](ssize_t num_bytes) -> absl::Span<char> {
          py::gil_scoped_acquire acquire;
          result = py::bytes(nullptr, num_bytes);
          char* bytes;
          PyBytes_AsStringAndSize(result.ptr(), &bytes, &num_bytes);
          return absl::Span<char>(bytes, num_bytes);
        }));
  }
  return result;
}

SackliReader GetSlice(const SackliReader& reader, py::slice slice) {
  ssize_t step, start, stop, slicelength;
  if (!slice.compute(static_cast<ssize_t>(reader.size()), &start, &stop, &step,
                     &slicelength)) {
    throw py::index_error("Invalid slice");
  }
  auto reader_slice = reader.Slice(start, step, slicelength);
  ThrowNonOkStatusAsException(reader_slice.status());
  return *std::move(reader_slice);
}

// Iterates over the reader and call a callback for each record in order.
// Early returns if the callback returns true.
// Every second, the GIL is acquired to check for signals.
// Returns whether any `callback` returned true.
template <typename CallBack>
bool AnyOf(SackliReader reader, CallBack&& callback) {
  py::gil_scoped_release release;
  SackliIterator iterator(std::move(reader));
  absl::Time time_start = absl::Now();
  for (;;) {
    auto result = iterator.next();
    if (!result.has_value()) {
      return false;
    }
    ThrowNonOkStatusAsException(result->status());
    if (callback(**result)) {
      return true;
    }
    absl::Time time_now = absl::Now();
    if (time_now - time_start > absl::Seconds(1)) {
      py::gil_scoped_acquire acquire;
      if (PyErr_CheckSignals() && PyErr_Occurred()) {
        throw py::error_already_set();
      }
      time_start = time_now;
    }
  }
}

constexpr char kIndexOfDoc[] = R"(
Returns the index of the first occurrence of the given value in the reader.

Raises a ValueError if the value is not found.
)";

size_t IndexOf(const SackliReader& reader, py::bytes value, ssize_t start,
               std::optional<ssize_t> stop) {
  absl::string_view bytes = py::cast<absl::string_view>(value);
  // Sequence.index semantics: negative bounds count from the end and both
  // bounds are clamped to [0, size]; the search range is [start, stop).
  const ssize_t size = static_cast<ssize_t>(reader.size());
  if (start < 0) {
    start = std::max<ssize_t>(start + size, 0);
  }
  ssize_t stop_index = stop.value_or(size);
  if (stop_index < 0) {
    stop_index += size;
  }
  stop_index = std::min(stop_index, size);
  if (start >= stop_index) {
    throw py::value_error("value is not in the sackli.Reader");
  }
  size_t index = start;
  auto reader_slice = reader.Slice(start, 1, stop_index - start);
  ThrowNonOkStatusAsException(reader_slice.status());
  if (!AnyOf(*std::move(reader_slice),
             [bytes, &index](absl::string_view record) {
               if (record == bytes) {
                 return true;
               }
               ++index;
               return false;
             })) {
    throw py::value_error("value is not in the sackli.Reader");
  }
  return index;
}

constexpr char kContainsDoc[] = R"(
Returns whether the given value is in the reader.
)";

bool Contains(const SackliReader& reader, py::bytes value) {
  auto bytes = py::cast<absl::string_view>(value);
  return AnyOf(reader,
               [bytes](absl::string_view record) { return record == bytes; });
}

constexpr char kCountDoc[] = R"(
Returns the number of occurrences of the given value in the reader.
)";

size_t Count(const SackliReader& reader, py::bytes value) {
  auto bytes = py::cast<absl::string_view>(value);
  size_t count = 0;
  AnyOf(reader, [bytes, &count](absl::string_view record) {
    if (record == bytes) {
      ++count;
    }
    return false;
  });
  return count;
}

// Iteration methods.

struct MakeBytes {
  py::bytes operator()(size_t num_bytes) const {
    py::gil_scoped_acquire acquire;
    return py::bytes(nullptr, num_bytes);
  }
};

struct SpanFromBytes {
  absl::Span<char> operator()(const py::bytes& result) const {
    py::gil_scoped_acquire acquire;
    char* bytes;
    ssize_t num_bytes;
    PyBytes_AsStringAndSize(result.ptr(), &bytes, &num_bytes);
    return absl::Span<char>(bytes, num_bytes);
  }
};

class ExceptionStore {
 public:
  void Store() {
    // Also clears the exception.
    PyErr_Fetch(&exception_, &value_, &traceback_);
  }
  void Restore() {
    if (HasException()) {
      PyErr_Restore(exception_, value_, traceback_);
      exception_ = nullptr;
      value_ = nullptr;
      traceback_ = nullptr;
    }
  }

  bool HasException() const { return exception_ != nullptr; }

  ~ExceptionStore() {
    Py_XDECREF(exception_);
    Py_XDECREF(value_);
    Py_XDECREF(traceback_);
  }

 private:
  PyObject* exception_ = nullptr;
  PyObject* value_ = nullptr;
  PyObject* traceback_ = nullptr;
};

// Helper to read batches of indices from a Python iterator.
class PythonBatchIterator {
 public:
  PythonBatchIterator(PyObject* indices_iter, size_t num_records,
                      ExceptionStore* exception_store)
      : indices_iter_(indices_iter),
        num_records_(num_records),
        exception_store_(exception_store) {}

  // Takes up to read_ahead indices from indices_iter_ and returns them in
  // indices. Any exceptions are stored in exception_store.
  // Use callback with GIL not held. GIL is released before returning.
  // Returns whether there were no Python exceptions or StopIteration occurred.
  bool operator()(size_t, size_t read_ahead,
                  std::vector<size_t>& indices) const {
    if (exception_store_->HasException()) {
      return false;
    }
    indices.reserve(read_ahead);
    {
      py::gil_scoped_acquire acquire;
      for (size_t index = 0; index < read_ahead; ++index) {
        PyObject* iter_obj = PyIter_Next(indices_iter_);
        if (iter_obj == nullptr) {
          if (PyErr_Occurred() &&
              !PyErr_ExceptionMatches(PyExc_StopIteration)) {
            exception_store_->Store();
            return !indices.empty();
          }
          PyErr_Clear();
          return true;
        }
        absl::Cleanup cleanup = [iter_obj]() { Py_XDECREF(iter_obj); };

        PyObject* index_obj = PyNumber_Index(iter_obj);
        if (PyErr_Occurred()) {
          exception_store_->Store();
          return !indices.empty();
        }
        int64_t result_index = PyLong_AsLongLong(index_obj);
        Py_DECREF(index_obj);
        if (result_index == -1) {
          if (PyErr_Occurred()) {
            exception_store_->Store();
            return !indices.empty();
          }
        }
        const int64_t original_index = result_index;
        if (result_index < 0) {
          result_index += static_cast<int64_t>(num_records_);
          if (result_index < 0) {
            std::string message = absl::StrCat(
                "index ", original_index, " out of range [",
                -static_cast<int64_t>(num_records_), ", ",
                num_records_, ")");
            PyErr_SetString(PyExc_IndexError, message.c_str());
            exception_store_->Store();
            return !indices.empty();
          }
        }
        indices.push_back(static_cast<size_t>(result_index));
      }
    }
    return true;
  }

  PyObject* indices_iter_;
  size_t num_records_;
  ExceptionStore* exception_store_;
};

class PythonIterator {
 public:
  // Iterator that returns py::bytes. Ensures GIL is held when creating/copying
  // py::bytes objects.
  using IteratorPyBytes =
      SackliIterator<MakeBytes, SpanFromBytes,
                   decltype([] { return py::gil_scoped_acquire(); })>;

  // Iterator that reads all records in the reader sequentially.
  PythonIterator(SackliReader reader, std::optional<size_t> read_ahead)
      : iterator_(
            std::make_unique<IteratorPyBytes>(std::move(reader), read_ahead)) {}

  // Iterator that reads records in the reader according to the sequence if
  // indices returned by index_iter.
  PythonIterator(SackliReader reader, py::object index_iter,
                 std::optional<size_t> read_ahead)
      : exception_store_(std::make_unique<ExceptionStore>()),
        index_iter_(std::move(index_iter)) {
    const size_t num_records = reader.size();
    iterator_ = std::make_unique<IteratorPyBytes>(
        std::move(reader), read_ahead,
        PythonBatchIterator(index_iter_.ptr(), num_records,
                            exception_store_.get()));
  }

  PythonIterator(PythonIterator&&) = default;
  ~PythonIterator() {
    if (iterator_ != nullptr) {
      py::gil_scoped_release release;
      iterator_ = nullptr;
    }
  }

  py::bytes next() {
    py::gil_scoped_release release;
    std::optional<absl::StatusOr<py::bytes>> result = iterator_->next();
    if (!result.has_value()) {
      if (exception_store_ != nullptr && exception_store_->HasException()) {
        py::gil_scoped_acquire acquire;
        exception_store_->Restore();
        throw py::error_already_set();
      }
      throw py::stop_iteration();
    }
    if (!result->ok()) {
      if (absl::IsAborted(result->status()) &&
          result->status().message().empty()) {
        if (exception_store_ != nullptr && exception_store_->HasException()) {
          py::gil_scoped_acquire acquire;
          exception_store_->Restore();
          if (PyErr_Occurred()) {
            throw py::error_already_set();
          }
        }
      }
      ThrowNonOkStatusAsException(result->status());
    }
    return *std::move(*result);
  }

 private:
  // Ensure exception_store_ address is valid if iterator is moved.
  // Can be Nullptr if no exception store is needed.
  std::unique_ptr<ExceptionStore> exception_store_;
  py::object index_iter_;
  std::unique_ptr<IteratorPyBytes> iterator_;
};

}  // namespace

void RegisterSackliReader(py::module& m) {
  auto register_sequence =
      py::module_::import("collections.abc").attr("Sequence").attr("register");

  auto reader = py::class_<SackliReader>(
      m, "Reader", "For reading a collection of Sackli-formatted shards.");

  auto reader_iterator = py::class_<PythonIterator>(
      m, "ReaderIterator", "Iterator for a SackliReader.");

  py::class_<SackliReader::Options>(reader, "Options", kOptionsDoc + 1)
      .def(
          py::init([](ShardingLayout sharding_layout,
                      LimitsPlacement limits_placement, Compression compression,
                      LimitsStorage limits_storage, int max_parallelism,
                      AccessPattern access_pattern, CachePolicy cache_policy) {
            return SackliReader::Options{
                .sharding_layout = sharding_layout,
                .limits_placement = limits_placement,
                .compression = compression,
                .limits_storage = limits_storage,
                .access_pattern = access_pattern,
                .cache_policy = cache_policy,
                .max_parallelism = max_parallelism,
            };
          }),
          py::arg("sharding_layout") = SackliReader::Options{}.sharding_layout,
          py::arg("limits_placement") = SackliReader::Options{}.limits_placement,
          py::arg("compression") = SackliReader::Options{}.compression,
          py::arg("limits_storage") = SackliReader::Options{}.limits_storage,
          py::arg("max_parallelism") = SackliReader::Options{}.max_parallelism,
          py::arg("access_pattern") = SackliReader::Options{}.access_pattern,
          py::arg("cache_policy") = SackliReader::Options{}.cache_policy)
      .def_readwrite("sharding_layout", &SackliReader::Options::sharding_layout)
      .def_readwrite("limits_placement", &SackliReader::Options::limits_placement)
      .def_readwrite("compression", &SackliReader::Options::compression)
      .def_readwrite("limits_storage", &SackliReader::Options::limits_storage)
      .def_readwrite("max_parallelism", &SackliReader::Options::max_parallelism)
      .def_readwrite("access_pattern", &SackliReader::Options::access_pattern)
      .def_readwrite("cache_policy", &SackliReader::Options::cache_policy);

  reader
      .def(py::init(&Init), py::arg("file_spec"),
           py::arg("options") = SackliReader::Options{}, py::doc(kInitDoc + 1))
      .def("__len__", &SackliReader::size)
      .def("__getitem__", &GetItem, py::arg("index"), py::doc(kGetItemDoc + 1))
      .def("__getitem__", &GetSlice, py::arg("slice"), py::doc(kGetItemDoc + 1))
      .def("__reversed__",
           [](const SackliReader& reader) {
             if (reader.size() == 0) {
               return reader;
             } else {
               auto reverse_reader =
                   reader.Slice(reader.size() - 1, -1, reader.size());
               ThrowNonOkStatusAsException(reverse_reader.status());
               return *std::move(reverse_reader);
             }
           })
      .def("approximate_bytes_per_record",
           &SackliReader::ApproximateNumBytesPerRecord)
      .def("read",
           [](const SackliReader& reader) {
             return ReadRange(reader, 0, reader.size());
           })
      .def(
          "read_range_iter",
          [](const SackliReader& reader, std::size_t start,
             std::size_t num_records,
             std::optional<size_t> read_ahead = std::nullopt) {
            auto reader_slice = reader.Slice(start, 1, num_records);
            ThrowNonOkStatusAsException(reader_slice.status());
            return PythonIterator(*std::move(reader_slice), read_ahead);
          },
          py::arg("start"), py::arg("num_records"), py::kw_only(),
          py::arg("read_ahead") = std::nullopt)
      .def(
          "read_indices_iter",
          [](const SackliReader& reader, py::object indices_iterable,
             std::optional<size_t> read_ahead = std::nullopt) {
            PyObject* indices_iter = PyObject_GetIter(indices_iterable.ptr());
            if (PyErr_Occurred()) {
              throw py::error_already_set();
            }
            return PythonIterator(
                reader, py::reinterpret_steal<py::object>(indices_iter),
                read_ahead);
          },
          py::arg("indices"), py::kw_only(),
          py::arg("read_ahead") = std::nullopt)
      .def("__iter__",
           [](const SackliReader& reader) {
             return PythonIterator(reader, std::nullopt);
           })
      .def("__contains__", &Contains, py::arg("value"),
           py::doc(kContainsDoc + 1))
      .def("index", &IndexOf, py::arg("value"), py::arg("start") = 0,
           py::arg("stop") = std::nullopt, py::doc(kIndexOfDoc + 1))
      .def("count", &Count, py::arg("value"), py::doc(kCountDoc + 1))
      .def("read_range", &ReadRange, py::arg("start"), py::arg("num_records"),
           py::doc(kReadRangeDoc + 1))
      .def("read_indices", &ReadIndicesFromNumpy<int64_t>, py::arg("indices"),
           py::doc(kReadIndicesDoc + 1))
      .def("read_indices", &ReadIndicesFromNumpy<uint64_t>, py::arg("indices"),
           py::doc(kReadIndicesDoc + 1))
      .def("read_indices", &ReadIndicesFromSlice, py::arg("indices"),
           py::doc(kReadIndicesDoc + 1))
      .def("read_indices", &ReadIndicesFromIterable, py::arg("indices"),
           py::doc(kReadIndicesDoc + 1));

  reader_iterator.def("__next__", &PythonIterator::next);
  reader_iterator.def(
      "__iter__",
      [](PythonIterator& iterator) -> PythonIterator& { return iterator; });
  register_sequence(reader);
}

}  // namespace sackli
