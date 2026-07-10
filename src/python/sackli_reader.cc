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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
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
#include "src/python/option_conversions.h"
#include "src/python/status_to_exception.h"
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

// On GIL builds, per-record bytes allocation from worker threads serializes
// on the GIL, making parallel reads slower than serial ones. There, workers
// read into plain C++ buffers instead (no GIL per record) and the results
// are converted to bytes in one pass under a single GIL hold, at the cost of
// one extra copy per record. Free-threading builds keep the zero-copy path
// above. Set once at module initialization.
std::atomic<bool> use_buffered_records{false};

// Whether `reader` should use buffered records: only worthwhile when worker
// threads are in play; serial readers keep the zero-copy path since the
// extra copy buys them nothing.
bool UseBufferedRecords(const SackliReader& reader) {
  return use_buffered_records.load(std::memory_order_relaxed) &&
         !reader.IsClosed() && reader.options().max_parallelism > 1;
}

// One record read into a plain C++ buffer. The buffer is shared so that
// duplicated indices can alias it cheaply.
struct RawRecord {
  std::shared_ptr<char[]> data;
  size_t size = 0;
};

RawRecord MakeRawRecordOfSize(size_t num_bytes) {
  std::shared_ptr<char[]> data;
  if (num_bytes > 0) {
#if SACKLI_HAVE_MAKE_SHARED_FOR_OVERWRITE
    data = std::make_shared_for_overwrite<char[]>(num_bytes);
#else
    // Older standard libraries do not provide make_shared_for_overwrite.
    data = std::shared_ptr<char[]>(new char[num_bytes]);
#endif
  }
  return RawRecord{std::move(data), num_bytes};
}

// Counterpart of IndexedAllocator/IndexedCopy for RawRecords; runs entirely
// without the GIL.
class RawIndexedAllocator {
 public:
  explicit RawIndexedAllocator(std::vector<RawRecord>& results)
      : results_(results) {}

  absl::Span<char> operator()(size_t result_index, size_t num_bytes) const {
    RawRecord& record = results_[result_index] = MakeRawRecordOfSize(num_bytes);
    return absl::Span<char>(record.data.get(), record.size);
  }

 private:
  std::vector<RawRecord>& results_;
};

class RawIndexedCopy {
 public:
  explicit RawIndexedCopy(std::vector<RawRecord>& results)
      : results_(results) {}

  void operator()(size_t from_index, size_t to_index) const {
    results_[to_index] = results_[from_index];
  }

 private:
  std::vector<RawRecord>& results_;
};

py::bytes RawRecordToBytes(const RawRecord& record) {
  PyObject* bytes = PyBytes_FromStringAndSize(
      record.data.get(), static_cast<ssize_t>(record.size));
  if (bytes == nullptr) {
    throw py::error_already_set();
  }
  return py::reinterpret_steal<py::bytes>(bytes);
}

// Converts raw records to a py::list of bytes. Must be called with the GIL
// held. Records aliasing the same buffer (duplicated indices) become the
// same bytes object, matching the zero-copy path.
py::list RawRecordsToList(absl::Span<const RawRecord> records) {
  py::list list(records.size());
  absl::flat_hash_map<const char*, PyObject*> aliased;
  for (size_t i = 0; i < records.size(); ++i) {
    const RawRecord& record = records[i];
    PyObject* bytes;
    if (record.data != nullptr && record.data.use_count() > 1) {
      auto [it, inserted] = aliased.try_emplace(record.data.get(), nullptr);
      if (!inserted) {
        bytes = it->second;
        Py_INCREF(bytes);
      } else {
        bytes = RawRecordToBytes(record).release().ptr();
        it->second = bytes;
      }
    } else {
      bytes = RawRecordToBytes(record).release().ptr();
    }
    PyList_SET_ITEM(list.ptr(), static_cast<ssize_t>(i), bytes);
  }
  return list;
}

constexpr char kOptionsDoc[] = R"(
Options for creating the sackli.Reader.

Enum-valued options also accept the case-insensitive name of an enum value
(e.g. `cache_policy="drop_after_read"`), and `compression` additionally
accepts "auto", "none" or "zstd".

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
  read_ahead_bytes: Byte budget that sizes the record batches read ahead when
    iterating (defaults to 1 MiB). For compressed files, the budget counts
    compressed on-disk bytes; decompressed records can require substantially
    more RAM, especially for high compression ratios or large records. Up to
    two batches can be in flight, so this is a sizing heuristic rather than a
    memory bound. The `read_ahead` argument of the iterator methods, which
    counts records, takes precedence when given.
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
  **kwargs: any `sackli.Reader.Options` field can also be passed directly
    (e.g. `sackli.Reader(path, cache_policy="drop_after_read")`), overriding
    the corresponding field of `options`.
)";

constexpr char kCloseDoc[] = R"(
Closes this reader handle; further operations on it raise ValueError.

The underlying files are closed once the last handle sharing them (this
reader, slices made from it, and any live iterators) is closed or garbage
collected. Do not call concurrently with in-flight reads on this handle.
)";

// Applies one Reader.Options field given as a keyword argument.
void ApplyReaderOptionKwarg(SackliReader::Options& options,
                            const std::string& key, py::handle value) {
  if (key == "sharding_layout") {
    options.sharding_layout = internal::ToOptionEnum<ShardingLayout>(value);
  } else if (key == "limits_placement") {
    options.limits_placement = internal::ToOptionEnum<LimitsPlacement>(value);
  } else if (key == "compression") {
    options.compression = internal::ToCompression(value);
  } else if (key == "limits_storage") {
    options.limits_storage = internal::ToOptionEnum<LimitsStorage>(value);
  } else if (key == "max_parallelism") {
    options.max_parallelism = py::cast<int>(value);
  } else if (key == "access_pattern") {
    options.access_pattern = internal::ToOptionEnum<AccessPattern>(value);
  } else if (key == "cache_policy") {
    options.cache_policy = internal::ToOptionEnum<CachePolicy>(value);
  } else if (key == "read_ahead_bytes") {
    options.read_ahead_bytes = py::cast<std::optional<size_t>>(value);
  } else {
    throw py::type_error(
        absl::StrCat("got an unexpected keyword argument '", key, "'"));
  }
}

// Raises ValueError for operations on a closed reader, mirroring closed
// Python file objects.
void EnsureOpen(const SackliReader& reader) {
  if (reader.IsClosed()) {
    throw py::value_error("Reader is closed.");
  }
}

SackliReader Init(py::object file_spec_obj, const SackliReader::Options& options) {
  static absl::NoDestructor<py::object> fspath(
      py::module::import("os").attr("fspath"));
  std::string file_spec = py::cast<std::string>((*fspath)(file_spec_obj));
  {
    py::gil_scoped_release release_gil;
    absl::StatusOr<SackliReader> reader = SackliReader::Open(file_spec, options);
    internal::ThrowIfNotOk(reader.status());
    return *std::move(reader);
  }
}

constexpr char kReadRangeDoc[] = R"(
Returns all the records in the range [start, start + num_records).
)";

py::list ReadRange(const SackliReader& reader, size_t start, size_t num_records) {
  EnsureOpen(reader);
  if (UseBufferedRecords(reader)) {
    std::vector<RawRecord> results(num_records);
    {
      py::gil_scoped_release release;
      internal::ThrowIfNotOk(reader.ReadRangeWithAllocator(
          start, num_records, RawIndexedAllocator(results)));
    }
    return RawRecordsToList(results);
  }
  std::vector<py::object> results(num_records);
  {
    py::gil_scoped_release release;
    internal::ThrowIfNotOk(reader.ReadRangeWithAllocator(
        start, num_records, IndexedAllocator(results)));
  }
  return MoveIntoList(results);
}

constexpr char kReadIndicesDoc[] = R"(
Returns the records at the given indices.
)";

py::list ReadIndicesFromSpan(const SackliReader& reader,
                             absl::Span<const size_t> indices) {
  EnsureOpen(reader);
  if (UseBufferedRecords(reader)) {
    std::vector<RawRecord> results(indices.size());
    {
      py::gil_scoped_release release;
      internal::ThrowIfNotOk(reader.ReadIndicesWithAllocator(
          indices, RawIndexedAllocator(results), RawIndexedCopy(results)));
    }
    return RawRecordsToList(results);
  }
  std::vector<py::object> results(indices.size());
  {
    py::gil_scoped_release release;
    internal::ThrowIfNotOk(reader.ReadIndicesWithAllocator(
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
  EnsureOpen(reader);
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
  EnsureOpen(reader);
  return ReadIndicesFromSpan(reader,
                             NormalizeNegativeIndices(indices, reader.size()));
}

py::list ReadIndicesFromSlice(const SackliReader& reader, py::slice slice) {
  EnsureOpen(reader);
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
  EnsureOpen(reader);
  const ssize_t size = static_cast<ssize_t>(reader.size());
  const ssize_t adjusted = index < 0 ? index + size : index;
  if (adjusted < 0 || adjusted >= size) {
    throw py::index_error(absl::StrCat("index ", index, " out of range [",
                                       -size, ", ", size, ")"));
  }
  py::bytes result;
  {
    py::gil_scoped_release release;
    internal::ThrowIfNotOk(reader.ReadWithAllocator(
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
  EnsureOpen(reader);
  ssize_t step, start, stop, slicelength;
  if (!slice.compute(static_cast<ssize_t>(reader.size()), &start, &stop, &step,
                     &slicelength)) {
    throw py::index_error("Invalid slice");
  }
  auto reader_slice = reader.Slice(start, step, slicelength);
  internal::ThrowIfNotOk(reader_slice.status());
  return *std::move(reader_slice);
}

// Iterates over the reader and call a callback for each record in order.
// Early returns if the callback returns true.
// Every second, the GIL is acquired to check for signals.
// Returns whether any `callback` returned true.
template <typename CallBack>
bool AnyOf(SackliReader reader, CallBack&& callback) {
  EnsureOpen(reader);
  py::gil_scoped_release release;
  SackliIterator iterator(std::move(reader));
  absl::Time time_start = absl::Now();
  for (;;) {
    auto result = iterator.next();
    if (!result.has_value()) {
      return false;
    }
    internal::ThrowIfNotOk(result->status());
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
  EnsureOpen(reader);
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
  internal::ThrowIfNotOk(reader_slice.status());
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

// Read-ahead helpers for RawRecords; run entirely without the GIL.
struct MakeRawRecord {
  RawRecord operator()(size_t num_bytes) const {
    return MakeRawRecordOfSize(num_bytes);
  }
};

struct SpanFromRawRecord {
  absl::Span<char> operator()(const RawRecord& record) const {
    return absl::Span<char>(record.data.get(), record.size);
  }
};

class PythonIterator {
 public:
  // Iterator that returns py::bytes zero-copy. Ensures the GIL is held when
  // creating/copying py::bytes objects; used on free-threading builds.
  using IteratorPyBytes =
      SackliIterator<MakeBytes, SpanFromBytes,
                   decltype([] { return py::gil_scoped_acquire(); })>;
  // Iterator over plain C++ buffers, converted to bytes in next(); used on
  // GIL builds where per-record GIL acquisition from worker threads would
  // serialize the read-ahead. See UseBufferedRecords().
  using IteratorRawRecord =
      SackliIterator<MakeRawRecord, SpanFromRawRecord, internal::NoOpEditGuard>;

  // Iterator that reads all records in the reader sequentially.
  PythonIterator(SackliReader reader, std::optional<size_t> read_ahead)
      : iterator_(MakeIterator(std::move(reader), read_ahead, {})) {}

  // Iterator that reads records in the reader according to the sequence of
  // indices returned by index_iter.
  PythonIterator(SackliReader reader, py::object index_iter,
                 std::optional<size_t> read_ahead)
      : exception_store_(std::make_unique<ExceptionStore>()),
        index_iter_(std::move(index_iter)) {
    const size_t num_records = reader.size();
    iterator_ = MakeIterator(
        std::move(reader), read_ahead,
        PythonBatchIterator(index_iter_.ptr(), num_records,
                            exception_store_.get()));
  }

  PythonIterator(PythonIterator&&) = default;
  ~PythonIterator() {
    py::gil_scoped_release release;
    iterator_ = {};
  }

  py::bytes next() {
    if (auto* raw = std::get_if<std::unique_ptr<IteratorRawRecord>>(
            &iterator_)) {
      std::optional<absl::StatusOr<RawRecord>> result;
      {
        py::gil_scoped_release release;
        result = (*raw)->next();
      }
      return RawRecordToBytes(Unwrap(std::move(result)));
    }
    auto& iterator = std::get<std::unique_ptr<IteratorPyBytes>>(iterator_);
    py::gil_scoped_release release;
    return Unwrap(iterator->next());
  }

 private:
  using IteratorVariant = std::variant<std::unique_ptr<IteratorPyBytes>,
                                       std::unique_ptr<IteratorRawRecord>>;

  static IteratorVariant MakeIterator(SackliReader reader,
                                      std::optional<size_t> read_ahead,
                                      SequenceReadBatch read_batch) {
    if (UseBufferedRecords(reader)) {
      return std::make_unique<IteratorRawRecord>(std::move(reader), read_ahead,
                                                 std::move(read_batch));
    }
    return std::make_unique<IteratorPyBytes>(std::move(reader), read_ahead,
                                             std::move(read_batch));
  }

  // Turns the underlying iterator's result into a value or the fitting
  // exception. Works with or without the GIL held; the exception paths
  // acquire it as needed.
  template <typename T>
  T Unwrap(std::optional<absl::StatusOr<T>> result) {
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
      internal::ThrowIfNotOk(result->status());
    }
    return *std::move(*result);
  }

  // Ensure exception_store_ address is valid if iterator is moved.
  // Can be Nullptr if no exception store is needed.
  std::unique_ptr<ExceptionStore> exception_store_;
  py::object index_iter_;
  IteratorVariant iterator_;
};

}  // namespace

void SetUseBufferedRecords(bool use_buffered) {
  use_buffered_records.store(use_buffered, std::memory_order_relaxed);
}

void RegisterSackliReader(py::module& m) {
  auto register_sequence =
      py::module_::import("collections.abc").attr("Sequence").attr("register");

  auto reader = py::class_<SackliReader>(
      m, "Reader", "For reading a collection of Sackli-formatted shards.");

  auto reader_iterator = py::class_<PythonIterator>(
      m, "ReaderIterator", "Iterator for a SackliReader.");

  py::class_<SackliReader::Options>(reader, "Options", kOptionsDoc + 1)
      .def(py::init([](const py::kwargs& kwargs) {
             SackliReader::Options options{};
             for (const auto& item : kwargs) {
               ApplyReaderOptionKwarg(
                   options, py::cast<std::string>(item.first), item.second);
             }
             return options;
           }),
           py::doc(kOptionsDoc + 1))
      .def_readwrite("sharding_layout", &SackliReader::Options::sharding_layout)
      .def_readwrite("limits_placement", &SackliReader::Options::limits_placement)
      .def_readwrite("compression", &SackliReader::Options::compression)
      .def_readwrite("limits_storage", &SackliReader::Options::limits_storage)
      .def_readwrite("max_parallelism", &SackliReader::Options::max_parallelism)
      .def_readwrite("access_pattern", &SackliReader::Options::access_pattern)
      .def_readwrite("cache_policy", &SackliReader::Options::cache_policy)
      .def_readwrite("read_ahead_bytes",
                     &SackliReader::Options::read_ahead_bytes);

  reader
      .def(py::init([](py::object file_spec,
                       std::optional<SackliReader::Options> options,
                       const py::kwargs& kwargs) {
             SackliReader::Options merged =
                 options.value_or(SackliReader::Options{});
             for (const auto& item : kwargs) {
               ApplyReaderOptionKwarg(
                   merged, py::cast<std::string>(item.first), item.second);
             }
             return Init(std::move(file_spec), merged);
           }),
           py::arg("file_spec"), py::arg("options") = py::none(),
           py::doc(kInitDoc + 1))
      .def("__len__",
           [](const SackliReader& reader) {
             EnsureOpen(reader);
             return reader.size();
           })
      .def("close", &SackliReader::Close, py::doc(kCloseDoc + 1))
      .def_property_readonly("closed", &SackliReader::IsClosed,
                             "Whether this reader handle has been closed.")
      .def("__enter__",
           [](const SackliReader& reader) -> const SackliReader& {
             EnsureOpen(reader);
             return reader;
           })
      .def(
          "__exit__",
          [](SackliReader& reader, py::handle exc_type, py::handle exc_value,
             py::handle traceback) { reader.Close(); },
          py::arg("exc_type"), py::arg("exc_value"), py::arg("traceback"))
      .def("__getitem__", &GetItem, py::arg("index"), py::doc(kGetItemDoc + 1))
      .def("__getitem__", &GetSlice, py::arg("slice"), py::doc(kGetItemDoc + 1))
      .def("__reversed__",
           [](const SackliReader& reader) {
             EnsureOpen(reader);
             if (reader.size() == 0) {
               return PythonIterator(reader, std::nullopt);
             }
             auto reverse_reader =
                 reader.Slice(reader.size() - 1, -1, reader.size());
             internal::ThrowIfNotOk(reverse_reader.status());
             return PythonIterator(*std::move(reverse_reader), std::nullopt);
           })
      .def("approximate_bytes_per_record",
           [](const SackliReader& reader) {
             EnsureOpen(reader);
             return reader.ApproximateNumBytesPerRecord();
           })
      .def("read",
           [](const SackliReader& reader) {
             return ReadRange(reader, 0, reader.size());
           })
      .def(
          "read_range_iter",
          [](const SackliReader& reader, std::size_t start,
             std::size_t num_records,
             std::optional<size_t> read_ahead = std::nullopt) {
            EnsureOpen(reader);
            auto reader_slice = reader.Slice(start, 1, num_records);
            internal::ThrowIfNotOk(reader_slice.status());
            return PythonIterator(*std::move(reader_slice), read_ahead);
          },
          py::arg("start"), py::arg("num_records"), py::kw_only(),
          py::arg("read_ahead") = std::nullopt)
      .def(
          "read_indices_iter",
          [](const SackliReader& reader, py::object indices_iterable,
             std::optional<size_t> read_ahead = std::nullopt) {
            EnsureOpen(reader);
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
             EnsureOpen(reader);
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
