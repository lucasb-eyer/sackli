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

from collections.abc import Callable, Iterable, Iterator, Sequence
import concurrent.futures
import itertools
import os
from pathlib import Path
import sysconfig
import threading
from typing import TypeAlias

import numpy as np
import pytest

import sackli


_NUM_RECORDS = 20
_STRESS_RECORDS = 257
_THREAD_TEST_TIMEOUT_SECONDS = 120

Compression: TypeAlias = (
    sackli.CompressionAutoDetect | sackli.CompressionNone | sackli.CompressionZstd
)

_NAMES = ('data.bagz', 'data.bag')
_OPTIONAL_LIMITS_PLACEMENTS = (
    None,
    sackli.LimitsPlacement.TAIL,
    sackli.LimitsPlacement.SEPARATE,
)
_LIMITS_PLACEMENTS = (
    sackli.LimitsPlacement.TAIL,
    sackli.LimitsPlacement.SEPARATE,
)
_OPTIONAL_COMPRESSIONS = (
    None,
    sackli.CompressionAutoDetect(),
    sackli.CompressionNone(),
    sackli.CompressionZstd(level=3),
)
_OPTIONAL_LIMITS_STORAGES = (
    None,
    sackli.LimitsStorage.IN_MEMORY,
    sackli.LimitsStorage.ON_DISK,
)
_LIMITS_STORAGES = (
    sackli.LimitsStorage.IN_MEMORY,
    sackli.LimitsStorage.ON_DISK,
)
_ACCESS_PATTERNS = (
    sackli.AccessPattern.SYSTEM,
    sackli.AccessPattern.RANDOM,
    sackli.AccessPattern.SEQUENTIAL,
)
_CACHE_POLICIES = (
    sackli.CachePolicy.SYSTEM,
    sackli.CachePolicy.DROP_AFTER_READ,
)
_DIRECT_IO_UNSUPPORTED_MARKERS = (
    'Direct I/O',
    'O_DIRECT',
    'STATX_DIOALIGN',
)


def _generate_record(row: int) -> bytes:
  if (row + 2) % 4 == 0:
    return b''
  return f'Record_{row:03d}'.encode()


def _generate_records(num_records: int) -> Iterator[bytes]:
  return (_generate_record(i) for i in range(num_records))


def _all_slices(num_records: int) -> Iterator[slice]:
  for start in range(0, num_records):
    for stop in range(start + 1, num_records + 1):
      for step in range(1, 4):
        yield slice(start, stop, step)
        yield slice(stop - 1, start - 1 if start > 0 else None, -step)


def _is_free_threading_build() -> bool:
  return bool(sysconfig.get_config_var('Py_GIL_DISABLED'))


def _thread_worker_count() -> int:
  cpu_count = os.cpu_count() or 1
  if _is_free_threading_build():
    return max(4, min(16, cpu_count * 2))
  return max(4, min(8, cpu_count))


def _thread_iterations() -> int:
  return 250 if _is_free_threading_build() else 100


def _generate_stress_record(row: int) -> bytes:
  if row % 17 == 0:
    return b''
  return (
      f'Record_{row:05d}:'.encode() + bytes([row % 251]) * (32 + row % 257)
  )


def _generate_stress_records(num_records: int) -> list[bytes]:
  return [_generate_stress_record(i) for i in range(num_records)]


def _write_records(path: Path, records: Sequence[bytes]) -> None:
  with sackli.Writer(path) as writer:
    for record in records:
      writer.write(record)


def _run_in_threads(num_workers: int, worker: Callable[[int], None]) -> None:
  with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
    futures = [pool.submit(worker, worker_id) for worker_id in range(num_workers)]
    for future in futures:
      future.result(timeout=_THREAD_TEST_TIMEOUT_SECONDS)


def _filtered_options(**kwargs: object) -> dict[str, object]:
  return {k: v for k, v in kwargs.items() if v is not None}


_WRITE_READ_CASES = tuple(
    itertools.product(
        _NAMES,
        _OPTIONAL_LIMITS_PLACEMENTS,
        _OPTIONAL_COMPRESSIONS,
        _OPTIONAL_LIMITS_STORAGES,
    )
)

_WRITE_WITHOUT_CONTEXT_CASES = tuple(
    itertools.product(_NAMES, _OPTIONAL_LIMITS_PLACEMENTS, _OPTIONAL_COMPRESSIONS)
)

_READER_POLICY_CASES = tuple(
    itertools.product(
        _NAMES,
        _LIMITS_PLACEMENTS,
        _LIMITS_STORAGES,
        _ACCESS_PATTERNS,
        _CACHE_POLICIES,
    )
)

_SHARED_RANDOM_ACCESS_CASES = tuple(itertools.product(_NAMES, _CACHE_POLICIES))


def _assert_reader_matches_records(
    reader: sackli.Reader, records: Sequence[bytes]
) -> None:
  check_indices = [0, len(records) // 2, len(records) - 1]
  for index in check_indices:
    assert reader[index] == records[index], f'row: {index}'
    assert reader[index] == records[index], f'row: {index}'

  start = 11
  count = 9
  assert reader.read_range(start, count) == records[start : start + count]

  indices = [7, 2, 7, len(records) - 3]
  result = reader.read_indices(indices)
  assert result == [records[index] for index in indices]
  assert result[0] is result[2]


def _is_direct_io_unsupported(exc: ValueError) -> bool:
  return any(marker in str(exc) for marker in _DIRECT_IO_UNSUPPORTED_MARKERS)


def _open_direct_io_reader_or_skip(
    file: Path, **options: object
) -> sackli.Reader:
  try:
    return sackli.Reader(
        file,
        sackli.Reader.Options(
            cache_policy=sackli.CachePolicy.DIRECT_IO, **options
        ),
    )
  except ValueError as exc:
    if _is_direct_io_unsupported(exc):
      pytest.skip(str(exc))
    raise


def test_approximate_bytes_per_record(tmp_path: Path) -> None:
  file = tmp_path / 'data.bag'
  records = list(_generate_records(_NUM_RECORDS))
  num_bytes = 0
  with sackli.Writer(file) as writer:
    for record in records:
      num_bytes += len(record)
      writer.write(record)
  reader = sackli.Reader(file)
  assert list(reader) == records
  assert file.stat().st_size == num_bytes + len(records) * 8
  assert int(reader.approximate_bytes_per_record() * len(records) + 0.5) == num_bytes


@pytest.mark.parametrize(
    ('name', 'limits_placement', 'compression', 'limits_storage'),
    _WRITE_READ_CASES,
)
def test_write_read(
    tmp_path: Path,
    name: str,
    limits_placement: sackli.LimitsPlacement | None,
    compression: Compression | None,
    limits_storage: sackli.LimitsStorage | None,
) -> None:
  file = tmp_path / name
  records = list(_generate_records(_NUM_RECORDS))
  writer_options = _filtered_options(
      limits_placement=limits_placement,
      compression=compression,
  )
  with sackli.Writer(file, sackli.Writer.Options(**writer_options)) as writer:
    for record in records:
      writer.write(record)

  reader_options = _filtered_options(
      **writer_options, limits_storage=limits_storage
  )
  reader = sackli.Reader(file, sackli.Reader.Options(**reader_options))

  all_indices = np.arange(len(records), dtype=np.int64)

  for i in range(_NUM_RECORDS):
    assert reader[i] == records[i], f'row: {i}'
    assert reader[all_indices[i]] == records[i], f'row: {i}'
  with pytest.raises(IndexError, match='25'):
    _ = reader[25]

  assert len(reader) == _NUM_RECORDS
  assert list(reader) == records

  a, b, c = reader.read_indices([3, 3, 3])
  assert a == _generate_record(3)
  assert a is b
  assert b is c
  with pytest.raises(IndexError, match='25'):
    reader.read_indices([3, 25, 3])

  a, b, c = reader.read_indices(np.array([3, 4, 3], dtype=np.int64))
  assert a == _generate_record(3)
  assert b == _generate_record(4)
  assert c is a

  a, b, c = reader.read_indices(np.array([5, 4, 3], dtype=np.int32))
  assert a == _generate_record(5)
  assert b == _generate_record(4)
  assert c == _generate_record(3)

  a, b, c = reader.read_indices(slice(2, None, -1))
  assert a == _generate_record(2)
  assert b == _generate_record(1)
  assert c == _generate_record(0)

  a, b, c = reader.read_indices(slice(2, 5))
  assert a == _generate_record(2)
  assert b == _generate_record(3)
  assert c == _generate_record(4)

  a, b, c = reader.read_range(5, 3)
  assert a == _generate_record(5)
  assert b == _generate_record(6)
  assert c == _generate_record(7)
  with pytest.raises(IndexError, match='25'):
    reader.read_range(25, 10)

  a, b, c = (*reader.read_range_iter(5, 3),)
  assert a == _generate_record(5)
  assert b == _generate_record(6)
  assert c == _generate_record(7)
  with pytest.raises(IndexError, match='25'):
    reader.read_range_iter(25, 10)

  index = sackli.Index(reader)
  num_empty = 5
  assert len(index) == len(records) - num_empty + 1
  for i, record in enumerate(records):
    assert record in index
    assert index[record] == (i if record else 2)

  assert index.get('25') is None
  with pytest.raises(KeyError, match='25'):
    _ = index['25']

  multi_index = sackli.MultiIndex(reader)
  expected_empty = [2, 6, 10, 14, 18]
  assert len(multi_index) == len(records) - len(expected_empty) + 1
  for i, record in enumerate(records):
    assert record in multi_index
    expected_index = [i] if record else expected_empty
    assert multi_index[record] == expected_index
    assert multi_index.get(record) == expected_index

  assert multi_index.get('25') is None
  obj = object()
  assert multi_index.get('25', default=obj) is obj
  with pytest.raises(KeyError, match='25'):
    _ = multi_index['25']


def test_pathlib(tmp_path: Path) -> None:
  root = Path('/posix:' + os.fspath(tmp_path))
  file = root / 'data.bagz'
  records = list(_generate_records(_NUM_RECORDS))
  with sackli.Writer(file, sackli.Writer.Options()) as writer:
    for record in records:
      writer.write(record)
  reader = sackli.Reader(file, sackli.Reader.Options())
  assert list(reader) == records


def test_file_not_found() -> None:
  with pytest.raises(FileNotFoundError, match='not_a_file.bagz'):
    sackli.Reader('not_a_file.bagz')


@pytest.mark.parametrize(
    ('name', 'limits_placement', 'compression'),
    _WRITE_WITHOUT_CONTEXT_CASES,
)
def test_write_without_context_read(
    tmp_path: Path,
    name: str,
    limits_placement: sackli.LimitsPlacement | None,
    compression: Compression | None,
) -> None:
  file = tmp_path / name
  records = list(_generate_records(_NUM_RECORDS))
  options = _filtered_options(
      limits_placement=limits_placement,
      compression=compression,
  )
  writer = sackli.Writer(file, sackli.Writer.Options(**options))
  for record in records:
    writer.write(record)
  writer.flush()
  writer.close()
  reader = sackli.Reader(file, sackli.Reader.Options(**options))
  assert list(reader) == records


def test_read_all(tmp_path: Path) -> None:
  file = tmp_path / 'data.bagz'
  records = list(_generate_records(_NUM_RECORDS))
  with sackli.Writer(file, sackli.Writer.Options()) as writer:
    for record in records:
      writer.write(record)
  reader = sackli.Reader(file, sackli.Reader.Options())
  assert reader.read() == records


@pytest.mark.parametrize(
    ('name', 'limits_placement', 'limits_storage', 'access_pattern', 'cache_policy'),
    _READER_POLICY_CASES,
)
def test_reader_access_and_cache_policies(
    tmp_path: Path,
    name: str,
    limits_placement: sackli.LimitsPlacement,
    limits_storage: sackli.LimitsStorage,
    access_pattern: sackli.AccessPattern,
    cache_policy: sackli.CachePolicy,
) -> None:
  file = tmp_path / name
  records = _generate_stress_records(_STRESS_RECORDS)
  with sackli.Writer(
      file, sackli.Writer.Options(limits_placement=limits_placement)
  ) as writer:
    for record in records:
      writer.write(record)

  reader = sackli.Reader(
      file,
      sackli.Reader.Options(
          limits_placement=limits_placement,
          limits_storage=limits_storage,
          access_pattern=access_pattern,
          cache_policy=cache_policy,
      ),
  )
  _assert_reader_matches_records(reader, records)


@pytest.mark.parametrize(
    ('name', 'limits_placement', 'limits_storage', 'access_pattern'),
    itertools.product(
        _NAMES,
        _LIMITS_PLACEMENTS,
        _LIMITS_STORAGES,
        _ACCESS_PATTERNS,
    ),
)
def test_reader_direct_io_policies_if_supported(
    tmp_path: Path,
    name: str,
    limits_placement: sackli.LimitsPlacement,
    limits_storage: sackli.LimitsStorage,
    access_pattern: sackli.AccessPattern,
) -> None:
  file = tmp_path / name
  records = _generate_stress_records(_STRESS_RECORDS)
  with sackli.Writer(
      file, sackli.Writer.Options(limits_placement=limits_placement)
  ) as writer:
    for record in records:
      writer.write(record)

  reader = _open_direct_io_reader_or_skip(
      file,
      limits_placement=limits_placement,
      limits_storage=limits_storage,
      access_pattern=access_pattern,
  )
  _assert_reader_matches_records(reader, records)


@pytest.mark.parametrize('name', _NAMES)
def test_reader_direct_io_if_supported(tmp_path: Path, name: str) -> None:
  file = tmp_path / name
  records = _generate_stress_records(_STRESS_RECORDS)
  _write_records(file, records)

  reader = _open_direct_io_reader_or_skip(file)
  _assert_reader_matches_records(reader, records)


def test_sequence_methods(tmp_path: Path) -> None:
  file = tmp_path / 'data.bagz'
  records = list(_generate_records(_NUM_RECORDS))
  with sackli.Writer(file, sackli.Writer.Options()) as writer:
    for record in records:
      writer.write(record)

  reader = sackli.Reader(file, sackli.Reader.Options())

  assert reader.index(records[0]) == 0
  assert reader.index(records[11]) == 11
  assert reader.index(b'', start=3) == 6

  with pytest.raises(ValueError):
    _ = reader.index(b'Missing')

  with pytest.raises(ValueError):
    _ = reader.index(records[11], stop=10)

  assert reader.count(records[0]) == 1
  assert reader.count(records[11]) == 1
  assert reader.count(b'') == 5
  assert reader.count(b'Missing') == 0

  assert records[0] in reader
  assert records[11] in reader
  assert b'' in reader
  assert b'Missing' not in reader

  reversed_reader = reversed(reader)
  assert isinstance(reversed_reader, sackli.Reader)
  assert list(reversed_reader) == list(reader)[::-1]


def test_slice_reader(tmp_path: Path) -> None:
  file = tmp_path / 'data.bagz'
  records = list(_generate_records(_NUM_RECORDS))
  with sackli.Writer(file, sackli.Writer.Options()) as writer:
    for record in records:
      writer.write(record)

  reader = sackli.Reader(file, sackli.Reader.Options())
  assert isinstance(reader, Sequence)
  all_values = reader.read_range(0, _NUM_RECORDS)

  for current_slice in _all_slices(_NUM_RECORDS):
    reader_slice = reader[current_slice]
    assert isinstance(reader_slice, sackli.Reader)
    assert list(reader_slice) == all_values[current_slice], f'slice: {current_slice}'
    assert reader_slice.read_indices(range(len(reader_slice))) == all_values[
        current_slice
    ], f'slice: {current_slice}'
    assert reader_slice.read_range(0, len(reader_slice)) == all_values[
        current_slice
    ], f'slice: {current_slice}'

  with pytest.raises(IndexError, match='Invalid slice'):
    _ = reader.read_indices(slice('Not a number'))

  with pytest.raises(IndexError, match='Invalid slice'):
    _ = reader[slice('Not a number')]


def test_iterator(tmp_path: Path) -> None:
  file = tmp_path / 'data.bagz'
  records = list(_generate_records(_NUM_RECORDS))
  with sackli.Writer(file) as writer:
    for record in records:
      writer.write(record)
  all_indices = np.arange(len(records))
  np.random.default_rng(42).shuffle(all_indices)
  reader = sackli.Reader(file, sackli.Reader.Options())
  for row, (index, record_index_iter, record_from_index) in enumerate(
      zip(
          all_indices,
          reader.read_indices_iter(iter(all_indices)),
          reader.read_indices(all_indices),
          strict=True,
      )
  ):
    expected_record = _generate_record(index)
    assert record_index_iter == expected_record, f'row: {row}'
    assert record_from_index == records[index], f'row: {row}'


@pytest.mark.parametrize('read_ahead', [None, 3, 4, 5])
def test_iterator_with_exception(tmp_path: Path, read_ahead: int | None) -> None:
  file = tmp_path / 'data.bagz'
  records = list(_generate_records(_NUM_RECORDS))
  with sackli.Writer(file) as writer:
    for record in records:
      writer.write(record)
  reader = sackli.Reader(file)

  class NotImplementedIterator(Iterable[int]):

    def __iter__(self):
      raise NotImplementedError()

  with pytest.raises(NotImplementedError):
    reader.read_indices_iter(NotImplementedIterator(), read_ahead=read_ahead)

  with pytest.raises(IndexError, match=f'{len(records)}'):
    _ = list(
        reader.read_indices_iter([3, 2, 3, len(records), 10], read_ahead=read_ahead)
    )

  def raise_at_4(i: int) -> int:
    if i == 4:
      raise ValueError('test')
    return i

  item_iter = reader.read_indices_iter(
      map(raise_at_4, range(10)), read_ahead=read_ahead
  )
  for _ in range(4):
    next(item_iter)

  with pytest.raises(ValueError, match='test'):
    next(item_iter)

  def bad_type_at_4(i: int) -> int | str:
    if i == 4:
      return 'test'
    return i

  item_iter = reader.read_indices_iter(
      map(bad_type_at_4, range(10)), read_ahead=read_ahead
  )

  for _ in range(4):
    next(item_iter)

  with pytest.raises(TypeError):
    next(item_iter)

  def overflow_at_4(i: int) -> int:
    if i == 4:
      return 2**64
    return i

  item_iter = reader.read_indices_iter(
      map(overflow_at_4, range(10)), read_ahead=read_ahead
  )
  for _ in range(4):
    next(item_iter)

  with pytest.raises(OverflowError):
    next(item_iter)

  item_iter = reader.read_indices_iter(
      map(overflow_at_4, range(10)), read_ahead=read_ahead
  )
  for _ in range(4):
    next(item_iter)
  del item_iter


@pytest.mark.parametrize(('name', 'cache_policy'), _SHARED_RANDOM_ACCESS_CASES)
def test_shared_reader_random_access_threadsafe(
    tmp_path: Path,
    name: str,
    cache_policy: sackli.CachePolicy,
) -> None:
  file = tmp_path / name
  records = _generate_stress_records(_STRESS_RECORDS)
  _write_records(file, records)
  reader = sackli.Reader(
      file,
      sackli.Reader.Options(
          access_pattern=sackli.AccessPattern.RANDOM,
          cache_policy=cache_policy,
      ),
  )
  num_workers = _thread_worker_count()
  barrier = threading.Barrier(num_workers)

  def worker(worker_id: int) -> None:
    rng = np.random.default_rng(worker_id)
    barrier.wait()
    for _ in range(_thread_iterations()):
      index = int(rng.integers(len(records)))
      assert reader[index] == records[index]
      assert reader[index] == records[index]

  _run_in_threads(num_workers, worker)


@pytest.mark.parametrize('name', _NAMES)
def test_shared_reader_direct_io_random_access_threadsafe(
    tmp_path: Path, name: str
) -> None:
  file = tmp_path / name
  records = _generate_stress_records(_STRESS_RECORDS)
  _write_records(file, records)
  reader = _open_direct_io_reader_or_skip(
      file, access_pattern=sackli.AccessPattern.RANDOM
  )
  num_workers = _thread_worker_count()
  barrier = threading.Barrier(num_workers)

  def worker(worker_id: int) -> None:
    rng = np.random.default_rng(worker_id)
    barrier.wait()
    for _ in range(_thread_iterations()):
      index = int(rng.integers(len(records)))
      assert reader[index] == records[index]
      assert reader[index] == records[index]

  _run_in_threads(num_workers, worker)


@pytest.mark.parametrize('name', _NAMES)
def test_shared_reader_in_memory_limits_first_touch_threadsafe(
    tmp_path: Path, name: str
) -> None:
  file = tmp_path / name
  records = _generate_stress_records(_STRESS_RECORDS)
  _write_records(file, records)
  reader = sackli.Reader(
      file,
      sackli.Reader.Options(limits_storage=sackli.LimitsStorage.IN_MEMORY),
  )
  num_workers = _thread_worker_count()
  barrier = threading.Barrier(num_workers)

  def worker(worker_id: int) -> None:
    rng = np.random.default_rng(1000 + worker_id)
    barrier.wait()
    index = worker_id % len(records)
    assert reader[index] == records[index]
    for _ in range(max(1, _thread_iterations() // 4)):
      index = int(rng.integers(len(records)))
      assert reader[index] == records[index]

  _run_in_threads(num_workers, worker)


@pytest.mark.parametrize('name', _NAMES)
def test_parallel_open_and_read_threadsafe(tmp_path: Path, name: str) -> None:
  file = tmp_path / name
  records = _generate_stress_records(_STRESS_RECORDS)
  _write_records(file, records)
  num_workers = _thread_worker_count()
  barrier = threading.Barrier(num_workers)

  def worker(worker_id: int) -> None:
    rng = np.random.default_rng(2000 + worker_id)
    barrier.wait()
    reader = sackli.Reader(file)
    for _ in range(_thread_iterations()):
      index = int(rng.integers(len(records)))
      assert reader[index] == records[index]

  _run_in_threads(num_workers, worker)


@pytest.mark.parametrize('max_parallelism', [1, 4])
def test_batch_reads_with_outer_threading(
    tmp_path: Path, max_parallelism: int
) -> None:
  file = tmp_path / 'data.bagz'
  records = _generate_stress_records(_STRESS_RECORDS)
  _write_records(file, records)
  reader = sackli.Reader(
      file, sackli.Reader.Options(max_parallelism=max_parallelism)
  )
  num_workers = min(_thread_worker_count(), 8)
  barrier = threading.Barrier(num_workers)
  iterations = max(25, _thread_iterations() // 4)

  def worker(worker_id: int) -> None:
    rng = np.random.default_rng(3000 + worker_id)
    barrier.wait()
    for _ in range(iterations):
      start = int(rng.integers(0, len(records) - 8))
      count = int(rng.integers(1, 9))
      assert reader.read_range(start, count) == records[start : start + count]

      indices = [int(rng.integers(len(records))) for _ in range(4)]
      indices[2] = indices[0]
      result = reader.read_indices(indices)
      assert result == [records[index] for index in indices]
      assert result[0] is result[2]

  _run_in_threads(num_workers, worker)


def test_shared_reader_mixed_valid_and_invalid_indices_threadsafe(
    tmp_path: Path,
) -> None:
  file = tmp_path / 'data.bagz'
  records = _generate_stress_records(_STRESS_RECORDS)
  _write_records(file, records)
  reader = sackli.Reader(file)
  num_workers = _thread_worker_count()
  barrier = threading.Barrier(num_workers)

  def worker(worker_id: int) -> None:
    rng = np.random.default_rng(4000 + worker_id)
    barrier.wait()
    for _ in range(_thread_iterations()):
      if int(rng.integers(2)) == 0:
        index = int(rng.integers(len(records)))
        assert reader[index] == records[index]
        continue

      index = len(records) + int(rng.integers(1, 33))
      case = int(rng.integers(3))
      if case == 0:
        with pytest.raises(IndexError):
          _ = reader[index]
      elif case == 1:
        with pytest.raises(IndexError):
          reader.read_indices([0, index, 1])
      else:
        with pytest.raises(IndexError):
          reader.read_range(index, 1)

  _run_in_threads(num_workers, worker)
