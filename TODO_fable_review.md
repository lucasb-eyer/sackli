# Code review findings (API elegance & speed)

Review of the sackli codebase focused on API elegance and speed, 2026-07-05.
Findings marked **[verified]** were reproduced empirically against the PyPI
wheel; benchmark numbers are from a page-cached local file (worst case for
threading overhead).

Already fixed (see git log): `index(value, start, stop)` treating stop as a
length; negative indexing in `__getitem__`/`read_indices`; per-record
decompressor-pool round-trips; allocation churn in the POSIX no-cache read
paths; assorted dead-code/README nits.

## Speed

### 1. Default `max_parallelism=100` + thread-spawn-per-call: 30x slowdown on warm data **[verified]**
`internal::ParallelDo` (`src/internal/parallel_do.cc:63`) creates and joins
fresh `std::thread`s on every `read_indices`/`read_range` call.

Measured, 256 random indices per call (typical dataloader batch), cached
uncompressed bag:

| max_parallelism | ms/batch of 256 |
|---|---|
| 1   | 0.049 |
| 8   | 0.81  |
| 100 | 1.46  |

- Fix: persistent executor/thread pool owned by `SackliReader::State`; cap
  workers more aggressively relative to batch size.
- Also: the `cpu_bound` parameter is accepted and ignored
  (`[[maybe_unused]]`) — 100 threads end up doing zstd decompression on an
  8-core box.
- Residual from the fixed allocation-churn item: once threads are pooled,
  per-call read buffers in `PosixBufferedFdPReadFile` /
  `PosixDirectPReadFile` can become pooled/thread-local too, eliminating the
  remaining one-malloc-per-PRead.

### 2. Sequential reads never parallelize within a shard **[verified]**
A contiguous range maps to one `ShardRange`, so `ReadShardRanges` runs
`ParallelDo(1, ...)` — `read()` and iteration of a single-shard compressed bag
are strictly single-threaded, including decompression.

- Verified: `read()` of a 100k-record `.bagz` takes 0.132 s at both
  `max_parallelism=1` and `100`.
- Fix: split large `ShardRange`s into ~`max_parallelism` sub-ranges in
  `State::ReadRangeWithAllocator`. Benefits `read()`, iteration read-ahead,
  and `Index`/`MultiIndex` construction, which all funnel through this path.

### 3. Per-record GIL acquisition eats parallelism on GIL builds **[verified]**
`IndexedAllocator`/`MakeBytes` grab the GIL once per record to allocate
`py::bytes`.

- Verified: full random permutation via `read_indices` with 100 threads takes
  0.147 s vs 0.132 s for single-threaded sequential `read()` — workers queue
  on the GIL.
- On free-threading builds the current design is right (zero-copy into the
  bytes object).
- Options: allocate one arena/`bytearray` per batch under a single GIL
  acquisition; or read into C++ buffers and convert per-batch; or just
  document that `max_parallelism > ~8` only pays off on nogil / I/O-bound.

### 4. `MultiIndex::Create` materializes the whole dataset
`src/sackli_multi_index.cc:32` reads everything into a `vector<string>` before
moving into the hash map — transient ~2x dataset RAM. Streaming in range
batches via `ReadRangeWithAllocator` would halve peak memory (matters for a
keys bag with billions of entries).

## API elegance

### 5. Kwargs on the constructor
`sackli.Reader(path, sackli.Reader.Options(cache_policy=...))` is the most
common line users will write. Accept
`sackli.Reader(path, cache_policy=..., access_pattern=...)` directly (keep
`Options` for compat). Accepting strings for enums
(`cache_policy="drop_after_read"`) would compound the win —
`sackli.CachePolicy.DROP_AFTER_READ` is 34 characters of ceremony.

### 6. No `close()` / context manager on `Reader`
Readers hold fds and mmaps until GC; a sharded reader over `@1000` files pins
1000+ fds (2000+ with the separate tail-limits handle), invisibly. Slices
share `State` via `shared_ptr` so `close()` has aliasing questions, but even a
documented "last reference closes" plus an explicit `close()` that invalidates
the family beats nothing.

### 7. Error mapping **[verified]**
Everything that isn't NotFound/OutOfRange becomes `ValueError` — verified a
permission error surfaces as `ValueError: PERMISSION_DENIED: ...`. Map
`PermissionDenied → PermissionError` and I/O-ish codes → `OSError` in
`ThrowNonOkStatusAsException`.

### 8. `read_ahead_bytes` not exposed to Python
Exists in C++ `Options` but absent from the bindings and `.pyi` — yet it's the
knob controlling iterator batch sizing. Expose it or drop it from the C++
options struct.

### 9. `reversed(data)` returns a `Reader`, not an iterator **[verified]**
Violates the `__reversed__` protocol (`next()` fails). The test asserts this
deliberately, and a lazily-sliced Reader is arguably more useful — but the
protocol says iterator. Cheap fix: return an iterator; the sliced Reader is
one expression away for those who want it.

## Suggested order of attack

1. **#1 + #2 together** (persistent executor, then sub-range splitting on top
   of it): they touch the same code (`ParallelDo` / `ReadShardRanges`), and
   together they fix both ends of the perf story — batched random access at
   the default settings and sequential/full-file throughput. Benchmark-driven.
2. **#5** (constructor kwargs + string enums): biggest ergonomic win, small
   isolated binding change.
3. Then #3 (GIL batching) with benchmarks on both GIL and nogil builds.
