# Code review findings (API elegance & speed)

Review of the sackli codebase focused on API elegance and speed, 2026-07-05.
Findings marked **[verified]** were reproduced empirically; benchmark numbers
are from a page-cached local file on 8 cores.

Already fixed (see git log): `index(value, start, stop)` treating stop as a
length; negative indexing; per-record decompressor-pool round-trips;
allocation churn in the POSIX no-cache read paths; persistent thread pool
(replacing thread-spawn-per-call); per-task-stint PyThreadState pinning;
shared-py::list contention fix; contiguous-read splitting (free-threading
builds); streamed Index/MultiIndex construction; assorted
dead-code/README nits.

Headline numbers after the fixes (100k records, page-cached, 8 cores,
free-threading build): compressed single-shard `read()` 0.13s -> 0.033s,
full-permutation `read_indices` 7.2s -> 0.046s, iteration 0.23s -> 0.036s.
GIL builds are at parity or better with the old wheel everywhere.

## Network-filesystem validation (2026-07-06)

Re-validated the big perf changes against a real remote server (~105 ms RTT,
heavy jitter) via NFS and SSHFS, new build vs pre-fork PyPI wheel, nogil,
interleaved A/B runs with medians (single uncontrolled runs were badly biased
by server-cache warmth — do not trust non-interleaved numbers on shared
links):

- Cold / latency-bound random reads (DROP_AFTER_READ): parity within noise
  (NFS medians ~170 vs ~150 rec/s; SSHFS same picture). Throughput was capped
  by the transport (~16 effective concurrent RPCs ≈ server nfsd threads), so
  the CPU-side changes neither help nor hurt. No regressions.
- Warm client page cache (SYSTEM policy, epoch cached locally): local
  findings reproduce exactly on NFS-backed files — `data[i]` from 192
  threads: new 1.40 M rec/s vs old 0.17 M (8x), and new *scales up* with
  threads while old collapses (pybind dispatch lock).
- Takeaway: the wins materialize whenever the bottleneck isn't the wire —
  cached epochs, fast parallel filesystems, local disk. On a
  latency-dominated link, server/transport tuning (nfsd threads, nconnect)
  is what matters.

Bench data left at /mnt/fbi-nfs/sackli_bench/data.bag (100 MB).

## Speed

### 1. GIL builds: parallel reads still serialize on per-record allocation **[verified]**
On GIL-enabled Python, every record allocation acquires the GIL from a worker
thread; with many workers on warm data this is slower than a single thread,
which is why contiguous-read splitting is disabled there (see
`SackliReader::SetCallbackConcurrencyHint`). The real fix is an arena
strategy for GIL builds: workers write records into plain C++ scratch buffers
(no GIL), and the results are converted to `py::bytes` under a single GIL
acquisition per task stint (one extra memcpy per record). The task-guard hook
in `internal::ThreadPool` is the natural attachment point. Free-threading
builds should keep the current zero-copy path.

- Ruled out (measured 2026-07-06): thread_local scratch buffers in
  `PosixBufferedFdPReadFile` / `PosixDirectPReadFile` to remove the remaining
  one-malloc-per-PRead. Prototyped and hammered with 32–192 reader threads on
  nogil (tmpfs DROP_AFTER_READ and NVMe DIRECT_IO): throughput unchanged
  within noise — the allocator's per-thread cache already makes the
  malloc/free pair negligible next to the two syscalls per read. Not worth
  the extra invariant (PRead callbacks must not re-enter PRead on a thread).

## API elegance

### 2. Kwargs on the constructor
`sackli.Reader(path, sackli.Reader.Options(cache_policy=...))` is the most
common line users will write. Accept
`sackli.Reader(path, cache_policy=..., access_pattern=...)` directly (keep
`Options` for compat). Accepting strings for enums
(`cache_policy="drop_after_read"`) would compound the win —
`sackli.CachePolicy.DROP_AFTER_READ` is 34 characters of ceremony.

### 3. No `close()` / context manager on `Reader`
Readers hold fds and mmaps until GC; a sharded reader over `@1000` files pins
1000+ fds (2000+ with the separate tail-limits handle), invisibly. Slices
share `State` via `shared_ptr` so `close()` has aliasing questions, but even a
documented "last reference closes" plus an explicit `close()` that invalidates
the family beats nothing.

### 4. Error mapping **[verified]**
Everything that isn't NotFound/OutOfRange becomes `ValueError` — verified a
permission error surfaces as `ValueError: PERMISSION_DENIED: ...`. Map
`PermissionDenied → PermissionError` and I/O-ish codes → `OSError` in
`ThrowNonOkStatusAsException`.

### 5. `read_ahead_bytes` not exposed to Python
Exists in C++ `Options` but absent from the bindings and `.pyi` — yet it's the
knob controlling iterator batch sizing. Expose it or drop it from the C++
options struct.

### 6. `reversed(data)` returns a `Reader`, not an iterator **[verified]**
Violates the `__reversed__` protocol (`next()` fails). The test asserts this
deliberately, and a lazily-sliced Reader is arguably more useful — but the
protocol says iterator. Cheap fix: return an iterator; the sliced Reader is
one expression away for those who want it.

## Suggested order of attack

1. **#2** (constructor kwargs + string enums): biggest ergonomic win, small
   isolated binding change.
2. **#1** (GIL arena batching) if GIL-build throughput matters at all; it
   also unlocks re-enabling contiguous-read splitting there.
3. The remaining API papercuts (#3–#6) as a batch.
