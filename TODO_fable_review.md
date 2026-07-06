# Code review findings (API elegance & speed)

Review of the sackli codebase focused on API elegance and speed, 2026-07-05.

Everything from the original review is now fixed except the one item below;
see git log for the individual changes: index(start, stop) semantics,
negative indexing, decompressor reuse, POSIX read-path allocation churn,
persistent thread pool, PyThreadState pinning, shared-py::list contention,
contiguous-read splitting, streamed Index/MultiIndex construction, the
pybind11 type-lookup patch (global dispatch mutex), constructor kwargs +
string enums, status -> Python exception mapping, read_ahead_bytes exposure,
__reversed__ protocol, Reader.close() / context manager, and assorted nits.

Headline numbers (100k records, page-cached, 8 cores, free-threading build):
compressed single-shard `read()` 0.13s -> 0.033s, full-permutation
`read_indices` 7.2s -> 0.046s, iteration 0.23s -> 0.036s, `data[i]` from 192
threads 0.47 -> 1.98 M rec/s (flat scaling instead of collapse).

## Network-filesystem validation (2026-07-06)

Re-validated against a real remote server (~105 ms RTT, heavy jitter) via NFS
and SSHFS, new build vs pre-fork PyPI wheel, nogil, interleaved A/B runs with
medians (single uncontrolled runs were badly biased by server-cache warmth —
do not trust non-interleaved numbers on shared links):

- Cold / latency-bound random reads (DROP_AFTER_READ): parity within noise,
  before and after raising the server's parallel connection limit — at this
  RTT the wire dominates. No regressions.
- Warm client page cache (SYSTEM policy, epoch cached locally): local
  findings reproduce exactly on NFS-backed files — `data[i]` from 192
  threads: new 1.40 M rec/s vs old 0.17 M (8x), scaling flat vs collapsing.
- Takeaway: the wins materialize whenever the bottleneck isn't the wire —
  cached epochs, fast parallel filesystems, local disk.

Bench data left at /mnt/fbi-nfs/sackli_bench/data.bag (100 MB); bench
scripts in ~/.claude/jobs/71448c86/tmp/netbench*.py.

## Remaining

### 1. GIL builds: parallel reads still serialize on per-record allocation **[verified]**
On GIL-enabled Python, every record allocation acquires the GIL from a worker
thread; with many workers on warm data this is slower than a single thread,
which is why contiguous-read splitting is disabled there (see
`SackliReader::SetCallbackConcurrencyHint`). The real fix is an arena
strategy for GIL builds: workers write records into plain C++ scratch buffers
(no GIL), and the results are converted to `py::bytes` under a single GIL
acquisition per task stint (one extra memcpy per record). The task-guard hook
in `internal::ThreadPool` is the natural attachment point. Free-threading
builds should keep the current zero-copy path. Only worth doing if GIL-build
throughput matters — GIL Python is a compatibility target here.

- Ruled out (measured 2026-07-06): thread_local scratch buffers in the POSIX
  no-cache read paths — throughput unchanged within noise at 32-192 reader
  threads; the allocator's per-thread cache already makes the malloc/free
  pair negligible next to the two syscalls per read.

## Upstream

- The pybind11 global-internals-mutex-per-dispatch finding
  (cmake/patches/pybind11-cache-type-lookup.patch) affects every pybind
  module on free-threading Python and is present through v3.0.4 and master;
  worth filing/PRing upstream.
