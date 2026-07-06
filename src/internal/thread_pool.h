// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SACKLI_SRC_INTERNAL_THREAD_POOL_H_
#define SACKLI_SRC_INTERNAL_THREAD_POOL_H_

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <deque>
#include <memory>
#include <thread>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"

namespace sackli::internal {

// A lazily-grown pool of worker threads for running ParallelDo-style batches
// without paying thread creation and teardown on every call.
//
// The calling thread always participates in its own batch, so calls make
// progress even when all workers are busy with other batches, and calls with
// `max_parallelism <= 1` or a single task never touch the pool.
//
// fork() safety: worker threads do not survive fork(). If the process id
// changed since the pool was last used (e.g. a dataloader forked worker
// processes while the pool was idle), the stale pool state is abandoned and
// the pool restarts with zero threads in the child. Forking while a batch is
// in flight is not supported (as with any multithreaded code).
class ThreadPool {
 public:
  // `max_threads` bounds the number of worker threads. Workers are started
  // on demand and kept alive until destruction.
  explicit ThreadPool(int max_threads);
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  // Calls `func` with all task ids in [0, `num_tasks`) concurrently on up to
  // `min(max_parallelism, max_threads + 1)` threads (workers plus the calling
  // thread). Stops handing out tasks after the first failure and returns one
  // of the failed statuses (not necessarily the smallest failing task id).
  // Blocks until every participating thread is done with this batch; `func`
  // is not used after ParallelDo returns.
  absl::Status ParallelDo(size_t num_tasks,
                          absl::FunctionRef<absl::Status(size_t task_id)> func,
                          int max_parallelism);

  // Optional process-wide hook invoked when a thread (worker or caller)
  // starts running tasks of a batch; the returned object is destroyed when
  // that thread is done with the batch. Language bindings use this to pin
  // expensive per-thread state around the many small callbacks a batch makes
  // — e.g. the Python bindings pin a PyThreadState, which is otherwise
  // created and destroyed by every record allocation on non-Python threads
  // (prohibitively expensive on free-threading builds).
  using TaskGuardFactory = std::shared_ptr<void> (*)();
  static void SetTaskGuardFactory(TaskGuardFactory factory);

 private:
  struct Batch;

  // All state shared with worker threads. Heap-allocated so that it can be
  // abandoned wholesale when fork() is detected.
  struct Shared {
    explicit Shared(int max_threads) : max_threads(max_threads) {}

    const int max_threads;
    absl::Mutex mu;
    bool shutdown ABSL_GUARDED_BY(mu) = false;
    int num_idle ABSL_GUARDED_BY(mu) = 0;
    std::vector<std::thread> threads ABSL_GUARDED_BY(mu);
    // Each entry is one requested helper slot for the referenced batch. The
    // batch outlives its entries: ParallelDo removes unclaimed entries and
    // waits for claimed ones before returning.
    std::deque<Batch*> queue ABSL_GUARDED_BY(mu);
    absl::CondVar work_available;
  };

  static void WorkerLoop(Shared* shared);
  static void RunTasks(Batch& batch);

  // Rebuilds `shared_` if this process is a fork of the one that owned the
  // worker threads.
  void HandleForkIfNeeded();

  std::unique_ptr<Shared> shared_;
  std::atomic<pid_t> pid_;
  absl::Mutex fork_mu_;
};

}  // namespace sackli::internal

#endif  // SACKLI_SRC_INTERNAL_THREAD_POOL_H_
