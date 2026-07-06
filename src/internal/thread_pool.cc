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

#include "src/internal/thread_pool.h"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <deque>
#include <limits>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"

namespace sackli::internal {

namespace {
std::atomic<ThreadPool::TaskGuardFactory> task_guard_factory{nullptr};
}  // namespace

void ThreadPool::SetTaskGuardFactory(TaskGuardFactory factory) {
  task_guard_factory.store(factory, std::memory_order_release);
}

struct ThreadPool::Batch {
  Batch(size_t num_tasks, absl::FunctionRef<absl::Status(size_t)> func)
      : num_tasks(num_tasks), func(func) {}

  const size_t num_tasks;
  absl::FunctionRef<absl::Status(size_t)> func;
  std::atomic<size_t> next_task{0};
  std::atomic<bool> failed{false};
  absl::Mutex mu;
  int pending_helpers ABSL_GUARDED_BY(mu) = 0;
  absl::Status status ABSL_GUARDED_BY(mu);
};

ThreadPool::ThreadPool(int max_threads)
    : shared_(std::make_unique<Shared>(std::max(max_threads, 0))),
      pid_(getpid()) {}

ThreadPool::~ThreadPool() {
  if (pid_.load(std::memory_order_relaxed) != getpid()) {
    // Forked child: the worker threads do not exist in this process, so the
    // std::thread handles must not be joined or destroyed. Leak instead.
    (void)shared_.release();
    return;
  }
  shared_->mu.Lock();
  shared_->shutdown = true;
  shared_->work_available.SignalAll();
  std::vector<std::thread> threads = std::move(shared_->threads);
  shared_->mu.Unlock();
  for (std::thread& thread : threads) {
    thread.join();
  }
}

void ThreadPool::RunTasks(Batch& batch) {
  std::shared_ptr<void> guard;
  if (TaskGuardFactory factory =
          task_guard_factory.load(std::memory_order_acquire);
      factory != nullptr) {
    guard = factory();
  }
  size_t task_id;
  while (!batch.failed.load(std::memory_order_relaxed) &&
         (task_id = batch.next_task.fetch_add(1, std::memory_order_relaxed)) <
             batch.num_tasks) {
    absl::Status status = batch.func(task_id);
    if (!status.ok()) {
      absl::MutexLock lock(&batch.mu);
      if (!batch.failed.exchange(true, std::memory_order_relaxed)) {
        batch.status = std::move(status);
      }
    }
  }
}

void ThreadPool::WorkerLoop(Shared* shared) {
  shared->mu.Lock();
  ++shared->num_idle;
  for (;;) {
    while (shared->queue.empty() && !shared->shutdown) {
      shared->work_available.Wait(&shared->mu);
    }
    if (shared->shutdown) {
      --shared->num_idle;
      shared->mu.Unlock();
      return;
    }
    Batch* batch = shared->queue.front();
    shared->queue.pop_front();
    --shared->num_idle;
    shared->mu.Unlock();

    RunTasks(*batch);
    {
      absl::MutexLock lock(&batch->mu);
      --batch->pending_helpers;
    }

    shared->mu.Lock();
    ++shared->num_idle;
  }
}

void ThreadPool::HandleForkIfNeeded() {
  if (pid_.load(std::memory_order_relaxed) == getpid()) {
    return;
  }
  absl::MutexLock lock(&fork_mu_);
  if (pid_.load(std::memory_order_relaxed) == getpid()) {
    return;
  }
  // This process is a forked child: the worker threads did not survive the
  // fork, and the inherited Shared block (with its std::thread handles and
  // possibly inconsistent mutex) must not be touched. Leak it and start over.
  const int max_threads = shared_->max_threads;
  (void)shared_.release();
  shared_ = std::make_unique<Shared>(max_threads);
  pid_.store(getpid(), std::memory_order_relaxed);
}

absl::Status ThreadPool::ParallelDo(
    size_t num_tasks, absl::FunctionRef<absl::Status(size_t)> func,
    int max_parallelism) {
  if (max_parallelism < 0) {
    return absl::InvalidArgumentError("max_parallelism must be non-negative");
  }
  // Every participant may overshoot `next_task` by one; keep the counter from
  // wrapping for absurdly large `num_tasks`.
  const size_t max_participants = static_cast<size_t>(max_parallelism) + 1;
  if (num_tasks > std::numeric_limits<size_t>::max() - max_participants) {
    num_tasks = std::numeric_limits<size_t>::max() - max_participants;
  }

  Batch batch(num_tasks, func);
  size_t max_helpers =
      std::min(num_tasks > 0 ? num_tasks - 1 : 0,
               max_parallelism > 0 ? static_cast<size_t>(max_parallelism) - 1
                                   : 0);

  if (max_helpers > 0) {
    HandleForkIfNeeded();
    Shared& shared = *shared_;
    absl::MutexLock lock(&shared.mu);
    max_helpers =
        std::min(max_helpers, static_cast<size_t>(shared.max_threads));
    if (max_helpers > 0) {
      const size_t capacity_left =
          static_cast<size_t>(shared.max_threads) - shared.threads.size();
      const size_t idle = static_cast<size_t>(shared.num_idle);
      const size_t to_spawn = std::min(
          capacity_left, max_helpers > idle ? max_helpers - idle : 0);
      for (size_t i = 0; i < to_spawn; ++i) {
        shared.threads.emplace_back(&ThreadPool::WorkerLoop, &shared);
      }
      {
        absl::MutexLock batch_lock(&batch.mu);
        batch.pending_helpers = static_cast<int>(max_helpers);
      }
      for (size_t i = 0; i < max_helpers; ++i) {
        shared.queue.push_back(&batch);
      }
      shared.work_available.SignalAll();
    }
  }

  RunTasks(batch);

  if (max_helpers > 0) {
    // Reclaim helper slots no worker picked up, then wait for the ones that
    // did; `batch` and `func` must outlive all of them.
    Shared& shared = *shared_;
    {
      absl::MutexLock lock(&shared.mu);
      const size_t removed = std::erase(shared.queue, &batch);
      if (removed > 0) {
        absl::MutexLock batch_lock(&batch.mu);
        batch.pending_helpers -= static_cast<int>(removed);
      }
    }
    batch.mu.LockWhen(absl::Condition(
        +[](int* pending) { return *pending == 0; }, &batch.pending_helpers));
    batch.mu.Unlock();
  }

  absl::MutexLock lock(&batch.mu);
  return batch.status;
}

}  // namespace sackli::internal
