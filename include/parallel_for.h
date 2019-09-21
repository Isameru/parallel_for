// MIT License
//
// Copyright (c) 2019 Mariusz £apiñski
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// TODO: Consider a handler throwing an exception.

#pragma once

#include <mutex>
#include <atomic>
#include <chrono>
#include <thread>
#include <cassert>
#include <iostream>
#include <algorithm>
#include <functional>
#include <condition_variable>

#ifdef _WIN32
#include <intrin.h>
#endif

// A pool of worker threads.
// You may create a single, globally accessible instance of this class and
// use For() member function to distribute workload among workers.
//
class ThreadHive
{
public:
    // A loop handler called by the For() member function to process loop iterations.
    // The first parameter is the starting index of the loop range, where the second
    // is the exclusive end (one beyond the last element/iteration index).
    //
    using Handler = std::function<void(int, int)>;

private:
    // A job defines the task for the worker.
    // Every worker has its own Job slot occupying a single cache line.
    // It is filled up by the scheduler thread, then read by the worker, which executes the loop handler.
    //
    struct Job
    {
        Handler* handler;                       // The loop handler to be called
        int start;                              // Starting index of the loop range
        int count;                              // Iteration count in the loop range
        std::atomic<int>* decreaseWhenDone;     // It is decremented by one by the worker after the work is done
    };

    // An atomic state of the worker, which is used to synchronize the scheduler threads with worker threads.
    //
    enum class WorkerState
    {
        Idle,           // The worker is available, aggressively spinning waiting for a job
        Busy,           // The worker is occupied with the assigned job
        Sleeping,       // The worker is available, sleeping on a mutex
        Awoke,          // The worker has been awoken from a sleep and expects new state
        Quitting        // The worker shall quit upon nearest occasion
    };

    // The worker thread and its state.
    // The atomic worker state is meant to occupy a single cache line.
    //
    struct Worker
    {
        std::thread thread;
        std::atomic<WorkerState> state;
    };

    // The structure used by the worker for sleeping/waking up.
    // Every worker has its own Bed slot.
    // Due to large paddings of condition_variable and mutex, the Bed structure is separated from the Worker structure.
    //
    struct Bed
    {
        std::condition_variable condVar;
        std::mutex mutex;
    };

    using Clock = std::chrono::high_resolution_clock;
    constexpr static int MaxWorkerCount = 63;

    const int workerCount;                                              // Number of worker threads started
    alignas(64) std::atomic<int> effectiveWorkerCount;
                std::atomic<uint64_t> workerAvailabilityMask = {0};
    alignas(64) Job jobs[MaxWorkerCount] = {};
    alignas(64) Worker workers[MaxWorkerCount];
    alignas(64) Bed beds[MaxWorkerCount];

public:
    // How long the worker will aggressively remain idle on a spin lock before putting itself to sleep on a mutex.
    //
    Clock::duration SpinLockDuration = std::chrono::microseconds{50};

    ThreadHive() :
        ThreadHive(ProposeWorkerCount())
    {}

    ThreadHive(int workerCount_) :
        workerCount{FixWorkerCount(workerCount_)}
    {
        effectiveWorkerCount = workerCount + 1;

        for (int workerIdx = 0; workerIdx < workerCount; ++workerIdx) {
            StartWorker(workerIdx);
        }
    }

    ~ThreadHive()
    {
        assert(effectiveWorkerCount == workerCount + 1);

        for (int workerIdx = 0; workerIdx < workerCount; ++workerIdx) {
            ActivateWorker(workerIdx, WorkerState::Quitting);
        }

        for (int workerIdx = 0; workerIdx < workerCount; ++workerIdx) {
            workers[workerIdx].thread.join();
        }
    }

    // Gets the number of workers created for this hive.
    //
    int WorkerCount() const noexcept
    {
        return workerCount;
    }

    // Parallel for-loop.
    // Invokes the specified handler with ranges covering the specified range.
    // The caller thread is the scheduler for available workers.
    // The scheduler thread also invokes the handler, but more frequently (for smaller ranges).
    //
    void For(int start, int exclusiveEnd, int minSpan, int maxSpan, Handler handler)
    {
        assert(minSpan > 0);
        assert(minSpan <= maxSpan);

        --effectiveWorkerCount;

        std::atomic<int> busyWorkerCount = {0};

        while (start < exclusiveEnd)
        {
            int spanLeft = exclusiveEnd - start;
            if (spanLeft < 2 * minSpan)
            {
                // Do all the work itself.
                //
                handler(start, exclusiveEnd);
                break;
            }

            auto exchangedWorkerAvailabilityMask = workerAvailabilityMask.exchange(0);

            auto hireMask = exchangedWorkerAvailabilityMask & (WorkerMask(effectiveWorkerCount + 1) - 1);

            if (!hireMask)
            {
                // There are no idle workers. Do the minimal portion of work itself and check again.
                //
                auto localEnd = std::min(start + minSpan, exclusiveEnd);
                handler(start, localEnd);
                start = localEnd;
                continue;
            }

            int workerIdx = First1BitIndex(hireMask);
            auto workerMask = WorkerMask(workerIdx);
            workerAvailabilityMask.fetch_or(exchangedWorkerAvailabilityMask ^ workerMask);

            auto availableWorkerCount = CountBits(hireMask);
            int spanPerWorker = std::min(std::max(spanLeft / (availableWorkerCount + 1), minSpan), maxSpan);

            auto localEnd = std::min(start + spanPerWorker, exclusiveEnd);
            ScheduleWorker(workerIdx, handler, start, localEnd - start, busyWorkerCount);
            start = localEnd;
        }

        ++effectiveWorkerCount;

        while (busyWorkerCount) {}
    }

private:
    // Returns the bit mask of the specified worker.
    //
    static uint64_t WorkerMask(int workerIdx)
    {
        return 1ull << workerIdx;
    }

    // Counts the number of 1-bits.
    //
    static int CountBits(uint64_t v)
    {
#if defined(GCC)
        return __builtin_popcountll(v);
#elif defined(_WIN32)
        return static_cast<int>(__popcnt64(v));
#else
        int c = 0;
        while (v)
        {
            c += static_cast<int>(v & 1);
            v >>= 1;
        }
        return c;
#endif
    }

    // Finds the first 1-bit starting from LSB and returns its index.
    //
    static int First1BitIndex(uint64_t v)
    {
        assert(v);
#if defined(GCC)
        return __builtin_ctzll(v);
#elif defined(_WIN32)
        unsigned long res;
        _BitScanForward64(&res, v);
        return static_cast<int>(res);
#else
        int c = 0;
        while (!(v & 1))
        {
            v >>= 1;
            ++c;
        }
        return c;
#endif
    }

    // Guess the desired number of worker threads.
    //
    static int ProposeWorkerCount()
    {
        int vpuCount = static_cast<int>(std::thread::hardware_concurrency());
        return vpuCount * 3 / 4;
    }

    // Clamp the specified number of desired worker threads to values 1...63.
    //
    static int FixWorkerCount(int workerCount)
    {
        workerCount = std::max(workerCount, 1);
        workerCount = std::min(workerCount, 63);
        return workerCount;
    }

    // Creates a new idle worker thread.
    //
    void StartWorker(int workerIdx)
    {
        auto& worker    = workers[workerIdx];
        worker.state    = WorkerState::Idle;
        worker.thread   = std::thread([this, workerIdx]() {
            RunWorker(workerIdx);
        });
        std::atomic_fetch_or(&workerAvailabilityMask, WorkerMask(workerIdx));
    }

    // Puts the specified worker to work.
    //
    void ScheduleWorker(int workerIdx, Handler& handler, int start, int count, std::atomic<int>& decreaseWhenDone)
    {
        auto& worker            = workers[workerIdx];
        auto& job               = jobs[workerIdx];

        job.handler             = &handler;
        job.start               = start;
        job.count               = count;
        job.decreaseWhenDone    = &decreaseWhenDone;

        ++decreaseWhenDone;

        ActivateWorker(workerIdx, WorkerState::Busy);
    }

    // Switches the worker state to either Busy of Quitting, assuming is it currently Idle or Sleeping.
    //
    void ActivateWorker(int workerIdx, WorkerState newState)
    {
        assert(newState == WorkerState::Busy || newState == WorkerState::Quitting);

        auto& worker = workers[workerIdx];

        const auto lastWorkerState = worker.state.exchange(newState);

        if (lastWorkerState == WorkerState::Sleeping) {
            AwakeWorker(workerIdx, newState);
        }
        else {
            assert(lastWorkerState == WorkerState::Idle);
        }
    }

    // Awakes the specified worker thread, assuming it is currently sleeping on a mutex.
    //
    void AwakeWorker(int workerIdx, WorkerState newState)
    {
        auto& worker    = workers[workerIdx];
        auto& bed       = beds[workerIdx];

        do {
            bed.condVar.notify_one();
        }
        while (worker.state != WorkerState::Awoke);

        worker.state = newState;
    }

    // The main body of a worker thread.
    //
    void RunWorker(int workerIdx)
    {
        auto& worker            = workers[workerIdx];
        const auto workerMask   = WorkerMask(workerIdx);
        auto& job               = jobs[workerIdx];
        auto& bed               = beds[workerIdx];

        auto lastBusyTime = Clock::now();

        while (worker.state != WorkerState::Quitting)
        {
            while (worker.state == WorkerState::Idle)
            {
                if (effectiveWorkerCount < workerIdx ||
                    Clock::now() - lastBusyTime > SpinLockDuration)
                {
                    auto expState = WorkerState::Idle;
                    if (worker.state.compare_exchange_weak(expState, WorkerState::Sleeping))
                    {
                        std::unique_lock<std::mutex> lock(bed.mutex);
                        bed.condVar.wait(lock, [&worker]() {
                            return worker.state != WorkerState::Sleeping;
                        });

                        worker.state = WorkerState::Awoke;
                        while (worker.state == WorkerState::Awoke) {}
                    }
                }
            }

            if (worker.state == WorkerState::Busy)
            {
                (*job.handler)(job.start, job.start + job.count);
                --(*job.decreaseWhenDone);

                worker.state = WorkerState::Idle;
                std::atomic_fetch_or(&workerAvailabilityMask, workerMask);
                lastBusyTime = Clock::now();
            }
        }
    }
};
