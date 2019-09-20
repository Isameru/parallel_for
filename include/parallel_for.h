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
// Create a single, globally accessible instance of this class and
// use For() member function to distribute workload among workers.
//
class ThreadHive
{
public:
    using Handler = std::function<void(int, int)>;

private:
    struct Job
    {
        Handler* handler;
        int start;
        int count;
        std::atomic<int>* decreaseWhenDone;
    };

    enum class WorkerState
    {
        Idle,
        Busy,
        Sleeping,
        Awoke,
        Quitting
    };

    struct Worker
    {
        std::thread thread;
        std::atomic<WorkerState> state;
    };

    struct Bed
    {
        std::condition_variable condVar;
        std::mutex mutex;
    };

    using Clock = std::chrono::high_resolution_clock;
    constexpr static int MaxWorkerCount = 63;

    alignas(64) std::atomic<int> workerCount;
                std::atomic<uint64_t> workerAvailabilityMask = {0};
    alignas(64) Job jobs[MaxWorkerCount] = {};
    alignas(64) Worker workers[MaxWorkerCount];
    alignas(64) Bed beds[MaxWorkerCount];

public:
    Clock::duration SpinLockDuration = std::chrono::microseconds{50};

    ThreadHive() :
        ThreadHive(ProposeWorkerCount())
    {}

    ThreadHive(int workerCount_) :
        workerCount{FixWorkerCount(workerCount_)}
    {
        for (int workerIdx = 0; workerIdx < workerCount; ++workerIdx)
        {
            StartWorker(workerIdx);
        }

        ++workerCount;
    }

    ~ThreadHive()
    {
        --workerCount;

        for (int workerIdx = 0; workerIdx < workerCount; ++workerIdx)
        {
            // TODO: This is incorrect.
            workers[workerIdx].state = WorkerState::Quitting;
            beds[workerIdx].condVar.notify_one();
        }

        for (int workerIdx = 0; workerIdx < workerCount; ++workerIdx)
        {
            workers[workerIdx].thread.join();
        }
    }

    void For(int start, int exclusiveEnd, int minSpan, int maxSpan, Handler handler)
    {
        assert(minSpan > 0);
        assert(minSpan <= maxSpan);

        --workerCount;

        std::atomic<int> busyWorkerCount = {0};

        while (start < exclusiveEnd)
        {
            int spanLeft = exclusiveEnd - start;
            if (spanLeft < 2 * minSpan)
            {
                // Do all the work on itself.
                //
                handler(start, exclusiveEnd);
                break;
            }

            auto exchangedWorkerAvailabilityMask = workerAvailabilityMask.exchange(0);

            auto hireMask = exchangedWorkerAvailabilityMask & (WorkerMask(workerCount + 1) - 1);

            if (!hireMask)
            {
                // There are no idle workers. Do the minimal portion of work on itself and check again.
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

        ++workerCount;

        while (busyWorkerCount) {}
    }

private:
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

    static int ProposeWorkerCount()
    {
        int vpuCount = static_cast<int>(std::thread::hardware_concurrency());
        return vpuCount * 3 / 4;
    }

    static int FixWorkerCount(int workerCount)
    {
        workerCount = std::max(workerCount, 1);
        workerCount = std::min(workerCount, 63);
        return workerCount;
    }

    void StartWorker(int workerIdx)
    {
        auto& worker = workers[workerIdx];
        worker.state = WorkerState::Idle;
        worker.thread = std::thread([this, workerIdx](){
            RunWorker(workerIdx);
        });
        std::atomic_fetch_or(&workerAvailabilityMask, WorkerMask(workerIdx));
    }

    void ScheduleWorker(int workerIdx, Handler& handler, int start, int count, std::atomic<int>& decreaseWhenDone)
    {
        auto& worker = workers[workerIdx];
        auto& job = jobs[workerIdx];

        job.handler = &handler;
        job.start = start;
        job.count = count;
        job.decreaseWhenDone = &decreaseWhenDone;

        ++decreaseWhenDone;
        const auto lastWorkerState = worker.state.exchange(WorkerState::Busy);
        if (lastWorkerState == WorkerState::Sleeping)
        {
            auto& bed = beds[workerIdx];

            do {
                bed.condVar.notify_one();
            }
            while (worker.state != WorkerState::Awoke);

            worker.state = WorkerState::Busy;
        }
        else
        {
            assert(lastWorkerState == WorkerState::Idle);
        }
    }

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
                if (Clock::now() - lastBusyTime > SpinLockDuration)
                {
                    auto expState = WorkerState::Idle;
                    if (worker.state.compare_exchange_weak(expState, WorkerState::Sleeping))
                    {
                        std::unique_lock<std::mutex> lock(bed.mutex);
                        bed.condVar.wait(lock, [&worker]() {
                            return worker.state != WorkerState::Sleeping;
                        });

                        expState = WorkerState::Busy;
                        if (worker.state.compare_exchange_weak(expState, WorkerState::Awoke))
                        {
                            while (worker.state == WorkerState::Awoke) {}
                        }
                        else
                        {
                            assert(worker.state == WorkerState::Quitting);
                        }
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
