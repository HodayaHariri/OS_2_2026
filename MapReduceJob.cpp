#include "MapReduceJob.h"
#include "JobInternal.h"
#include "MapContext.h"
#include "ReduceContext.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>

/*
===============================================
Helpers:
===============================================
*/

static MapReduceStage getStage(uint64_t val)
{
  return static_cast<MapReduceStage>((val >> STAGE_SHIFT) & STAGE_MASK);
}

/*
===============================================
Phase Functions:
===============================================
*/

/**
 * MAP PHASE
 * Identical logic to last year's map_phase().
 * Change: client.map() now takes (shared_ptr<K1>, shared_ptr<V1>, MapContext&)
 *         instead of (K1*, V1*, void*).
 */
static void map_phase(ThreadContext* tc)
{
  {
    std::unique_lock<std::mutex> lock(tc->job->map_mutex);
    if (getStage(tc->job->atomic_index_counter) == UNDEFINED_STAGE) {
      tc->job->atomic_index_counter =
          (uint64_t)MAP_STAGE << STAGE_SHIFT |
          tc->job->atomic_index_counter.load();
    }
  }

  while (true) {
    uint64_t index;
    {
      std::unique_lock<std::mutex> lock(tc->job->map_mutex);
      index = tc->job->atomic_index_counter.load();
      if ((index & TOTAL_MASK) >= tc->job->inputVec.size()) {
        break;
      }
      tc->job->atomic_index_counter++;
    }

    auto [k1, v1] = tc->job->inputVec.at(index & TOTAL_MASK);

    // NEW: wrap thread context in a MapContext and pass by reference.
    MapContext ctx(tc);
    tc->job->client.map(k1, v1, ctx);

    {
      std::unique_lock<std::mutex> lock(tc->job->counter_mutex);
      tc->job->atomic_index_counter += 1ULL << TOTAL_SHIFT;
    }
  }
}

/**
 * SORT PHASE — unchanged from last year.
 * Each thread sorts its own intermediate vector by K2.
 */
static void sort_phase(ThreadContext* tc)
{
  if (!tc->intermediate_vec->empty()) {
    std::sort(tc->intermediate_vec->begin(),
              tc->intermediate_vec->end(),
              [](const IntermediatePair& a, const IntermediatePair& b) {
                  return *(a.first) < *(b.first);
              });
  }
}

/**
 * SHUFFLE PHASE — identical logic to last year's shuffle_phase().
 * No API change needed here: operates on the internal IntermediateVec directly.
 */
static void shuffle_phase(ThreadContext* tc)
{
  {
    std::unique_lock<std::mutex> lock(tc->job->counter_mutex);
    if (getStage(tc->job->atomic_index_counter) != SHUFFLE_STAGE) {
      tc->job->atomic_index_counter.store(
          (uint64_t)SHUFFLE_STAGE << STAGE_SHIFT);
    }
  }

  while (true) {
    // Find the largest key at the back of all sorted vectors.
    K2* max_key = nullptr;
    for (const auto& t : tc->job->contexts) {
      const auto& ivec = t->intermediate_vec;
      if (!ivec->empty()) {
        K2* candidate = ivec->back().first.get(); // .get() — shared_ptr
        if (!max_key || *max_key < *candidate) {
          max_key = candidate;
        }
      }
    }
    if (!max_key) break; // all vectors empty

    auto* group = new IntermediateVec();
    for (auto& t : tc->job->contexts) {
      auto& ivec = t->intermediate_vec;
      while (!ivec->empty()) {
        const IntermediatePair& back = ivec->back();
        // equality via !(a<b) && !(b<a)
        if (!(*back.first < *max_key) && !(*max_key < *back.first)) {
          group->push_back(back);
          ivec->pop_back();
          {
            std::unique_lock<std::mutex> lock(tc->job->counter_mutex);
            tc->job->atomic_index_counter += 1ULL << TOTAL_SHIFT;
          }
        } else {
          break;
        }
      }
    }
    tc->job->shuffled.push_back(group);
  }
}

/**
 * REDUCE PHASE
 * Change: client.reduce() now takes (const IntermediateVec&, ReduceContext&)
 *         instead of (const IntermediateVec*, void*).
 */
static void reduce_phase(ThreadContext* tc)
{
  {
    std::unique_lock<std::mutex> lock(tc->job->counter_mutex);
    if (getStage(tc->job->atomic_index_counter) != REDUCE_STAGE) {
      tc->job->atomic_index_counter.store(
          (uint64_t)REDUCE_STAGE << STAGE_SHIFT);
    }
  }

  while (true) {
    uint64_t index;
    {
      std::unique_lock<std::mutex> lock(tc->job->counter_mutex);
      index = tc->job->atomic_index_counter.load();
      if ((index & TOTAL_MASK) >= tc->job->shuffled.size()) {
        break;
      }
      tc->job->atomic_index_counter++;
    }

    // Each thread works on its own uniquely claimed group — no shared
    // data is touched during reduce(), so no extra mutex is needed here.
    // Removing reduce_mutex allows all threads to run reduce() in parallel,
    // which is the expected behaviour (and required by the barrier test).
    IntermediateVec* cur_vec = tc->job->shuffled.at(index & TOTAL_MASK);

    ReduceContext ctx(tc);
    tc->job->client.reduce(*cur_vec, ctx);

    {
      std::unique_lock<std::mutex> lock(tc->job->counter_mutex);
      tc->job->atomic_index_counter +=
          (uint64_t)cur_vec->size() << TOTAL_SHIFT;
    }
  }
}


/*
===============================================
Worker Thread Entry Point:
===============================================
*/

static void thread_func(ThreadContext* tc)
{
  map_phase(tc);
  sort_phase(tc);
  tc->job->barrier->barrier(); // wait for all threads to finish map+sort

  if (tc->threadID == SHUFFLE_THREAD) {
    shuffle_phase(tc);
  }

  tc->job->barrier->barrier(); // wait for shuffle to finish

  reduce_phase(tc);
}

/*
===============================================
Resource Cleanup:
===============================================
*/

static void release_resources(JobInternal* job)
{
  if (!job) return;
  for (IntermediateVec* vec : job->shuffled) {
    delete vec;
  }
  for (auto* tc : job->contexts) {
    if (tc) {
      delete tc->intermediate_vec;
      delete tc;
    }
  }
  delete job->barrier;
}

/*
===============================================
Implement:
===============================================
*/

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
{
  job_ = nullptr;
  try {
    job_ = new JobInternal(client, inputVec, multiThreadLevel);
    job_->barrier = new Barrier(multiThreadLevel);

    job_->contexts.resize(multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; ++i) {
      job_->contexts[i] = new ThreadContext();
      job_->contexts[i]->threadID        = i;
      job_->contexts[i]->job             = job_;
      job_->contexts[i]->intermediate_vec = new IntermediateVec();
    }
  } catch (const std::exception& e) {
    std::cerr << "system error: MapReduceJob allocation failed" << std::endl;
    release_resources(job_);
    delete job_;
    std::exit(EXIT_FAILURE);
  }

  for (int i = 0; i < multiThreadLevel; ++i) {
    try {
      job_->threads.emplace_back(
          [tc = job_->contexts[i]]() { thread_func(tc); });
    } catch (const std::system_error& e) {
      std::cerr << "system error: thread creation failed" << std::endl;
      std::exit(EXIT_FAILURE);
    }
  }
}

MapReduceState MapReduceJob::getState(void) const
{
  MapReduceState result{UNDEFINED_STAGE, 0.0};
  if (!job_) return result;

  std::unique_lock<std::mutex> lock(job_->counter_mutex);
  uint64_t packed = job_->atomic_index_counter.load();

  auto stage = getStage(packed);
  uint64_t total = (packed >> TOTAL_SHIFT) & TOTAL_MASK;

  double percentage = 0.0;
  if (stage == MAP_STAGE && !job_->inputVec.empty()) {
    percentage = 100.0 * (double)total / (double)job_->inputVec.size();
  } else if ((stage == SHUFFLE_STAGE || stage == REDUCE_STAGE) &&
             job_->total_intermediate_pairs > 0) {
    percentage = 100.0 * (double)total /
                 (double)job_->total_intermediate_pairs.load();
  }

  result.stage      = stage;
  result.percentage = std::min(percentage, 100.0);
  return result;
}

void MapReduceJob::wait(void)
{
  if (!job_) return;
  std::lock_guard<std::mutex> lock(job_->join_mutex);
  if (job_->joined) return;
  for (auto& t : job_->threads) {
    if (t.joinable()) t.join();
  }
  job_->joined = true;
}

OutputVec MapReduceJob::getOutput(void)
{
  wait();
  return job_->outputVec;
}

bool MapReduceJob::isDone(void) const
{
  if (!job_) return true;
  std::lock_guard<std::mutex> lock(job_->join_mutex);
  return job_->joined;
}

MapReduceJob::~MapReduceJob()
{
  if (!job_) return;
  wait();
  release_resources(job_);
  delete job_;
}
