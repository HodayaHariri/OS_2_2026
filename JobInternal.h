#ifndef JOB_INTERNAL_H
#define JOB_INTERNAL_H

#include "MapReduceClient.h"
#include "MapReduceKeys.h"
#include "Barrier.h"

#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
#include <iostream>

// packed-counter bit layout
//
//  A single uint64_t encodes the full job progress without extra locking:
//
//   63        62        61 .... 31        30 .... 0
//  [ stage (2b) ][ processed/total (31b) ][ index (31b) ]
//
//  bits 63-62  stage   — current pipeline stage (MapReduceStage enum, 2 bits)
//  bits 61-31  total   — number of items completed in the current stage (31 bits)
//  bits 30- 0  index   — next work-item index to claim (31 bits)
//
//  The counter is read and written under the appropriate mutex; the atomic
//  type ensures torn reads are impossible on x86-64.

/// Bit position of the stage field inside the packed counter.
#define STAGE_SHIFT   62
/// Bit position of the "processed" field inside the packed counter.
#define TOTAL_SHIFT   31
/// Mask for extracting the lower 31-bit index field.
#define PROCESS_MASK  0x7FFFFFFF
/// Mask for extracting the 31-bit processed/total field.
#define TOTAL_MASK    0x7FFFFFFF
/// Mask for extracting the 2-bit stage field.
#define STAGE_MASK    0x3

/// Thread ID of the thread that is responsible for the Shuffle phase.
#define SHUFFLE_THREAD 0

struct JobInternal; // forward declaration needed by ThreadContext

// ─────────────────────────────────────────────────────────────────────────────

/**
 * @struct ThreadContext
 * @brief  Holds all per-thread state needed during the Map→Sort→Reduce phases.
 *
 * The framework allocates one ThreadContext per worker thread before any
 * thread is spawned.  Each context is freed by release_resources() after
 * all threads have joined.
 *
 * ThreadContext replaces the old "void* context" pattern: instead of casting
 * an opaque pointer, every phase function receives a typed ThreadContext*.
 */
struct ThreadContext {
    // Zero-based index of this thread (0 … multiThreadLevel-1).
    // Thread 0 is also the dedicated Shuffle thread (SHUFFLE_THREAD).
    int threadID = 0;

    // Non-owning pointer back to the shared job state.
    // Valid for the entire lifetime of the worker thread.
    JobInternal* job = nullptr;

    /**
     * Per-thread intermediate vector.
     * During the Map phase each thread appends its own (K2,V2) pairs here
     * via MapContext::addIntermediate().  After the Map phase the vector is
     * sorted in-place (Sort phase) and then drained by the Shuffle phase.
     *
     * Ownership: allocated by the framework before thread creation;
     * freed by release_resources() after all threads join.
     */
    IntermediateVec* intermediate_vec = nullptr;
};


/**
 * @struct JobInternal
 * @brief  Central state object shared by all worker threads for one
 *         MapReduce job.
 *
 * JobInternal is an implementation detail of MapReduceJob; it is never
 * exposed to client code.  MapReduceJob holds a single owning pointer to a
 * JobInternal and is responsible for creating and destroying it.
 *
 * The struct bundles together:
 *  - references to the client-supplied data (client, inputVec)
 *  - the output vector that accumulates Reduce results
 *  - per-thread contexts and std::thread handles
 *  - the shuffled intermediate groups produced by the Shuffle phase
 *  - a single packed atomic counter that tracks stage and progress
 *  - a suite of mutexes that protect each shared resource
 *  - a Barrier used to synchronise the transition between pipeline stages
 */
struct JobInternal {

    // client data (read-only after construction)

    // Reference to the user-supplied MapReduceClient (map + reduce logic).
    const MapReduceClient& client;

    // Reference to the user-supplied input vector.  Not modified by the job.
    const InputVec& inputVec;

    // Output vector populated by ReduceContext::addOutput() during the
    // Reduce phase.  Returned to the caller via MapReduceJob::getOutput().
    OutputVec outputVec;

    // threading

    // Number of worker threads requested by the caller.
    int multiThreadLevel;

    // One ThreadContext per worker thread, indexed by threadID.
    std::vector<ThreadContext*> contexts;

    // std::thread handles for all worker threads.
    std::vector<std::thread> threads;

    /**
     * Groups of intermediate pairs produced by the Shuffle phase.
     * Each element is an IntermediateVec* containing all (K2,V2) pairs that
     * share the same K2 key.  The Reduce phase claims groups from this vector
     * one at a time (protected by counter_mutex).
     *
     * Ownership: each IntermediateVec* is allocated by shuffle_phase() and
     * freed by release_resources().
     */
    std::vector<IntermediateVec*> shuffled;

    // atomic progress counter

    /**
     * Packed 64-bit counter encoding stage, processed count, and work index.
     * See the bit-layout diagram at the top of this file.
     * Must be read/written under the appropriate mutex to avoid mixed reads
     * across non-atomic compound operations (increment + stage change).
     */
    std::atomic<uint64_t> atomic_index_counter{0};

    /**
     * Total number of (K2,V2) pairs emitted by all map() calls combined.
     * Incremented inside MapContext::addIntermediate() (under emit2_mutex).
     * Used as the denominator when computing Shuffle/Reduce percentage.
     */
    std::atomic<int> total_intermediate_pairs{0};

    // synchronisation

    /**
     * Reusable barrier used twice per job:
     *  1. After the Map+Sort phases — all threads wait before Shuffle starts.
     *  2. After the Shuffle phase  — all threads wait before Reduce starts.
     * Allocated in MapReduceJob's constructor; freed by release_resources().
     */
    Barrier* barrier = nullptr;

    /// True once all worker threads have been joined.  Protected by join_mutex.
    bool joined = false;

    /// Protects the joined flag and the join loop in MapReduceJob::wait().
    std::mutex join_mutex;

    /// Protects the stage-transition check and work-item claim in map_phase().
    std::mutex map_mutex;

    /// Protects appending to a thread's intermediate_vec and incrementing
    /// total_intermediate_pairs inside MapContext::addIntermediate().
    std::mutex emit2_mutex;

    /// Protects appending to the shared outputVec inside
    /// ReduceContext::addOutput().
    std::mutex emit3_mutex;

    /// General-purpose mutex protecting atomic_index_counter for compound
    /// read-modify-write operations (stage transitions, progress updates).
    std::mutex counter_mutex;

    // constructor / destructor

    /**
     * @brief Constructs a JobInternal with the given client and input data.
     *
     * Does not allocate per-thread resources or spawn threads; that is done
     * by MapReduceJob's constructor after this object is fully initialised.
     *
     * @param client           The MapReduce client providing map/reduce logic.
     * @param inputVec         The input pairs to process.
     * @param multiThreadLevel Number of worker threads to use.
     */
    JobInternal(const MapReduceClient& client,
                const InputVec&        inputVec,
                int                    multiThreadLevel)
        : client(client)
        , inputVec(inputVec)
        , multiThreadLevel(multiThreadLevel)
    {}

    /// Default destructor — actual resource cleanup is performed by
    /// release_resources() before this destructor runs.
    ~JobInternal() = default;
};

#endif // JOB_INTERNAL_H