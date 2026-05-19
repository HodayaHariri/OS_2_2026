#ifndef REDUCE_CONTEXT_H
#define REDUCE_CONTEXT_H

#include "MapReduceKeys.h"

// Forward-declare to avoid a circular include with JobInternal.h.
struct ThreadContext;

class ReduceContext
{
 private:
  /// Non-owning pointer to the calling thread's context.
  /// Lifetime is guaranteed by the framework to outlast this object.
  ThreadContext* tc_;

 public:
  /**
   * @brief Constructs a ReduceContext bound to the given thread context.
   *
   * Called exclusively by the framework's reduce_phase(); client code never
   * constructs a ReduceContext directly.
   *
   * @param tc  Pointer to the calling thread's ThreadContext.
   *            Must remain valid for the lifetime of this object.
   */
  explicit ReduceContext(ThreadContext* tc);

  void addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value);

    /*
    You can change everything else, including the constructor/desturctor
    You can also add fields here (even public ones)
    */
};

#endif // REDUCE_CONTEXT_H