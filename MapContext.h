#ifndef MAP_CONTEXT_H
#define MAP_CONTEXT_H

#include "MapReduceKeys.h"
// you can add other includes as you wish

// Forward-declare to avoid a circular include with JobInternal.h.
struct ThreadContext;

class MapContext
{
 private:
  // Non-owning pointer to the calling thread's context.
  // Lifetime is guaranteed by the framework to outlast this object.
  ThreadContext* tc_;
public:
    /*
    You must keep and implement this function:
    */
    void addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value);

    /*
    You can change everything else, including the constructor/desturctor
    You can also add fields here (even public ones)
    */

  /**
  * @brief Constructs a MapContext bound to the given thread context.
  *
  * Called exclusively by the framework's map_phase(); client code never
  * constructs a MapContext directly.
  *
  * @param tc  Pointer to the calling thread's ThreadContext.
  *            Must remain valid for the lifetime of this object.
  */
  explicit MapContext(ThreadContext* tc);

};

#endif // MAP_CONTEXT_H