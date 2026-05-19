#include "ReduceContext.h"
#include "JobInternal.h"

// implement here your constructor and destructor
ReduceContext::ReduceContext(ThreadContext* tc)
    : tc_(tc)
{}

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
  std::unique_lock<std::mutex> lock(tc_->job->emit3_mutex);
  tc_->job->outputVec.emplace_back(std::move(key), std::move(value));
}