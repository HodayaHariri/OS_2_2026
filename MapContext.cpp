#include "MapContext.h"
#include "JobInternal.h"

MapContext::MapContext(ThreadContext* tc)
    : tc_(tc)
{}


void MapContext::addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value)
{
  std::unique_lock<std::mutex> lock(tc_->job->emit2_mutex);
  tc_->job->total_intermediate_pairs++;
  tc_->intermediate_vec->emplace_back(std::move(key), std::move(value));
}
