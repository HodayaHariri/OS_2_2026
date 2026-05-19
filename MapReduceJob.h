#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include "MapReduceClient.h"
// you can add other includes here

struct JobInternal;

enum MapReduceStage
{
	UNDEFINED_STAGE, // 0
	MAP_STAGE, // 1
	SHUFFLE_STAGE, // 2
	REDUCE_STAGE // 3
};

class MapReduceState
{
public:
	MapReduceStage stage;
	double percentage;

	inline bool operator==(const MapReduceState &other) const
	{
		return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
	}

	inline bool operator!=(const MapReduceState &other) const
	{
		return !(*this == other);
	}
};

class MapReduceJob
{
public:
	/*
	You CAN NOT change or add properties to this part (public API).
	*/

	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	~MapReduceJob();

	MapReduceState getState(void) const;

	bool isDone(void) const;
	
	void wait(void);

	OutputVec getOutput(void);

private:
	/*
		You can change everything on this part (these are just recommendations)
	*/
  JobInternal* job_;
};
	
#endif // MAP_REDUCE_JOB_H
