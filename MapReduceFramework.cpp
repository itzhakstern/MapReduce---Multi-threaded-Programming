#include "Job.h"

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    return (JobHandle *) new Job(client, inputVec, outputVec, multiThreadLevel);
}

void emit2(K2* key, V2* value, void* context){
    auto temp = (Context*) (context);
    temp->get_job()->emit2PushInterMid(key,value,context);
}

void emit3(K3* key, V3* value, void* context){
    auto temp = (Context*) (context);
    temp->get_job()->emit3PushOut(key,value,context);
}


void waitForJob(JobHandle job){
    ((Job *) job)->waitForThreads();}

void getJobState(JobHandle job, JobState* state){
    *state = ((Job *) job)->currentState(job);
}
void closeJobHandle(JobHandle job){
    delete ((Job*)job);
}
