#include "Job.h"
#include <algorithm>
#include <iostream>

Job::Job(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) :
        _atomicCounterMap(0),
        _atomicCounterReduce(0),
        _atomicCounterShuffle(0),
        _counterReduce(0),
        _atomicState(MAP_STAGE),
        _inputVec(&inputVec),
        _outputVec(&outputVec),
        _multiThreadLevel(multiThreadLevel),
        _client(&client),
        _barrier(multiThreadLevel),
        _numIntermediatePair(0),
        _startShuffle(true),
        _startWait(true),
        _threads(nullptr),
        _context(nullptr),
        _shuffleMutex(PTHREAD_MUTEX_INITIALIZER),
        _waitMutex(PTHREAD_MUTEX_INITIALIZER),
        _outputMutex(PTHREAD_MUTEX_INITIALIZER)
{
    _threads = new pthread_t[multiThreadLevel];
    _context = new Context[multiThreadLevel];
    for(int i = 0; i < multiThreadLevel; i++)
    {
        _context[i] = Context(i, this);
    }
    for(int i = 0; i < multiThreadLevel; i++){
        if(pthread_create(_threads + i, nullptr, mapPhase, _context + i) != 0){
            systemError("create threads is failed");
        }
    }
}

Job::~Job() {
    waitForThreads();
    delete [] _context;
    delete [] _threads;
    if(pthread_mutex_destroy(&_shuffleMutex) != 0){
        systemError("destroy mutex is failed");
    }
    if(pthread_mutex_destroy(&_waitMutex) != 0){
        systemError("destroy mutex is failed");
    }
    if(pthread_mutex_destroy(&_outputMutex) != 0){
        systemError("destroy mutex is failed");
    }
}

void Job::emit2PushInterMid(K2* key, V2* value, void* context) {
    _numIntermediatePair++;
    auto temp = (Context*) (context);
    temp->get_IntermediateVec()->push_back(IntermediatePair(key,value));

}

void Job::emit3PushOut(K3* key, V3* value, void* context) {
    auto temp = (Context*) (context);
    if(pthread_mutex_lock(&temp->get_job()->_outputMutex) != 0){
        systemError("mutex lock is failed");
    }
    temp->get_job()->_outputVec->push_back(OutputPair(key,value));
    if(pthread_mutex_unlock(&temp->get_job()->_outputMutex) != 0){
        systemError("mutex unlock is failed");
    }
}

void *Job::mapPhase(void *context) {
    auto temp = (Context*) (context);
    size_t index;
    for (index = (temp->get_job()->_atomicCounterMap)++;index < temp->get_job()->_inputVec->size();
                    index = (temp->get_job()->_atomicCounterMap)++){
        temp->get_job()->_client->map((*temp->get_job()->_inputVec)[index].first,
                            (*temp->get_job()->_inputVec)[index].second,context);
    }
    sort_elements(context);
    temp->get_job()->_barrier.barrier();
    if(pthread_mutex_lock(&temp->get_job()->_shuffleMutex) != 0){
        systemError("mutex lock is failed");
    }
    if(temp->get_job()->_startShuffle){
        temp->get_job()->_startShuffle = false;
        temp->get_job()->_atomicState++;
        shuffle(temp);
    }
    if(pthread_mutex_unlock(&temp->get_job()->_shuffleMutex) != 0){
        systemError("mutex unlock is failed");
    }
    for (size_t index = temp->get_job()->_atomicCounterReduce++; index < temp->get_job()->_intermediateVec.size();
                    index = temp->get_job()->_atomicCounterReduce++){
        temp->get_job()->_counterReduce += temp->get_job()->_intermediateVec[index].size();
        temp->get_job()->_client->reduce(&temp->get_job()->_intermediateVec[index], context);
    }
    return nullptr;
}

void Job::sort_elements(void *context) {
    auto temp = (Context*) (context);
    std::sort(temp->get_IntermediateVec()->begin(),temp->get_IntermediateVec()->end(),comp);
}

bool Job::comp(const IntermediatePair &x, const IntermediatePair &y) {
    return *x.first<(*y.first);
}

void Job::shuffle(Context *context) {
    auto temp = (Context*) (context);
    while (true) {
        IntermediatePair max_pair,curr_pair;
        for (int i = 0; i < temp->get_job()->_multiThreadLevel; i++) {
            if(temp->get_job()->_context[i].get_IntermediateVec()->empty()){continue;}
            curr_pair = temp->get_job()->_context[i].get_IntermediateVec()->back();
            if(!max_pair.first || comp(max_pair,curr_pair)){
                max_pair = curr_pair;
            }
        }
        IntermediateVec temp_vac;
        for (int i = 0; i < temp->get_job()->_multiThreadLevel; i++) {
            if (temp->get_job()->_context[i].get_IntermediateVec()->size() == 0) { continue; }
            while (!(*temp->get_job()->_context[i].get_IntermediateVec()->back().first < *max_pair.first) &&
                    !(*max_pair.first < *temp->get_job()->_context[i].get_IntermediateVec()->back().first )) {
                IntermediatePair pair = temp->get_job()->_context[i].get_IntermediateVec()->back();
                temp->get_job()->_context[i].get_IntermediateVec()->pop_back();
                temp_vac.push_back(pair);
                temp->get_job()->_numIntermediatePair--;
                temp->get_job()->_atomicCounterShuffle++;
                if (temp->get_job()->_context[i].get_IntermediateVec()->size() == 0) { break; }
            }
        }
        if(!temp_vac.empty()){
            temp->get_job()->_intermediateVec.push_back(temp_vac);}
        if(!temp->get_job()->_numIntermediatePair){
            break;
        }
    }
    temp->get_job()->_atomicState++;
}

void Job::waitForThreads() {
    if(pthread_mutex_lock(&_waitMutex)){
        systemError("mutex lock is failed");
    }
    if(_startWait){
        _startWait = false;
        for(int i = 0; i < _multiThreadLevel; i++){
            if(pthread_join(_threads[i], nullptr) != 0){
                systemError("threads join is failed");
            }
        }
    }
    if(pthread_mutex_unlock(&_waitMutex)){
        systemError("mutex unlock is failed");
    }
}

JobState Job::currentState(JobHandle job) {
    auto temp = (Job*) (job);
    float percentage;
    uint64_t stateOfJob = temp->_atomicState;
    if(stateOfJob == MAP_STAGE){
        if(temp->_inputVec->size() == 0){percentage = 100;}
        else{
            percentage = (float) temp->_atomicCounterMap / temp->_inputVec->size();}
    }if(stateOfJob == SHUFFLE_STAGE){
        if(temp->_numIntermediatePair == 0){percentage = 100;}
        else{
            percentage =(float ) temp->_atomicCounterShuffle / temp->_numIntermediatePair;}
    }if(stateOfJob == REDUCE_STAGE){
        if(temp->_atomicCounterShuffle == 0){percentage = 100;}
        else{
            percentage = (float) temp->_counterReduce / temp->_atomicCounterShuffle;}
    }
    JobState state;
    state.percentage = std::min<float>(percentage * 100 ,100);
    state.stage = (stage_t) stateOfJob;
    return state;
}

void Job::systemError(const std::string& msg) {
    std::cerr << SYSTEM_ERROR_MESSAGE << msg << std::endl;
    exit(EXIT_FAILURE);
}




