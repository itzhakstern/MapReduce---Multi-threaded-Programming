#ifndef MAPREDUCE_EX3_JOB_H
#define MAPREDUCE_EX3_JOB_H

#include "MapReduceFramework.h"
#include "pthread.h"
#include <atomic>
#include "Barrier.h"
#include <semaphore.h>
#include <string>

#define SYSTEM_ERROR_MESSAGE "system error: "
/**
 * the class of the all job
 */
class Job;
/**
 * a class for each thread
 */
class Context{
private:
    /**
     * thread id
     */
    int idThread;
    /**
     * the intermediate for each vector
     */
    IntermediateVec privateIntermidVec;
    /**
     * the pointer for the job
     */
    Job* thisJob;

public:
    /**
     * the constructor of the class
     * @param idThread the id of the thread
     * @param job the pointer for the thread
     */
    Context(int idThread,  Job* job) :idThread(idThread), privateIntermidVec(0), thisJob(job) {}
    /**
     * the default constructor
     */
    Context():idThread(0),privateIntermidVec(0),thisJob(nullptr){}
    /**
     * operatot =
     * @param other the other context to be copy
     * @return a pointer to this
     */
    Context& operator=(const Context& other){
        idThread = other.idThread;
        privateIntermidVec = other.privateIntermidVec;
        thisJob = other.thisJob;
        return *this;
    }
    /**
     * geter for the pointer for the job
     * @return
     */
    Job* get_job(){return thisJob;}
    /**
     * geter fot the intermediate vector
     * @return
     */
    IntermediateVec* get_IntermediateVec(){
        return &privateIntermidVec;
    }
};



/**
 *
 */
class Job{
private:
    /**
     * the mutex for the output process
     */
    pthread_mutex_t _outputMutex;
    /**
     * the mutex for the shuffle process
     */
    pthread_mutex_t _shuffleMutex;
    /**
     * the mutex for the wait process
     */
    pthread_mutex_t _waitMutex;
    /**
     * pointer for the threads
     */
    pthread_t* _threads;
    /**
     * a atomic value for count the percentage of the map phase
     */
    std::atomic<uint64_t> _atomicCounterMap;
    /**
     * a atomic value for count the percentage of the Reduce phase
     */
    std::atomic<uint64_t> _atomicCounterReduce;
    /**
     * a atomic value for count the percentage of the Reduce phase
     */
    std::atomic<uint64_t> _counterReduce;
    /**
     * a atomic value for count the percentage of the Intermediate pairs phase
     */
    std::atomic<uint64_t> _numIntermediatePair;
    /**
     * a atomic value for count the percentage of the shuffle phase
     */
    std::atomic<uint64_t> _atomicCounterShuffle;
    /**
     * a atomic value for state
     */
    std::atomic<int> _atomicState;
    /**
     * a pointer to the input vector
     */
    const InputVec *_inputVec;
    /**
     * a pointer to the output vector
     */
    OutputVec* _outputVec;
    /**
     * the number of threads in the process
     */
    int _multiThreadLevel;
    /**
     * a pointer to the client object
     */
    const MapReduceClient* _client;
    /**
     * a barrier to make sure all the threads wait intel the sort phase pass
     */
    Barrier _barrier;
    /**
     * a boolean that give as the sign that one thread start the shuffle phase
     */
    bool _startShuffle;
    /**
     * a boolean that give as the sign that we did pthread_join or not
     */
    bool _startWait;
    /**
     * a vector of all the intermediate vectors after the shuffle phase
     */
    std::vector<IntermediateVec> _intermediateVec;
    /**
     * a pointer to array of contexts for each thread
     */
    Context *_context;
    /**
     * a function that run with each thread, this is the main function that call the function sort,and reduce.
     * @param context the context for each thread
     * @return the function dos not return anything
     */
    static void* mapPhase(void* context);
    /**
     * a sort function that sort the intermediate vector.
     * @param context the context of the thread
     */
    static void sort_elements(void *context);
    /**
     * a comparision function between tow keys K1,K2.
     * @param x the first key
     * @param y the second key
     * @return true if the first is smaller, false otherwise
     */
    static bool comp(const IntermediatePair &x,const IntermediatePair &y);
    /**
     * delete the operator = of the class Job
     * @param other
     * @return
     */
    Job operator=(const Job &other) = delete;
    /**
     * delete the constructor of the class Job
     * @param other
     * @return
     */
    Job(const Job &other) = delete;
    /**
     * delete the operator = of the class Job
     * @param other
     * @return
     */
    Job operator=(const Job &&other) = delete;
    /**
     * delete the constructor of the class Job
     * @param other
     * @return
     */
    Job(const Job &&other) = delete;
    /**
     * a function that print a error the std::cerr
     * @param msg the message
     */
    static void systemError(const std::string& msg);

public:
    /**
     * the constructor of the Job class
     * @param client a pointer to the client object
     * @param inputVec the input vector
     * @param outputVec the output vector
     * @param multiThreadLevel the number of threads
     */
    Job(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec,
        int multiThreadLevel);
    /**
     * the distractore of the class
     */
    ~Job();
    /**
     * the function call pthread_join
     */
    void waitForThreads();
    /**
     * the function gets the current state of the job
     * @param job the job
     * @return the current state
     */
    JobState currentState(JobHandle job);
    /**
     * the function add the pair to intermediate vector
     * @param key the key of the pair
     * @param value the value of the pair
     * @param context the context of the thread
     */
    void emit2PushInterMid(K2* key, V2* value, void* context);
    /**
     * the function add the pair to output vector
     * @param key the key of the pair
     * @param value the value of the pair
     * @param context the context of the thread
     */
    void emit3PushOut(K3* key, V3* value, void* context);
    /**
     * the function shuffle the intermediate vector into vector of vectors which each vector has the same key
     * @param context a context of a thread that should ran the shuffle phase(das not matter who is it)
     */
    static void shuffle(Context *context);
};

#endif //MAPREDUCE_EX3_JOB_H
