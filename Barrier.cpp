#include "Barrier.h"
#include <cstdlib>
#include <cstdio>
#include <iostream>

#define SYSTEM_ERROR_MESSAGE "system error: "

Barrier::Barrier(int numThreads)
		: mutex(PTHREAD_MUTEX_INITIALIZER)
		, cv(PTHREAD_COND_INITIALIZER)
		, count(0)
		, numThreads(numThreads)
{ }


Barrier::~Barrier()
{
	if (pthread_mutex_destroy(&mutex) != 0) {
		std::cerr << SYSTEM_ERROR_MESSAGE <<"[[Barrier]] error on pthread_mutex_destroy\n";
		exit(1);
	}
	if (pthread_cond_destroy(&cv) != 0){
		std::cerr << SYSTEM_ERROR_MESSAGE << "[[Barrier]] error on pthread_cond_destroy\n";
		exit(1);
	}
}


void Barrier::barrier()
{
	if (pthread_mutex_lock(&mutex) != 0){
		std::cerr << SYSTEM_ERROR_MESSAGE << "[[Barrier]] error on pthread_mutex_lock\n";
		exit(1);
	}
	if (++count < numThreads) {
		if (pthread_cond_wait(&cv, &mutex) != 0){
			std::cerr << SYSTEM_ERROR_MESSAGE << "[[Barrier]] error on pthread_cond_wait\n";
			exit(1);
		}
	} else {
		count = 0;
		if (pthread_cond_broadcast(&cv) != 0) {
			std::cerr << SYSTEM_ERROR_MESSAGE << "[[Barrier]] error on pthread_cond_broadcast\n";
			exit(1);
		}
	}
	if (pthread_mutex_unlock(&mutex) != 0) {
		std::cerr << SYSTEM_ERROR_MESSAGE << "[[Barrier]] error on pthread_mutex_unlock\n";
		exit(1);
	}
}
