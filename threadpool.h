#ifndef THREADPOOL_H
#define THREADPOOL_H
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
typedef struct job
{
    void* (*callback_function)(void *arg);    //thread callback function
    void *arg;                                // callback function arguments
    struct job *next;
}job;

typedef struct threadpool
{
    int thread_num;                   //thread num in the pool
    int queue_max_num;                //maximum job in the queue
    struct job *head;                 //pointer to the head job
    struct job *tail;                 //pointer to the tail job
    pthread_t *pthreads;              //pthread_t of all threads
    pthread_mutex_t mutex;            //mutex
    pthread_cond_t queue_empty;       // if the queue is empty
    pthread_cond_t queue_not_empty;   // if the queue is not empty
    pthread_cond_t queue_not_full;    // if the queue is not full
    pthread_cond_t no_more_work; // if there is no more work
    int queue_cur_num;                // current jobs in the queue
    int work_cur_num;
    int queue_close;                  // is queue closed
    int pool_close;                   // is the pool closed
} threadpool;

threadpool* threadpool_init(int thread_num, int queue_max_num);

int threadpool_try_add_job(threadpool* pool, void* (*callback_function)(void *arg), void *arg);

int threadpool_add_job(threadpool *pool, void* (*callback_function)(void *arg), void *arg);

int threadpool_join(threadpool *pool);

int threadpool_destroy(threadpool *pool);

void* threadpool_function(void* arg);

#endif // THREADPOOL_H
