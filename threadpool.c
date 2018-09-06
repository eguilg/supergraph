#include "threadpool.h"

threadpool* threadpool_init(int thread_num, int queue_max_num)
{
    threadpool *pool = NULL;
    do
    {
        pool = malloc(sizeof(threadpool));
        if (NULL == pool)
        {
            printf("failed to malloc threadpool!\n");
            break;
        }
        pool->thread_num = thread_num;
        pool->queue_max_num = queue_max_num;
        pool->queue_cur_num = 0;
        pool->work_cur_num = 0;
        pool->head = NULL;
        pool->tail = NULL;
        if (pthread_mutex_init(&(pool->mutex), NULL))
        {
            printf("failed to init mutex!\n");
            break;
        }
        if (pthread_cond_init(&(pool->queue_empty), NULL))
        {
            printf("failed to init queue_empty!\n");
            break;
        }
        if (pthread_cond_init(&(pool->queue_not_empty), NULL))
        {
            printf("failed to init queue_not_empty!\n");
            break;
        }
        if (pthread_cond_init(&(pool->queue_not_full), NULL))
        {
            printf("failed to init queue_not_full!\n");
            break;
        }
        if (pthread_cond_init(&(pool->no_more_work), NULL))
        {
            printf("failed to init no_more_work!\n");
            break;
        }
        pool->pthreads = malloc(sizeof(pthread_t) * thread_num);
        if (NULL == pool->pthreads)
        {
            printf("failed to malloc pthreads!\n");
            break;
        }
        pool->queue_close = 0;
        pool->pool_close = 0;
        int i;
        for (i = 0; i < pool->thread_num; ++i)
        {
            pthread_create(&(pool->pthreads[i]), NULL, threadpool_function, (void *)pool);
        }

        return pool;
    } while (0);

    return NULL;
}

int threadpool_try_add_job(threadpool* pool, void* (*callback_function)(void *arg), void *arg){
    assert(pool != NULL);
    assert(callback_function != NULL);
    assert(arg != NULL);
    pthread_mutex_lock(&(pool->mutex));
    if ((pool->work_cur_num==pool->thread_num)&&(pool->queue_cur_num == pool->queue_max_num) && !(pool->queue_close || pool->pool_close))
    {
         pthread_mutex_unlock(&(pool->mutex));

        return -1;   // return if the queue is full
    }
    if ((pool->work_cur_num<pool->thread_num)&&(pool->queue_cur_num == pool->queue_max_num) && !(pool->queue_close || pool->pool_close))
    {
        pthread_cond_wait(&(pool->queue_not_full), &(pool->mutex));   // wait if the queue is full
    }
    if (pool->queue_close || pool->pool_close)    // exit if the queue or the pool is closed
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }
    job *pjob =(job*) malloc(sizeof(job));
    if (NULL == pjob)
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }
    pjob->callback_function = callback_function;
    pjob->arg = arg;
    pjob->next = NULL;

    if (pool->head == NULL)
    {
        pool->head = pool->tail = pjob;
        pthread_cond_broadcast(&(pool->queue_not_empty));  // set the queue not empty
    }
    else
    {
        pool->tail->next = pjob;
        pool->tail = pjob;
    }
    pool->queue_cur_num++;
    pthread_mutex_unlock(&(pool->mutex));
    return 0;

}

int threadpool_add_job(threadpool* pool, void* (*callback_function)(void *arg), void *arg)
{
    assert(pool != NULL);
    assert(callback_function != NULL);
    assert(arg != NULL);

    pthread_mutex_lock(&(pool->mutex));
    while ((pool->queue_cur_num == pool->queue_max_num) && !(pool->queue_close || pool->pool_close))
    {
        pthread_cond_wait(&(pool->queue_not_full), &(pool->mutex));   // wait if the queue is full
    }
    if (pool->queue_close || pool->pool_close)    // exit if the queue or the pool is closed
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }
    job *pjob =(job*) malloc(sizeof(job));
    if (NULL == pjob)
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }
    pjob->callback_function = callback_function;
    pjob->arg = arg;
    pjob->next = NULL;
    if (pool->head == NULL)
    {
        pool->head = pool->tail = pjob;
        pthread_cond_broadcast(&(pool->queue_not_empty));  // set the queue not empty
    }
    else
    {
        pool->tail->next = pjob;
        pool->tail = pjob;
    }
    pool->queue_cur_num++;
    pthread_mutex_unlock(&(pool->mutex));
    return 0;
}

void* threadpool_function(void* arg)
{
    threadpool *pool = (threadpool*)arg;
    job *pjob = NULL;
    while (1)  // loop forever
    {
        pthread_mutex_lock(&(pool->mutex));
        while ((pool->queue_cur_num == 0) && !pool->pool_close)
        {
            pthread_cond_wait(&(pool->queue_not_empty), &(pool->mutex));
        }
        if (pool->pool_close)   // exit when the pool is closed
        {
            pthread_mutex_unlock(&(pool->mutex));
            pthread_exit(NULL);
        }
        pool->queue_cur_num--;
        pjob = pool->head;
        if (pool->queue_cur_num == 0)
        {
            pool->head = pool->tail = NULL;
        }
        else
        {
            pool->head = pjob->next;
        }
        if (pool->queue_cur_num == 0)
        {
            pthread_cond_signal(&(pool->queue_empty));
        }
        if (pool->queue_cur_num == pool->queue_max_num - 1)
        {
            pthread_cond_broadcast(&(pool->queue_not_full));
        }
        pool->work_cur_num++;
        pthread_mutex_unlock(&(pool->mutex));

        (*(pjob->callback_function))(pjob->arg);
        free(pjob);
        pjob = NULL;
        pthread_mutex_lock(&(pool->mutex));
        pool->work_cur_num--;
        if(pool->queue_cur_num == 0 && pool->work_cur_num == 0){
            pthread_cond_broadcast(&(pool->no_more_work));
        }
        pthread_mutex_unlock(&(pool->mutex));


    }
}

int threadpool_join(threadpool *pool){
    while(pool->queue_cur_num || pool->work_cur_num){
        pthread_cond_wait(&(pool->no_more_work), &(pool->mutex));
    }
    pthread_mutex_unlock( &(pool->mutex));
    return 0;
}

int threadpool_destroy(threadpool *pool)
{
    assert(pool != NULL);
    pthread_mutex_lock(&(pool->mutex));
    if (pool->queue_close || pool->pool_close)
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }

    pool->queue_close = 1;        // set the queue closed
    while (pool->queue_cur_num != 0)
    {
        pthread_cond_wait(&(pool->queue_empty), &(pool->mutex));
    }
    pool->pool_close = 1;      // set the pool closed
    while(pool->work_cur_num !=0 ){
        pthread_cond_wait(&(pool->no_more_work), &(pool->mutex));
    }

    pthread_mutex_unlock(&(pool->mutex));
    pthread_cond_broadcast(&(pool->queue_not_empty));
    pthread_cond_broadcast(&(pool->queue_not_full));
    pthread_cond_broadcast(&(pool->no_more_work));
    int i;
    for (i = 0; i < pool->thread_num; ++i)
    {
        pthread_join(pool->pthreads[i], NULL);
    }

    pthread_mutex_destroy(&(pool->mutex));          // clean
    pthread_cond_destroy(&(pool->queue_empty));
    pthread_cond_destroy(&(pool->queue_not_empty));
    pthread_cond_destroy(&(pool->queue_not_full));
    pthread_cond_destroy(&(pool->no_more_work));
    free(pool->pthreads);
    job *p;
    while (pool->head != NULL)
    {
        p = pool->head;
        pool->head = p->next;
        free(p);
    }
    free(pool);
    return 0;
}