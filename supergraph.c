#include "supergraph.h"

query_helper* engine_setup(size_t n_processors) {
    query_helper* helper =(query_helper*)malloc(sizeof(query_helper));
    helper->pool = threadpool_init((int) n_processors,(int) (n_processors * 5));
    helper->n_processors = n_processors;
    return helper;
}

void*_find_all_reposts_base_work(void* arg){
    thread_worker *worker = (thread_worker*) arg;
    //find the target post indice
    if(worker->target_post_idx >= worker->n_posts){
        for(size_t i =0; i < worker->n_posts; i++){
            if(worker->all_posts[i].pst_id == worker->target_post_id)
                worker->target_post_idx = i;
        }
    }
    if(worker->target_post_idx >= worker->n_posts){
        free(worker);
        worker = NULL;
        return NULL;
    }

    post target_post = worker->all_posts[worker->target_post_idx];

    // write the repost of target post to result
    pthread_mutex_lock(worker->result_mutex); // get the result lock

    // add the current root post to result
    if(worker->result->n_elements == 0){
        worker->result->elements =  (void **) malloc(sizeof(post*));
        worker->result->elements[worker->result->n_elements] = (void*)(worker->all_posts + worker->target_post_idx);
        worker->result->n_elements++;
    }

    // add the reposts of curr root post
    worker->result->elements = (void **) realloc(worker->result->elements,
                                                 sizeof(post*)*(target_post.n_reposted + worker->result->n_elements));
    for(size_t i = 0; i < target_post.n_reposted; i++){
        worker->result->elements[worker->result->n_elements] = (void *)(worker->all_posts +target_post.reposted_idxs[i]);
        worker->result->n_elements++;
    }
    pthread_mutex_unlock(worker->result_mutex);// release the result lock

    // next level
    for(size_t i = 0; i < target_post.n_reposted; i++){
        // init the worker
        thread_worker * nxt_worker =  (thread_worker *)malloc(sizeof(thread_worker));
        nxt_worker->all_posts = worker->all_posts;
        nxt_worker->n_posts = worker->n_posts;
        nxt_worker->target_post_id = target_post.pst_id;
        nxt_worker->target_post_idx= target_post.reposted_idxs[i];
        nxt_worker->result = worker->result;
        nxt_worker->result_mutex = worker->result_mutex;
        nxt_worker->pool = worker->pool;
        // adding the job, add a new thread if  there exists avaliable thread in the pool or the queue isnt full,
        // otherwise continue search on current thread
        if(0 == threadpool_try_add_job(worker->pool, _find_all_reposts_base_work, nxt_worker)){ // new thread added
            continue;
        }else{
            _find_all_reposts_base_work(nxt_worker); // all threads are busy, continue search by current thread itself
        }

    }
    free(worker);
    worker = NULL;
    return NULL;
}

result* find_all_reposts(post* posts, size_t count, uint64_t post_id, query_helper* helper) {

    // init result
    result* res = (result*)malloc(sizeof(result));
    res->n_elements = 0;
    res->elements = NULL;

    // init worker
    thread_worker * worker =  (thread_worker *)malloc(sizeof(thread_worker));
    worker->all_posts = posts;
    worker->n_posts = count;
    worker->target_post_id = post_id;
    worker->target_post_idx = -1;
    worker->result = res;

    pthread_mutex_t result_mutex;
    pthread_mutex_init(&result_mutex, NULL);
    worker->result_mutex = &result_mutex;
    worker->pool = helper->pool;

    // start main thread
    threadpool_add_job(helper->pool, _find_all_reposts_base_work, worker);
    // block until finishing all jobs in the pool
    threadpool_join(helper->pool);
    pthread_mutex_destroy(&result_mutex);
    return res;
}

void*_find_original_base_work(void *arg){

    thread_worker *worker = (thread_worker*) arg;
    post *p = worker->all_posts + worker->search_start;
    post target_post = worker->all_posts[*(worker->father_post_idx)];
    while(!(*(worker->fathor_post_found))
          && p < worker->all_posts + worker->search_end){
        // posted after the target post cant be the origin of the target
        if(p->n_reposted && p->reposted_idxs && p->timestamp <= target_post.timestamp){
            for(size_t i=0; i<p->n_reposted; i++){
                if(p->reposted_idxs[i] == *(worker->father_post_idx)){
                    if(!worker->result->elements || worker->result->n_elements == 0){
                          worker->result->elements = (void**) malloc(sizeof(post*));
                          worker->result->n_elements = 1;
                    }
                    worker->result->elements[0] = p;
                    *(worker->father_post_idx) = p - worker->all_posts;
                    *(worker->fathor_post_found) = 1;
                    break;
                }
            }
        }
        p++;
    }
    return NULL;
}

result* find_original(post* posts, size_t count, uint64_t post_id, query_helper* helper) {
    // init result
    result* res = (result*)malloc(sizeof(result));
    res->n_elements = 0;
    res->elements = NULL;

    size_t * post_idx = ( size_t *)malloc(sizeof(size_t));
    *post_idx = SIZE_MAX;
    for(size_t i =0; i <  count; i++){
        if(posts[i].pst_id == post_id)
           *post_idx = i;
    }
    if(*post_idx >= count){
        free(post_idx);
        post_idx = NULL;
        return res;
    }

    // init the result origin as the given post
    res->n_elements = 1;
    res->elements = (void**) malloc(sizeof(post*));
    res->elements[0] = posts + *post_idx;

    int * father_post_found =  ( int *)malloc(sizeof(int));
    *father_post_found = 0;
    // init workers
    size_t n_workers = helper->n_processors;
    thread_worker * workers =  (thread_worker *)malloc(sizeof(thread_worker)*n_workers);
    for(size_t i = 0; i < n_workers; i++)
    {
        workers[i].search_start = i * (count/n_workers);
        workers[i].search_end = (i == n_workers -1)? count : (i+1) * (count/n_workers);
        workers[i].all_posts = posts;
        workers[i].n_posts = count;
        workers[i].father_post_idx = post_idx;
        workers[i].fathor_post_found = father_post_found;
        workers[i].result = res;
        workers[i].pool = helper->pool;
    }

    while(1){
        for(size_t i = 0; i < n_workers; i++)
        {
            threadpool_add_job(helper->pool, _find_original_base_work, &workers[i]);
        }
        threadpool_join(helper->pool);

        if(!*father_post_found)
            break;
        *father_post_found = 0;
    }

    // clean up
    free(post_idx);
    post_idx = NULL;
    free(father_post_found);
    father_post_found = NULL;
    free(workers);
    workers = NULL;

    return res;
}

void* _shortest_user_link_base_work(void *arg){
    thread_worker *worker = (thread_worker*) arg;

    user cur_root_user = worker->all_users[worker->cur_root_user_idx];
    for(size_t i=0; i<cur_root_user.n_followers; i++){
        size_t idx = cur_root_user.follower_idxs[i];
        if(idx == worker->cur_root_user_idx  )
            break;
        int is_updating = 0;
        while(1){
            pthread_mutex_lock(&(worker->user_distance_mutexs[worker->cur_root_user_idx]));
          if(pthread_mutex_trylock(&(worker->user_distance_mutexs[idx]))!=0) {
                 pthread_mutex_unlock(&(worker->user_distance_mutexs[worker->cur_root_user_idx]));
            }else{
                if(worker->user_distance[idx] > 1 + worker->user_distance[worker->cur_root_user_idx]){
                    worker->user_distance[idx] =1+ worker->user_distance[worker->cur_root_user_idx];
                    is_updating = 1;
                }
                pthread_mutex_unlock(&(worker->user_distance_mutexs[worker->cur_root_user_idx]));
                break;
            }
        }
        if(idx == worker->userB_idx){
            pthread_mutex_unlock(&(worker->user_distance_mutexs[idx]));
            break;
        }
        if(is_updating){ // changed

//            pthread_mutex_lock(&(worker->user_distance_mutexs[idx]));
//            pthread_mutex_lock(&(worker->user_distance_mutexs[worker->userB_idx]));
            if(worker->user_distance[idx]+1>=worker->user_distance[worker->userB_idx]){

                // found userB or cur distance larger than userB, stop

                pthread_mutex_unlock(&(worker->user_distance_mutexs[idx]));
//                pthread_mutex_unlock(&(worker->user_distance_mutexs[worker->userB_idx]));
                break;
            }else{

                pthread_mutex_unlock(&(worker->user_distance_mutexs[idx]));
//                pthread_mutex_unlock(&(worker->user_distance_mutexs[worker->userB_idx]));
                // next level
                thread_worker * nxt_worker =  (thread_worker *)malloc(sizeof(thread_worker));
                nxt_worker->all_users = worker->all_users;
                nxt_worker->n_users = worker->n_users;
                nxt_worker->user_distance = worker->user_distance;
                nxt_worker->user_distance_mutexs = worker->user_distance_mutexs;
                nxt_worker->cur_root_user_idx = idx;
                nxt_worker->userB_idx = worker->userB_idx;
                nxt_worker->pool = worker->pool;

                if(0 == threadpool_try_add_job(worker->pool, _shortest_user_link_base_work, nxt_worker)){
                    continue;
                }else{
                    _shortest_user_link_base_work(nxt_worker);
                }
            }

        }else{

            pthread_mutex_unlock(&(worker->user_distance_mutexs[idx]));
        }
    }
    // clean up

    free(worker);
    worker = NULL;
    return NULL;
}

result* shortest_user_link(user* users, size_t count, uint64_t userA, uint64_t userB, query_helper* helper) {

    if(userA == userB)
        return NULL;


    // get user A & B idx
    size_t userA_idx = count+1;
    size_t userB_idx = count+1;
    for(size_t i = 0; i<count; i++){
        if(userA_idx < count && userB_idx < count)
            break;
        if(users[i].user_id == userA)
            userA_idx = i;
        if(users[i].user_id == userB)
            userB_idx = i;
    }
    if(userA_idx > count || userB_idx >count)
        return NULL;

    // init result

    result* res = (result*)malloc(sizeof(result));
    res->n_elements = 0;
    res->elements = NULL;

    // init user distance to A
    // init user visit
    // init distance mutexs
    size_t * user_distance = (size_t*)malloc(sizeof(size_t)*count);
    pthread_mutex_t distance_mutexs[count];
    for(size_t i = 0; i<count; i++){
        user_distance[i] = count;
        pthread_mutex_init(&distance_mutexs[i], NULL);
    }
    user_distance[userA_idx] = 0;

    // init worker
    thread_worker * worker =  (thread_worker *)malloc(sizeof(thread_worker));
    worker->all_users = users;
    worker->n_users = count;
    worker->user_distance = user_distance;
    worker->user_distance_mutexs = distance_mutexs;
    worker->cur_root_user_idx = userA_idx;
    worker->userB_idx = userB_idx;
    worker->pool = helper->pool;

    threadpool_add_job(helper->pool, _shortest_user_link_base_work, worker);
    threadpool_join(helper->pool);

    // figure out the path using the distance table

    size_t min_dis = count;
    for(size_t j = 0; j< users[userB_idx].n_following; j ++){
        if(min_dis > user_distance[users[userB_idx].following_idxs[j]]){
            min_dis = user_distance[users[userB_idx].following_idxs[j]];
        }
    }
    user_distance[userB_idx] = min_dis+1;
     size_t dis_B = user_distance[userB_idx];
    if(dis_B < count)
    {
        res->n_elements = dis_B+1;
        //printf("%lu\n",res->n_elements);
        res->elements = (void**)malloc(sizeof(user *)*res->n_elements);
        size_t cur_idx = userB_idx;
        size_t min_dis = dis_B;
        size_t cur_min_idx = userB_idx;
        for(size_t i = 0; i<res->n_elements; i++){
            res->elements[i] = (void*)&(users[cur_idx]);

            for(size_t j = 0; j<users[cur_idx].n_following; j++){
                if(user_distance[users[cur_idx].following_idxs[j]]<min_dis){
                    cur_min_idx = users[cur_idx].following_idxs[j];
                    min_dis = user_distance[cur_min_idx];
                }
            }
            cur_idx = cur_min_idx;
        }

        res->elements[res->n_elements-1] = (void*)&users[userA_idx];
    }


    // clean up
    for(size_t i=0; i<count;i++){
        pthread_mutex_destroy(&distance_mutexs[i]);
    }
    free(user_distance);
    user_distance = NULL;

    return res;
}

int * _tag_reposts(post* posts, size_t count){
    int* reposts_tag = (int*)malloc(sizeof(int)*count);
    memset(reposts_tag, 0, sizeof(int)*count);
    for(size_t i = 0; i<count; i++){
        for(size_t j = 0; j<posts[i].n_reposted;j++){
            if(posts[i].reposted_idxs[j]<count)
                reposts_tag[posts[i].reposted_idxs[j]]=1;
        }
    }
    return reposts_tag;
}

int * _tag_bots(user* users, size_t user_count, int* reposts_tag, size_t post_count, criteria* crit ){
    int* bots_tag = (int*)malloc(sizeof(int)*user_count); // malloc of bots_tag
    memset(bots_tag, 0, sizeof(int)*user_count);
    for(size_t i = 0; i<user_count; i++){
        if(users[i].n_followers < crit->acc_rep_threshold * (users[i].n_followers+users[i].n_following)){
            bots_tag[i] = 1;
            continue;
        }
        size_t n_reposts = 0;
        for(size_t j = 0;j < users[i].n_posts;j++){
            if(users[i].post_idxs[j]<post_count){
                if(reposts_tag[users[i].post_idxs[j]] == 1){
                    n_reposts++;
                }
            }
        }
        if(n_reposts > users[i].n_posts*crit->oc_threshold)
            bots_tag[i] = 1;
    }
    return bots_tag;
}

void* _tag_discrete_bots(void * arg){
    thread_worker*  worker = (thread_worker*) arg;
    for(size_t i = worker->search_start; i<worker->search_end;i++){
        if(worker->bots_tag[i] == 1||worker->all_users[i].n_followers==0)
            continue;
        size_t n_bots_follower = 0;
        for(size_t j = 0; j < worker->all_users[i].n_followers; j++){
            if(worker->all_users[i].follower_idxs[j]<worker->n_users){
                if(worker->bots_tag[worker->all_users[i].follower_idxs[j]<worker->n_users] == 1){
                    n_bots_follower++;
                }
            }
        }
        if(n_bots_follower > worker->crit->bot_net_threshold * worker->all_users[i].n_followers){
            worker->bots_tag[i]=1;
            *(worker->updated) = 1;
        }
    }
    return NULL;
}

result* find_bots(user* users, size_t user_count, post* posts, size_t post_count, criteria* crit, query_helper* helper) {
    result* res = (result*)malloc(sizeof(result));// malloc of res
    res->n_elements = 0;
    res->elements = NULL;

    if(crit->acc_rep_threshold>1 || crit->acc_rep_threshold<0 ||crit->bot_net_threshold>1||crit->bot_net_threshold<0
            ||crit->oc_threshold>1||crit->oc_threshold<0)
        return res;

    int* reposts_tag = _tag_reposts(posts, post_count); // malloc of reposts_tag
    int* bots_tag = _tag_bots(users, user_count, reposts_tag, post_count, crit); // malloc of bots_tag
    int* updated = (int*) malloc(sizeof(int));

    // init workers
    size_t n_workers = helper->n_processors;
    thread_worker * workers =  (thread_worker *)malloc(sizeof(thread_worker)*n_workers);
    for(size_t i = 0; i < n_workers; i++)
    {
        workers[i].search_start = i * (user_count/n_workers);
        workers[i].search_end = (i == n_workers -1)? user_count : (i+1) * (user_count/n_workers);
        workers[i].all_users = users;
        workers[i].n_users =user_count;
        workers[i].bots_tag = bots_tag;
        workers[i].updated = updated;
        workers[i].crit = crit;
        workers[i].pool = helper->pool;
    }


    while(1){
        *updated = 0;
        for(size_t i = 0; i < n_workers; i++)
        {
            threadpool_add_job(helper->pool, _tag_discrete_bots, &workers[i]);
        }
        threadpool_join(helper->pool);

        if(*updated==0)
            break;
    }

    for(size_t i = 0; i<user_count; i++){
        if(bots_tag[i]==1)
            res->n_elements++;
    }

    if(res->n_elements>0){
          res->elements = (void**)malloc(sizeof(user*)*res->n_elements);
          size_t count = 0;
          for(size_t i = 0; i<user_count; i++){
              if(bots_tag[i]==1){
                  res->elements[count] = &users[i];
                  count++;
              }
          }
    }



    //clean up
    free(reposts_tag); reposts_tag = NULL;
    free(bots_tag); bots_tag = NULL;
    free(updated); updated = NULL;
    free(workers);
    workers = NULL;
    return res;
}

void engine_cleanup(query_helper* helpers) {
    threadpool_destroy(helpers->pool);
    free(helpers);
}


