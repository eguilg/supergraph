#ifndef SUPERGRAPH_H
#define SUPERGRAPH_H
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <memory.h>
#include "threadpool.h"


typedef struct query_helper query_helper;
typedef struct result result;
typedef struct criteria criteria;
typedef struct user user;
typedef struct post post;
typedef struct thread_worker thread_worker;



struct query_helper {
	int dummy_field;
    size_t n_processors;
    threadpool *pool;

};

struct result {
	void** elements;
	size_t n_elements;
};

struct criteria {
	float oc_threshold;
	float acc_rep_threshold;
	float bot_net_threshold;
};

struct user {
	uint64_t user_id;
	size_t* follower_idxs;
	size_t n_followers;
	size_t* following_idxs;
	size_t n_following;
	size_t* post_idxs;
	size_t n_posts;
};

struct post {
	uint64_t pst_id;
	uint64_t timestamp;
	size_t* reposted_idxs;
	size_t n_reposted;
};

struct thread_worker
{
    /*
     * common use
     */
    post* all_posts;
    size_t n_posts;
    user* all_users;
    size_t  n_users;
    result* result;
    pthread_mutex_t*  result_mutex;
    threadpool* pool;

    /*
     * for find_all_reposts
     */
    uint64_t target_post_id;
    size_t target_post_idx;

    /*
     * for find_original
     */
    int * fathor_post_found;
    size_t * father_post_idx;
    size_t search_start;
    size_t search_end;

    /*
     * for shortest_user_link
     */
    size_t * user_distance;
    pthread_mutex_t * user_distance_mutexs;
    size_t cur_root_user_idx;
    size_t userB_idx;

    /*
     * for find_bots
     */
    int * reposts_tag;
    int * bots_tag;
    int* updated;
    criteria* crit;
};


query_helper* engine_setup(size_t n_processors);

result* find_all_reposts(post* posts, size_t count, uint64_t post_id, query_helper* helper);

result* find_original(post* posts, size_t count, uint64_t post_id, query_helper* helper);

result* shortest_user_link(user* users, size_t count, uint64_t userA, uint64_t userB, query_helper* helper);

result* find_bots(user* users, size_t user_count, post* posts, size_t post_count, criteria* crit, query_helper* helper);

void engine_cleanup(query_helper* helpers);

#endif
