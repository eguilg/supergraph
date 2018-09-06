#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include "threadpool.h"
#include "supergraph.h"
#include "supergraph_loader.h"

int main(void)
{
    size_t * user_count = (size_t*) malloc(sizeof(size_t));
    user * users = user_loader(user_count, "../graphs/social_medium/social.users");
    size_t * post_count = (size_t*) malloc(sizeof(size_t));
    post* posts = post_loader(post_count, "../graphs/social_medium/social.posts");

    printf("user count: %lu\n", *user_count);
    printf("post count: %lu\n", *post_count);

    clock_t begin = clock();
    query_helper* helper = engine_setup(4);
    double time_elapsed = (double)(clock() - begin) / CLOCKS_PER_SEC;
    printf("Setup time: %f\n", time_elapsed);

    size_t target_post_idx = 666;

    begin = clock();
    result* r1 = find_all_reposts(posts, *post_count, posts[target_post_idx].pst_id, helper);
    time_elapsed = (double)(clock() - begin) / CLOCKS_PER_SEC;
    printf("Query 1 time: %f\n", time_elapsed);
    printf("Query 1 n_elements: %d\n",r1->n_elements);
    free(r1->elements);
    free(r1);

    begin = clock();
    result* r2 = find_original(posts, *post_count, posts[target_post_idx].pst_id, helper);
    time_elapsed = (double)(clock() - begin) / CLOCKS_PER_SEC;
    printf("Query 2 time: %f\n", time_elapsed);
    printf("Query 2 id: %d\n",((post*)(r2->elements[0]))->pst_id);
    free(r2->elements);
    free(r2);

    size_t userA_idx = 0;
    size_t userB_idx = 5;
    begin = clock();
    result* r3 = shortest_user_link(users, *user_count,
                                    users[userA_idx].user_id, users[userB_idx].user_id, helper);
    time_elapsed = (double)(clock() - begin) / CLOCKS_PER_SEC;
    printf("Query 3 time: %f\n", time_elapsed);
    printf("Query 3 n_elements: %d\n",r3->n_elements);
//    printf("%lu->%lu:\n",  users[userA_idx].user_id ,users[userB_idx].user_id);
//    for(size_t i = 0; i< r3->n_elements; i++){
//        printf("%lu ", ((user*)(r3->elements[i]))->user_id);
//    }
//    printf("\n");
//    for(size_t i = 0; i< r3->n_elements; i++){
//        printf("%lu: ", ((user*)(r3->elements[i]))->user_id);
//        for(size_t j=0; j < ((user*)(r3->elements[i]))->n_followers;j++){
//            printf("%lu ",users[((user*)(r3->elements[i]))->follower_idxs[j]].user_id);
//        }
//        printf("\n");
//    }
    free(r3->elements);
    free(r3);

//    begin = clock();
//    int * reposts_tag = tag_reposts(posts, *post_count);
//    time_elapsed = (double)(clock() - begin)/ CLOCKS_PER_SEC;
//    printf("tag reposts time: %f\n", time_elapsed);
    criteria a;
    a.acc_rep_threshold = 0.1;
    a.bot_net_threshold = 0.9;
    a.oc_threshold = 0.175;
    begin = clock();
    result* r4 = find_bots(users, *user_count, posts, *post_count, &a,helper);
    time_elapsed = (double)(clock() - begin) / CLOCKS_PER_SEC;
    printf("Query 4 time: %f\n", time_elapsed);
    printf("Query 4 n_elements: %d\n",r4->n_elements);
    free(r4->elements);
    free(r4);

    begin = clock();
    engine_cleanup(helper);
    time_elapsed = (double)(clock() - begin)/ CLOCKS_PER_SEC;
    printf("engine_cleanup time: %f\n", time_elapsed);

    free(posts); posts = NULL;
    free(users); users = NULL;
    free(user_count); user_count =NULL;
    free(post_count); post_count = NULL;
}
