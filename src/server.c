/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <time.h>



#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DIV_UP(x,y) ((x + y - 1)/y)
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define MAX_WORKER_THREADS  3
#define MAX_SHARE_PER_THREAD 15
#define JOIN_VECTOR_SIZE 2000  //Number of ints per vector. (32k instruction L1 cache, shared by 2 threads = 16k. 16k/sizeof(int) = 4000)

//#define NAIVE_BATCH 1
#define MULTICORE_BATCH 1

//#define NAIVE_JOIN 1 //Use the naive join method
//#define VECTOR_JOIN 1//Use vectorized processing
#define FOUR_THREAD_JOIN 1 //Use 4 cores to join


// void* select_worker(void* args){
//     size_t start = ((int*)args)[0];
//     size_t end = ((int*)args)[1];
//     size_t num_share = end-start;
//     cs165_log(stdout, "I am in charge of doing %i through %i!\n", start, end-1);
//     Column* active = query_buffer[start].operator_fields.select_operator.column;
//     Column res[num_share];
//     bool exists_lower[num_share];
//     bool exists_upper[num_share];
//     int lower[num_share];
//     int upper[num_share];
//     char* lvals[num_share];
//     for(size_t i = 0; i < num_share; i++){
//         lvals[i] = query_buffer[i+start].operator_fields.select_operator.lval;
//         res[i].column_max = RES_START_SIZE;
//         res[i].column_length = 0;
//         //res[i].data = (int*) malloc(sizeof(int)*RES_START_SIZE);
//         res[i].data = (int*) malloc(sizeof(int)*active->column_length);
//         strcpy(res[i].name, lvals[i]);
//         res[i].type = INT;
//         exists_lower[i] = query_buffer[i+start].operator_fields.select_operator.exists_lower;
//         exists_upper[i] = query_buffer[i+start].operator_fields.select_operator.exists_upper;
//         lower[i] = query_buffer[i+start].operator_fields.select_operator.lower;
//         upper[i] = query_buffer[i+start].operator_fields.select_operator.upper;
//     }


//     for(size_t i = 0; i < active->column_length; i++){
//         for(size_t j = 0; j < num_share; j++){
//             if( (exists_upper[j] && exists_lower[j] && active->data[i] >= lower[j] && active->data[i] < upper[j]) || (exists_upper[j] && !exists_lower[j] && active->data[i] < upper[j]) ||  (!exists_upper[j] && exists_lower[j] && active->data[i] >= lower[j]) ){
//                 res[j].data[res[j].column_length++] = i;
//                 // if(res[j].column_length == res[j].column_max){
//                 //     res[j].data = realloc(res[j].data, sizeof(int) * (RES_MULTIPLE * res[j].column_max));
//                 //     res[j].column_max = res[j].column_max * RES_MULTIPLE;
//                 // }
//             }            
//         }
//     }

//     for(size_t i = 0; i < num_share; i++){
//         Column* to_store = (Column*)malloc(sizeof(Column));
//         memcpy(to_store, &res[i], sizeof(Column));
//         to_store->data = realloc(to_store->data, sizeof(int)*to_store->column_length);
//         to_store->column_max = to_store->column_length;
//         store_client_variable(lvals[i], to_store);
//     }

//     return NULL;
// }

//void share_scan(size_t start, size_t end){
void* share_scan(void* args){
    size_t start = ((int*)args)[0];
    size_t end = ((int*)args)[1];
    cs165_log(stdout, "I am in charge of scanning %i through %i!\n", start, end-1);
    size_t num_share = end-start;
    Column* active = query_buffer[start].operator_fields.select_operator.column;
    Column res[num_share];
    // bool exists_lower[num_share];
    // bool exists_upper[num_share];
    int lower[num_share];
    int upper[num_share];
    char* lvals[num_share];
    for(size_t i = 0; i < num_share; i++){
        lvals[i] = query_buffer[i+start].operator_fields.select_operator.lval;
        res[i].column_max = active->column_length;
        res[i].column_length = 0;
        res[i].data = (int*) malloc(sizeof(int)*active->column_length);
        strcpy(res[i].name, lvals[i]);
        res[i].type = INT;
 //       exists_lower[i] = query_buffer[i+start].operator_fields.select_operator.exists_lower;
 //       exists_upper[i] = query_buffer[i+start].operator_fields.select_operator.exists_upper;
        lower[i] = query_buffer[i+start].operator_fields.select_operator.lower;
        upper[i] = query_buffer[i+start].operator_fields.select_operator.upper;
    }



    for(size_t i = 0; i < active->column_length; i++){
        int cur_data = active->data[i];
        for(size_t j = 0; j < num_share; j++){
            // if( (exists_upper[j] && exists_lower[j] && active->data[i] >= lower[j] && active->data[i] < upper[j]) || (exists_upper[j] && !exists_lower[j] && active->data[i] < upper[j]) ||  (!exists_upper[j] && exists_lower[j] && active->data[i] >= lower[j]) ){
            //     res[j].data[res[j].column_length++] = i;
            // }     

            if(cur_data >= lower[j] && cur_data < upper[j]){
                res[j].data[res[j].column_length++] = i;
            }       
        }
    }

    for(size_t i = 0; i < num_share; i++){
        Column* to_store = (Column*)malloc(sizeof(Column));
        memcpy(to_store, &res[i], sizeof(Column));
        to_store->data = realloc(to_store->data, sizeof(int)*to_store->column_length);
        to_store->column_max = to_store->column_length;
        store_client_variable(lvals[i], to_store);
    }

    return NULL;
}

void* join_worker(void* args){
    JoinArgs* real_args = (JoinArgs*) args;
    size_t start = real_args->start;
    size_t end = real_args->end;
    Column* outer_val =real_args->outer_val;
    Column* outer_pos =real_args->outer_pos;
    Column* inner_val =real_args->inner_val;
    Column* inner_pos =real_args->inner_pos;
    Column* outer_ret =real_args->outer_ret;
    Column* inner_ret =real_args->inner_ret;
    for(size_t i = start; i < end; i++){
        for(size_t j = 0; j < inner_val->column_length; j++){
            if(outer_val->data[i] == inner_val->data[j]){
                outer_ret->data[outer_ret->column_length++] = outer_pos->data[i];
                inner_ret->data[inner_ret->column_length++] = inner_pos->data[j];
            }
        }
    }
    return NULL;
}

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/

//Assume DSL is correct.
char* execute_DbOperator(DbOperator* query) {
    cs165_log(stdout, "New DB Operator got: Type: %i, fd: %i\n", query->type, query->client_fd);
    if(query->type == INSERT){
        Table* table = query->operator_fields.insert_operator.table;
        int* values = query->operator_fields.insert_operator.values;
        cs165_log(stdout,"our table is named %s\n", table->name);
        cs165_log(stdout, "Our table length is: %i\n", table->table_length);
        table->table_length++;
        for(size_t i = 0; i < table->col_count; i++){
            cs165_log(stdout,"Inserting: %i into col: %s\n", values[i], table->columns[i].name);
            if(table->table_length == 1){ //Do NOT realloc an array of size 0!
                table->columns[i].column_max = COL_INCREMENT;
                cs165_log(stdout, "ALLOCATING COL TO %i\n", COL_INCREMENT);
                table->columns[i].data = (int*) malloc(sizeof(int) * COL_INCREMENT);
            }else{
                cs165_log(stdout, "TABLE LENGTH IS %i, max column length is %i\n" , table->table_length , table->columns[i].column_max);
                if(table->table_length >= table->columns[i].column_max){
                    table->columns[i].column_max += COL_INCREMENT;
                    cs165_log(stdout, "REALLOCATING COL TO %i!\n", table->columns[i].column_max);
                    table->columns[i].data = (int*) realloc(table->columns[i].data, (sizeof(int) * (table->columns[i].column_max)));
                }
            }
            table->columns[i].data[table->table_length - 1] = values[i];
            table->columns[i].column_length ++;
            cs165_log(stdout,"inserted: %i into col: %s. This column now has %i elements\n", values[i], table->columns[i].name, table->columns[i].column_length);
        }
        db_operator_free(query);  
        return "Done";
    }
    if(query->type == SELECT){

        Column* column = query->operator_fields.select_operator.column;
        Column* assoc_pos = query->operator_fields.select_operator.assoc_pos;
        char* lval = query->operator_fields.select_operator.lval;
        bool exists_lower = query->operator_fields.select_operator.exists_lower;
        bool exists_upper = query->operator_fields.select_operator.exists_upper;
        int lower = query->operator_fields.select_operator.lower;
        int upper = query->operator_fields.select_operator.upper;

        if(!exists_upper && !exists_lower){
            db_operator_free(query);
            return "Error in execute_DbOperator: Neither upper nor lower specified";
        }

        cs165_log(stdout, "Select from column %s, store to %s", column->name, lval);
        if(exists_upper){
            cs165_log(stdout, ", where < %i", upper);
        }
        if(exists_lower){
            cs165_log(stdout, ", where >= %i", lower);
        }
        cs165_log(stdout, "\n");


        int* result = (int*) malloc(column->column_length * sizeof(int));
        size_t result_index = 0;
                    cs165_log(stdout,"Column length: %i\n", column->column_length);

        if(assoc_pos == NULL){
            for(size_t i = 0; i < column->column_length; i++){
                if( (exists_upper && exists_lower && column->data[i] >= lower && column->data[i] < upper) || (exists_upper && !exists_lower && column->data[i] < upper) ||  (!exists_upper && exists_lower && column->data[i] >= lower) ){
                    //cs165_log(stdout, "%i qualifies\n", column->data[i]);
                    result[result_index++] = i;
                }
            }
        } else {
            for(size_t i = 0; i < column->column_length; i++){
                if( (exists_upper && exists_lower && column->data[i] >= lower && column->data[i] < upper) || (exists_upper && !exists_lower && column->data[i] < upper) ||  (!exists_upper && exists_lower && column->data[i] >= lower) ){
                    //cs165_log(stdout, "%i qualifies\n", column->data[i]);
                    result[result_index++] = assoc_pos->data[i];
                }
            }            
        }

        cs165_log(stdout,"Number of hits: %i\n", result_index);

        result = (int*) realloc(result, sizeof(int) * result_index);

        Column* new_col = (Column*) malloc(sizeof(Column));
        strcpy(new_col->name, lval);
        new_col->data = result;
        new_col->column_length = result_index;
        new_col->type = INT;
        store_client_variable(lval, new_col);
        db_operator_free(query);
        return "Done";
    }


    if(query->type == RUN_BATCH){
        cs165_log(stdout, "Starting batch run..\n");
#ifdef NAIVE_BATCH
        for(size_t i = 0; i < query_buffer_count; i++){
            DbOperator* new_query = (DbOperator*) malloc(sizeof(DbOperator));
            memcpy(new_query, &(query_buffer[i]), sizeof(DbOperator));
            char* res = execute_DbOperator(new_query);
            cs165_log(stdout, "output from exec: %s\n", res);
        }
#endif
#ifdef MULTICORE_BATCH
        size_t cur_task = 0;
        while(cur_task < query_buffer_count){
            size_t needed_worker_threads = MIN(MAX_WORKER_THREADS, DIV_UP((query_buffer_count-cur_task),MAX_SHARE_PER_THREAD));

            pthread_t threads[needed_worker_threads];
            int args[needed_worker_threads][2];

            for(size_t i = 0; i < needed_worker_threads; i++){
                size_t tasks_to_do = MIN(MAX_SHARE_PER_THREAD, (query_buffer_count-cur_task));
                args[i][0] = cur_task;
                args[i][1] = cur_task+tasks_to_do;
                cs165_log(stdout, "Assigning to thread number %i, a range of %i to %i\n", i, args[i][0] , args[i][1]-1);

                if(pthread_create(&(threads[i]), NULL, share_scan, &(args[i]))){
                    log_err("FATAL: Cannot create thread!");
                    exit(1);
                }

                cur_task += tasks_to_do;
            }

            for(size_t i = 0; i < needed_worker_threads; i++){
                (void) pthread_join(threads[i], NULL);
            }

        }
#endif
        db_operator_free(query);  
        return "All Done";
    }

    if(query->type == JOIN){
        Column* pos1 = query->operator_fields.join_operator.pos1;
        Column* val1 = query->operator_fields.join_operator.val1;
        Column* pos2 = query->operator_fields.join_operator.pos2;
        Column* val2 = query->operator_fields.join_operator.val2;
        char* sto_pos1 = query->operator_fields.join_operator.sto_pos1;
        char* sto_pos2 = query->operator_fields.join_operator.sto_pos2;
        cs165_log(stdout, "val1 is %s, pos1 is %s, val2 is %s, pos2 is %s\n" ,val1->name,pos1->name, val2->name, pos2->name);
        cs165_log(stdout, "store 1 is %s, store 2 is %s\n", sto_pos1, sto_pos2);
        Column* res1 = (Column*) malloc(sizeof(Column));
        Column* res2 = (Column*) malloc(sizeof(Column));
        strcpy(res1->name, sto_pos1);
        strcpy(res2->name, sto_pos2);
        res1->column_length = 0;
        res2->column_length = 0;
        res1->type = INT;
        res2->type = INT;
        res1->index = NULL;
        res2->index = NULL;
        res1->column_max = MIN(pos1->column_length, pos2->column_length);
        res2->column_max = res1->column_max;
        res1->data = (int*) malloc(sizeof(int) * res1->column_max);
        res2->data = (int*) malloc(sizeof(int) * res2->column_max);

#ifdef NAIVE_JOIN
        for(size_t i = 0; i < val1->column_length; i++){
            for(size_t j = 0; j < val2->column_length; j++){
                if(val1->data[i] == val2->data[j]){
                    res1->data[res1->column_length++] = pos1->data[i];
                    res2->data[res2->column_length++] = pos2->data[j];
                }
            }
        }
#endif
#ifdef VECTOR_JOIN
        size_t v1cl = val1->column_length;
        size_t v2cl = val2->column_length;
        for(size_t vctr_strt = 0; vctr_strt < v1cl; vctr_strt += JOIN_VECTOR_SIZE){
            size_t upper_limit = MIN(v1cl, vctr_strt + JOIN_VECTOR_SIZE);
            for(size_t j = 0; j < v2cl; j++){
                for(size_t i = vctr_strt; i < upper_limit; i++){
                    if(val1->data[i] == val2->data[j]){
                        res1->data[res1->column_length++] = pos1->data[i];
                        res2->data[res2->column_length++] = pos2->data[j];
                    }                    
                }
            }

        }
#endif
#ifdef FOUR_THREAD_JOIN
        size_t first_start = 0;
        size_t third_start = val1->column_length / 2;
        size_t second_start = third_start / 2;
        size_t fourth_start = third_start + (val1->column_length-second_start)/2;
        cs165_log(stdout, "first: %i, second %i, third %i, fourth %i\n", first_start, second_start, third_start, fourth_start);

        Column first_res_outer;
        Column first_res_inner;
        Column second_res_outer;
        Column second_res_inner;
        Column third_res_outer;
        Column third_res_inner;
        Column fourt_res_outer;
        Column fourt_res_inner;
        memset(&first_res_outer, 0, sizeof(Column));
        memset(&first_res_inner, 0, sizeof(Column));
        memset(&second_res_outer, 0, sizeof(Column));
        memset(&second_res_inner, 0, sizeof(Column));
        memset(&third_res_outer, 0, sizeof(Column));
        memset(&third_res_inner, 0, sizeof(Column));
        memset(&fourt_res_outer, 0, sizeof(Column));
        memset(&fourt_res_inner, 0, sizeof(Column));
        first_res_outer.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,second_start-first_start)) );
        first_res_inner.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,second_start-first_start)) );
        second_res_outer.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,third_start-second_start)) );
        second_res_inner.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,third_start-second_start)) );
        third_res_outer.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,fourth_start-third_start)) );
        third_res_inner.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,fourth_start-third_start)) );
        fourt_res_outer.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,val1->column_length-fourth_start)) );
        fourt_res_inner.data = (int*) malloc(sizeof(int) * (MIN(val2->column_length,val1->column_length-fourth_start)) );

        JoinArgs args1 = {.start=first_start,.end=second_start,.outer_val=val1,.outer_pos=pos1,.inner_val=val2,.inner_pos=pos2,.outer_ret=&first_res_outer,.inner_ret=&first_res_inner,};
        JoinArgs args2 = {.start=second_start,.end=third_start,.outer_val=val1,.outer_pos=pos1,.inner_val=val2,.inner_pos=pos2,.outer_ret=&second_res_outer,.inner_ret=&second_res_inner,};
        JoinArgs args3 = {.start=third_start,.end=fourth_start,.outer_val=val1,.outer_pos=pos1,.inner_val=val2,.inner_pos=pos2,.outer_ret=&third_res_outer,.inner_ret=&third_res_inner,};
        JoinArgs args4 = {.start=fourth_start,.end=val1->column_length,.outer_val=val1,.outer_pos=pos1,.inner_val=val2,.inner_pos=pos2,.outer_ret=&fourt_res_outer,.inner_ret=&fourt_res_inner,};

        pthread_t threads[4];
        pthread_create(&(threads[0]), NULL, join_worker, &(args1));
        pthread_create(&(threads[1]), NULL, join_worker, &(args2));
        pthread_create(&(threads[2]), NULL, join_worker, &(args3));
        pthread_create(&(threads[3]), NULL, join_worker, &(args4));
        (void) pthread_join(threads[0], NULL);
        (void) pthread_join(threads[1], NULL);
        (void) pthread_join(threads[2], NULL);
        (void) pthread_join(threads[3], NULL);

        memcpy(res1->data + res1->column_length, first_res_outer.data, sizeof(int) * first_res_outer.column_length);
        res1->column_length+=first_res_outer.column_length;
        memcpy(res1->data + res1->column_length, second_res_outer.data, sizeof(int) * second_res_outer.column_length);
        res1->column_length+=second_res_outer.column_length;
        memcpy(res1->data + res1->column_length, third_res_outer.data, sizeof(int) * third_res_outer.column_length);
        res1->column_length+=third_res_outer.column_length;
        memcpy(res1->data + res1->column_length, fourt_res_outer.data, sizeof(int) * fourt_res_outer.column_length);
        res1->column_length+=fourt_res_outer.column_length;

        memcpy(res2->data + res2->column_length, first_res_inner.data, sizeof(int) * first_res_inner.column_length);
        res2->column_length+=first_res_inner.column_length;
        memcpy(res2->data + res2->column_length, second_res_inner.data, sizeof(int) * second_res_inner.column_length);
        res2->column_length+=second_res_inner.column_length;
        memcpy(res2->data + res2->column_length, third_res_inner.data, sizeof(int) * third_res_inner.column_length);
        res2->column_length+=third_res_inner.column_length;
        memcpy(res2->data + res2->column_length, fourt_res_inner.data, sizeof(int) * fourt_res_inner.column_length);
        res2->column_length+=fourt_res_inner.column_length;

        free(first_res_outer.data);
        free(first_res_inner.data);
        free(second_res_outer.data);
        free(second_res_inner.data);
        free(third_res_outer.data);
        free(third_res_inner.data);
        free(fourt_res_outer.data);
        free(fourt_res_inner.data);

#endif

        res1->data = realloc(res1->data, sizeof(int) * res1->column_length);
        res2->data = realloc(res2->data, sizeof(int) * res2->column_length);
        store_client_variable(sto_pos1, res1);
        store_client_variable(sto_pos2, res2);
        db_operator_free(query);
        return "Done";
    }

//     if(query->type == RUN_BATCH){
// #define DIV_UP(x,y) ((x + y - 1)/y)
// #define MIN(x, y) (((x) < (y)) ? (x) : (y))
//         cs165_log(stdout, "starting batch run\n");
//         for(size_t cur_vector = 0; cur_vector < query_buffer_count; cur_vector+=MAX_SHARE_PER_THREAD){
//             size_t start = cur_vector;
//             size_t end = MIN(cur_vector + MAX_SHARE_PER_THREAD ,query_buffer_count);
//             cs165_log(stdout, "Starts at %i, ends at %i\n", start, end-1);
//             // clock_t start_t = clock(), diff;
//             share_scan(start, end);
//             // diff = clock() - start_t;
//             // int msec = diff * 1000 / CLOCKS_PER_SEC;
//             // printf("batch:%dms\n", msec%1000);
//         }


//         return "All Done";
//     }

    db_operator_free(query);
    return "Error in execute_DbOperator";
}



char* acknowledge_and_add_DbOperator(DbOperator* query){
    if(query_buffer_count == DEFAULT_QUERY_BUFFER_SIZE){
        return "Query Queue full!! (oops!)";
    }
    if(query->type != SELECT){
        return "Query not a select operator!";
    }
    if(query_buffer_count > 1 && query->operator_fields.select_operator.column != query_buffer[0].operator_fields.select_operator.column){
        return "Query needs to reference same column!";
    }
    memcpy(&(query_buffer[query_buffer_count]), query, sizeof(DbOperator));

            // DbOperator* querya = &(query_buffer[query_buffer_count]);
            // Column* column = querya->operator_fields.select_operator.column;
            // char* lval = querya->operator_fields.select_operator.lval;
            // bool exists_lower = querya->operator_fields.select_operator.exists_lower;
            // bool exists_upper = querya->operator_fields.select_operator.exists_upper;
            // int lower = querya->operator_fields.select_operator.lower;
            // int upper = querya->operator_fields.select_operator.upper;

            // if(!exists_upper && !exists_lower){
            //     db_operator_free(querya);
            //     return "Error in execute_DbOperator: Neither upper nor lower specified";
            // }

            // cs165_log(stdout, "Select from column %s, store to %s, (%p)\n", column->name, lval, lval);
            // if(exists_upper){
            //     cs165_log(stdout, ", where < %i", upper);
            // }
            // if(exists_lower){
            //     cs165_log(stdout, ", where >= %i", lower);
            // }
            // cs165_log(stdout, "\n");

    query_buffer_count++;
    free(query); //Special case here: don't want to free the lval string referenced by query
    return "Query Queued";
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
bool handle_client(int client_socket) {
    bool keep_going = true;
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    client_context = (ClientContext*) malloc(sizeof(ClientContext));
    client_context->columns = NULL;
    client_context->col_count = 0;

    //init query queue
    memset(&query_buffer, 0 , sizeof(query_buffer));
    query_buffer_count = 0;
    query_capture_state = false;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            char* result;
            if(send_message.status == OK_WAIT_FOR_RESPONSE){
                // 2. Handle request
                if(query_capture_state){
                    result = acknowledge_and_add_DbOperator(query);
                } else {
                    // clock_t start_t = clock(), diff;
                    result = execute_DbOperator(query);
                    // diff = clock() - start_t;
                    // int msec = diff * 1000 / CLOCKS_PER_SEC;
                    // printf("%dms\n", msec);
                }
            }else if(send_message.status == SHUTTING_DOWN){
                keep_going = false;
                free(query);
                result = "";
                shutdown_server();
                done = 1;
            } else{
                free(query);
                result = "";
            }

            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            
            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
            if(send_message.status == OK_WAIT_FOR_RESPONSE){
                // 4. Send response of request
                if (send(client_socket, result, send_message.length, 0) == -1) {
                    log_err("Failed to send message.");
                    exit(1);
                }          
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    return keep_going;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

void shutdown_server(){
    if(current_db != NULL){
        Status sync_status = sync_db(current_db);
        if(sync_status.code != OK){
            log_err("ERROR IN SHUTDOWN SYNC: %s\n", sync_status.error_message); //Oh well.
        }
        shutdown_database(current_db);
    }

//Also shut down client context if any. Will need to do this recursively though.
}

void shutdown_database(Db* db){ //Closes out current database
    for(size_t i = 0; i < db->tables_size; i++){
        Table curr_tbl = db->tables[i];
        for(size_t j = 0; j < curr_tbl.col_count; j++){
            Column curr_col = curr_tbl.columns[j];
            free(curr_col.data);
        }
        free(curr_tbl.columns);
    }
    free(db->tables);
    free(db);
}

void db_operator_free(DbOperator* query){
    cs165_log(stdout,"Freeing db operator!!");
    switch(query->type){
        case INSERT:
            free(query->operator_fields.insert_operator.values);
            break;
        case SELECT:
            free(query->operator_fields.select_operator.lval);
            break;
        case JOIN:
            free(query->operator_fields.join_operator.sto_pos1);
            free(query->operator_fields.join_operator.sto_pos2);
            break;
        default:
            break;
    }
    free(query);
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }
    int client_socket = 0;

    do{
        if(client_context != NULL){
            for(size_t i = 0; i < client_context-> col_count; i++){
                if(client_context->columns[i].data != NULL){
                    free(client_context->columns[i].data );
                }
            }
            free(client_context->columns);
            free(client_context);
        }




        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }
    } while(handle_client(client_socket));
    return 0;
}
