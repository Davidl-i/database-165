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

#define MAX_WORKER_THREADS  4
#define MAX_SHARE_PER_THREAD 10
#define RES_START_SIZE 1024     //meant only for dynamically sized results
#define RES_MULTIPLE 500        //only for dynamically sized results


void* select_worker(void* args){
    size_t start = ((int*)args)[0];
    size_t end = ((int*)args)[1];
    size_t num_share = end-start;
    cs165_log(stdout, "I am in charge of doing %i through %i!\n", start, end-1);
    Column* active = query_buffer[start].operator_fields.select_operator.column;
    Column res[num_share];
    bool exists_lower[num_share];
    bool exists_upper[num_share];
    int lower[num_share];
    int upper[num_share];
    char* lvals[num_share];
    for(size_t i = 0; i < num_share; i++){
        lvals[i] = query_buffer[i+start].operator_fields.select_operator.lval;
        res[i].column_max = RES_START_SIZE;
        res[i].column_length = 0;
        //res[i].data = (int*) malloc(sizeof(int)*RES_START_SIZE);
        res[i].data = (int*) malloc(sizeof(int)*active->column_length);
        strcpy(res[i].name, lvals[i]);
        res[i].type = INT;
        exists_lower[i] = query_buffer[i+start].operator_fields.select_operator.exists_lower;
        exists_upper[i] = query_buffer[i+start].operator_fields.select_operator.exists_upper;
        lower[i] = query_buffer[i+start].operator_fields.select_operator.lower;
        upper[i] = query_buffer[i+start].operator_fields.select_operator.upper;
    }


    for(size_t i = 0; i < active->column_length; i++){
        for(size_t j = 0; j < num_share; j++){
            if( (exists_upper[j] && exists_lower[j] && active->data[i] >= lower[j] && active->data[i] < upper[j]) || (exists_upper[j] && !exists_lower[j] && active->data[i] < upper[j]) ||  (!exists_upper[j] && exists_lower[j] && active->data[i] >= lower[j]) ){
                res[j].data[res[j].column_length++] = i;
                // if(res[j].column_length == res[j].column_max){
                //     res[j].data = realloc(res[j].data, sizeof(int) * (RES_MULTIPLE * res[j].column_max));
                //     res[j].column_max = res[j].column_max * RES_MULTIPLE;
                // }
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
        res[i].column_max = RES_START_SIZE;
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
// #ifdef LOG //Don't waste time if not logging
//         for(size_t i = 0; i < table->col_count; i++){
//             cs165_log(stdout, "Col: %s\n", table->columns[i].name);
//             for(size_t j = 0; j < table->table_length; j++){
//                 cs165_log(stdout, "%i\n", table->columns[i].data[j]);
//             }
//         }
// #endif
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
        // for(size_t i = 0; i < query_buffer_count; i++){
        //     DbOperator* new_query = (DbOperator*) malloc(sizeof(DbOperator));
        //     memcpy(new_query, &(query_buffer[i]), sizeof(DbOperator));
        //     char* res = execute_DbOperator(new_query);
        //     cs165_log(stdout, "output from exec: %s\n", res);
        // }



#define DIV_UP(x,y) ((x + y - 1)/y)
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

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

        db_operator_free(query);  
        return "All Done";
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
