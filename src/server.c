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

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024


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
                table->columns[i].data = (int*) malloc(sizeof(int) * 1);
            }else{
                table->columns[i].data = (int*) realloc(table->columns[i].data, (sizeof(int) * table->table_length));
            }
            table->columns[i].data[table->table_length - 1] = values[i];
            table->columns[i].column_length ++;
            cs165_log(stdout,"inserted: %i into col: %s. This column now has %i elements\n", values[i], table->columns[i].name, table->columns[i].column_length);
        }
#ifdef LOG //Don't waste time if not logging
        for(size_t i = 0; i < table->col_count; i++){
            cs165_log(stdout, "Col: %s\n", table->columns[i].name);
            for(size_t j = 0; j < table->table_length; j++){
                cs165_log(stdout, "%i\n", table->columns[i].data[j]);
            }
        }
#endif
        free(query);
        return "Done";
    }
    if(query->type == SELECT){

        Column* column = query->operator_fields.select_operator.column;
        char* lval = query->operator_fields.select_operator.lval;
        bool exists_lower = query->operator_fields.select_operator.exists_lower;
        bool exists_upper = query->operator_fields.select_operator.exists_upper;
        int lower = query->operator_fields.select_operator.lower;
        int upper = query->operator_fields.select_operator.upper;

        int* result = (int*) malloc(column->column_length * sizeof(int));
        size_t result_index = 0;
                    cs165_log(stdout,"%i\n", column->column_length);

        for(size_t i = 0; i < column->column_length; i++){
            if( (exists_upper && exists_lower && column->data[i] >= lower && column->data[i] < upper) || (exists_upper && !exists_lower && column->data[i] >= upper) ||  (!exists_upper && exists_lower && column->data[i] < lower) ||  (!exists_lower && !exists_upper) ){
                cs165_log(stdout, "%i qualifies\n", column->data[i]);
                result[result_index++] = i;
            }
        }
                            cs165_log(stdout,"%i\n", result_index);

        result = (int*) realloc(result, sizeof(int) * result_index);

        Column* new_col = (Column*) malloc(sizeof(Column));
        strcpy(new_col->name, lval);
        new_col->data = result;
        new_col->column_length = result_index;

        if(client_context->col_count == 0){
            client_context->columns = (Column*) malloc(sizeof(Column));
            client_context->col_count++;
        }else{
            client_context->columns = (Column*) realloc(client_context->columns, sizeof(Column) * (client_context->col_count + 1));
            client_context->col_count++;
        }
                            cs165_log(stdout,"Memcpying to the %ith element\n", client_context->col_count - 1);

        memcpy( &client_context->columns[client_context->col_count - 1], new_col, sizeof(Column));

        free(query);
        return "Done";
    }

    free(query);
    return "Error in execute_DbOperator";
}



/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
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
                result = execute_DbOperator(query);      
            }else if(send_message.status == SHUTTING_DOWN){
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

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}
