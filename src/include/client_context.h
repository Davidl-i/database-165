#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"
#include "message.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

ClientContext* client_context;

DbOperator query_buffer[DEFAULT_QUERY_BUFFER_SIZE];
size_t query_buffer_count;
bool query_capture_state;


Table* lookup_table(const char* name);
Column* lookup_column(const char* name);
Column* lookup_client_context(const char* name);
message_status serve_print(char* command, int client_socket);
void store_client_variable(const char* name, Column* col);
#endif
