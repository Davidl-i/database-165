#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"
#include "message.h"
ClientContext* client_context;

Table* lookup_table(const char* name);
Column* lookup_column(const char* name);
Column* lookup_client_context(const char* name);
message_status serve_print(char* command, int client_socket);
#endif
