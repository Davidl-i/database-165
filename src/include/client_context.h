#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(const char* name);
Column* lookup_column(const char* name);

#endif
