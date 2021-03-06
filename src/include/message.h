#ifndef MESSAGE_H__
#define MESSAGE_H__
#include "cs165_api.h"

#define FILE_BUF_SIZE 500
#define MAX_PAYLOAD_SIZE 8000

// mesage_status defines the status of the previous request.
// FEEL FREE TO ADD YOUR OWN OR REMOVE ANY THAT ARE UNUSED IN YOUR PROJECT
typedef enum message_status {
    OK_DONE,
    OK_WAIT_FOR_RESPONSE,
    UNKNOWN_COMMAND,
    QUERY_UNSUPPORTED,
    OBJECT_ALREADY_EXISTS,
    OBJECT_NOT_FOUND,
    INCORRECT_FORMAT, 
    EXECUTION_ERROR,
    INCORRECT_FILE_FORMAT,
    FILE_NOT_FOUND,
    INDEX_ALREADY_EXISTS,
    SHUTTING_DOWN,
} message_status;

extern const char* MESSAGE_EXPLANATION[];

// message is a single packet of information sent between client/server.
// message_status: defines the status of the message.
// length: defines the length of the string message to be sent.
// payload: defines the payload of the message.
typedef struct message {
    message_status status;
    int length;
    char* payload;
} message;

typedef struct print_packet{
    int payload[MAX_PAYLOAD_SIZE];
    size_t length;
    bool final;
    DataType type;
} print_packet;

#endif
