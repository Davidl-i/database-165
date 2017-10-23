/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

const char* MESSAGE_EXPLANATION[] = { "OK_DONE", "OK_WAIT_FOR_RESPONSE", "UNKNOWN_COMMAND", "QUERY_UNSUPPORTED", "OBJECT_ALREADY_EXISTS", "OBJECT_NOT_FOUND", "INCORRECT_FORMAT", "EXECUTION_ERROR", "INCORRECT_FILE_FORMAT", "FILE_NOT_FOUND", "INDEX_ALREADY_EXISTS"};
/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

char* get_file(char* cmd){
    cs165_log(stdout, "cmd: %s\n", cmd);
    char* cmd_parse = trim_whitespace(cmd);
    cmd_parse += 4;
    if (strncmp(cmd_parse, "(", 1) == 0) {
        cmd_parse++;
    }else{
        return NULL;
    }
    cmd_parse = trim_quotes(cmd_parse);
    int last_char = strlen(cmd_parse) - 1;
    if (last_char < 0 || cmd_parse[last_char] != ')') {
        return NULL;
    }
    cmd_parse[last_char] = '\0';
    cs165_log(stdout, "Getting File: %s\n", cmd_parse);
    int fd = open(cmd_parse, O_RDONLY);
    if(fd < 0){
        log_err("Problem opening file %s\n", cmd_parse);
        return NULL;
    }
    size_t filesize = lseek(fd, 0, SEEK_END); lseek(fd, 0, SEEK_SET);
    char* header = "load:\n";
    char* output = (char*) calloc((strlen(header) + filesize + 1),sizeof(char));
    strcpy(output, header);
    int r_out = read(fd, output + strlen(header), filesize);
    if(r_out < 0){
        free(output);
        close(fd);
        return NULL;
    }
    cs165_log(stdout, "put %i bytes into the packet\n", r_out);
    close(fd);
    return output;
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        if(send_message.payload != read_buffer && send_message.payload != NULL){    
            cs165_log(stdout, "Freeing size of %i\n", strlen(send_message.payload));
            free(send_message.payload);
        }
        send_message.payload = read_buffer;
        if (strncmp(read_buffer, "load", 4) == 0) {
            send_message.payload = get_file(read_buffer);
            if(send_message.payload == NULL){
                log_err("Wrong load format!\n");
                continue;
            }
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(send_message.payload);
        if (send_message.length > 1) {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                cs165_log(stdout, "Received status code is %s\n", MESSAGE_EXPLANATION[recv_message.status]);
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE) && (int) recv_message.length > 0) {
                    cs165_log(stdout, "|____");
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];
                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
                        cs165_log(stdout, "Response: %s\n", payload);  
                    }
                }
            } else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                else {
		            log_info("Server closed connection\n");
		        }
                exit(1);
            }
        }
        cs165_log(stdout, "Looping back \n");
    }
    close(client_socket);
    return 0;
}
