/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE
#define _BSD_SOURCE

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
#include <stdbool.h>


#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"
#include "cs165_api.h"


#define DEFAULT_STDIN_BUFFER_SIZE 1024

const char* MESSAGE_EXPLANATION[] = { "OK_DONE", "OK_WAIT_FOR_RESPONSE", "UNKNOWN_COMMAND", "QUERY_UNSUPPORTED", "OBJECT_ALREADY_EXISTS", "OBJECT_NOT_FOUND", "INCORRECT_FORMAT", "EXECUTION_ERROR", "INCORRECT_FILE_FORMAT", "FILE_NOT_FOUND", "INDEX_ALREADY_EXISTS", "SHUTTING_DOWN"};
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

Column* print_one(char* varname, int client_socket){
    ssize_t len;
    message send_message;
    message recv_message;
    print_packet recv_packet;
    char send_payload[MAX_PAYLOAD_SIZE];
    sprintf(send_payload, "print(%s)", varname); //Syntax verified on client side.
    send_message.payload = send_payload;
    send_message.length = strlen(send_payload);
    send_message.status = OK_DONE;

    Column* ret = (Column*) malloc(sizeof(Column));
    strcpy(ret->name, varname);
    ret->data = NULL;
    ret->column_length = 0;
    ret->index = NULL;

    do{
        cs165_log(stdout, "payload = %s, length  = %i\n", send_message.payload, send_message.length);
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
            if ((recv_message.status == OK_WAIT_FOR_RESPONSE) && (int) recv_message.length == sizeof(print_packet)) {
                if ((len = recv(client_socket, &recv_packet, sizeof(print_packet), 0)) > 0) {
                    // if(recv_packet.type == INT){
                    //     for(size_t i = 0; i < recv_packet.length; i++){
                    //         printf("%i\n", recv_packet.payload[i]);
                    //     }
                    // }
                    // if(recv_packet.type == LONG){
                    //     for(size_t i = 0; i < recv_packet.length; i+= sizeof(long)/sizeof(int)){ //assuming doubles take up n*sizeof(int) bytes
                    //         long val;
                    //         memcpy(&val, &(recv_packet.payload[i]), sizeof(long));
                    //         printf("%ld\n", val);
                    //     } 
                    // }
                    // if(recv_packet.type == FLOAT){
                    //     for(size_t i = 0; i < recv_packet.length; i+= sizeof(double)/sizeof(int)){ //assuming doubles take up n*sizeof(int) bytes
                    //         double val;
                    //         memcpy(&val, &(recv_packet.payload[i]), sizeof(double));
                    //         printf("%.2f\n", val);
                    //     } 
                    // }

                    if(ret->data == NULL){
                        ret->data = (int*)malloc(recv_packet.length * sizeof(int));
                    } else{
                        ret->data = (int*)realloc(ret->data, (ret->column_length + recv_packet.length) * sizeof(int));
                    }

                    memcpy(ret->data + (sizeof(int) * ret->column_length), recv_packet.payload, sizeof(int)*recv_packet.length);
                    ret->column_length += recv_packet.length;
                    ret->type = recv_packet.type;

                   // printf("Final packet? %i\n", recv_packet.final);
                    sprintf(send_payload, "OK");
                    send_message.length = strlen(send_payload);
                } else {
                    log_err("Print receive: Failed to receive anything intelligible.");
                    exit(1);     
                }
            } else {
                log_err("Error from server: %s\n", MESSAGE_EXPLANATION[recv_message.status]);
                return NULL;
            }
        } else{
            log_err("Print: Failed to receive anything intelligible.");
            exit(1);        
        } 
    } while(! recv_packet.final);


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

    if((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0){                               //from processing query
        cs165_log(stdout, "Received status code is %s\n", MESSAGE_EXPLANATION[recv_message.status]);
    }
    return ret;
}

Status print_handler(char* cmd, int client_socket){
    Status ret;
    ret.code = OK;
    cs165_log(stdout, "cmd: %s\n", cmd);
    char* cmd_parse = trim_whitespace(cmd);
    cmd_parse += 5;
    if (strncmp(cmd_parse, "(", 1) == 0) {
        cmd_parse++;
    }else{
        ret.code = ERROR;
        ret.error_message = "Print: invalid syntax!";
        return ret;
    }
    int last_char = strlen(cmd_parse) - 1;
    if (last_char < 0 || cmd_parse[last_char] != ')') {
        ret.code = ERROR;
        ret.error_message = "Print: invalid syntax )!";
        return ret;
    }
    cmd_parse[last_char] = '\0';
    char* token;
    char** tokenizer = &cmd_parse;
    char* backup = (char*) malloc(strlen(cmd_parse) + 1);
    strcpy(backup, cmd_parse);

    size_t print_width = 0;
    while ((token = strsep(tokenizer, ",")) != NULL) {
        //cs165_log(stdout, "Printing: %s\n", token);
        print_width ++;
    }
    cs165_log(stdout, "Print width %i\n", print_width);
    Column* print_cols[print_width];
    size_t cur_ptrs[print_width];
    memset(print_cols, 0, sizeof(print_cols));
    memset(cur_ptrs, 0, sizeof(cur_ptrs));
    tokenizer = &backup;

    size_t cur_col = 0;
    while ((token = strsep(tokenizer, ",")) != NULL) {
        Column* col = print_one(token, client_socket);
        if(col == NULL){
            ret.code = ERROR;
            ret.error_message = "Error in communicating with server while printing!";
            goto print_cleanup;
        }
        print_cols[cur_col] = col;
        cur_col++;
    }

    bool halt_print = false;
    long val_long;
    double val_dbl;
    do{
        for(size_t i = 0; i < print_width; i++){
            //cs165_log(stdout, "curptr = %i\n", cur_ptrs[i]);
            switch(print_cols[i]->type){
                case INT:
                    printf("%i", print_cols[i]->data[cur_ptrs[i]]);
                    cur_ptrs[i] += 1;
                    break;
                case LONG:
                    memcpy(&val_long, &(print_cols[i]->data[cur_ptrs[i]]), sizeof(long));
                    printf("%ld", val_long);
                    cur_ptrs[i] += sizeof(long) / sizeof(int);
                    break;
                case FLOAT:
                    memcpy(&val_dbl, &(print_cols[i]->data[cur_ptrs[i]]), sizeof(double));
                    printf("%.2f", val_dbl);
                    cur_ptrs[i] += sizeof(double) / sizeof(int);
                    break; 
            }
            if(i != print_width-1){
                printf(",");
            }
            if(cur_ptrs[i] >= print_cols[i]->column_length){
                halt_print = true;
            }
        }
        printf("\n");
    } while(! halt_print);





    // Column* print_col = print_one(cmd_parse, client_socket);
    // if(print_col->type == INT){
    //     for(size_t i = 0; i < print_col->column_length; i++){
    //         printf("%i\n", print_col->data[i]);
    //     }
    // }
    // if(print_col->type == LONG){
    //     for(size_t i = 0; i < print_col->column_length; i+= sizeof(long)/sizeof(int)){ //assuming doubles take up n*sizeof(int) bytes
    //         long val;
    //         memcpy(&val, &(print_col->data[i]), sizeof(long));
    //         printf("%ld\n", val);
    //     } 
    // }
    // if(print_col->type == FLOAT){
    //     for(size_t i = 0; i < print_col->column_length; i+= sizeof(double)/sizeof(int)){ //assuming doubles take up n*sizeof(int) bytes
    //         double val;
    //         memcpy(&val, &(print_col->data[i]), sizeof(double));
    //         printf("%.2f\n", val);
    //     } 
    // }  
    // free(print_col->data);
    // free(print_col);  
print_cleanup:
    if(backup != NULL){
        free(backup);
    }
    for(size_t i = 0; i < print_width; i++){
        if(print_cols[i] != NULL){
            free(print_cols[i]->data);
            free(print_cols[i]);            
        }
    }
    return ret;
}

//Stateless sending to server.
Status send_file_to_server(char* cmd, int client_socket){
    Status ret;
    ret.code = OK;
    cs165_log(stdout, "cmd: %s\n", cmd);
    char* cmd_parse = trim_whitespace(cmd);
    cmd_parse += 4;
    if (strncmp(cmd_parse, "(", 1) == 0) {
        cmd_parse++;
    }else{
        ret.code = ERROR;
        ret.error_message = "Load: invalid syntax!";
        return ret;
    }
    cmd_parse = trim_quotes(cmd_parse);
    int last_char = strlen(cmd_parse) - 1;
    if (last_char < 0 || cmd_parse[last_char] != ')') {
        ret.code = ERROR;
        ret.error_message = "Load: invalid syntax )!";
        return ret;
    }
    cmd_parse[last_char] = '\0';
    cs165_log(stdout, "Getting File: %s\n", cmd_parse);
    FILE* fd = fopen(cmd_parse, "r");
    if(fd == NULL){
        log_err("Problem opening file %s\n", cmd_parse);
        ret.code = IO_ERROR;
        ret.error_message = "Load: cannot open client-side file!";
        return ret;
    }
    char file_header [FILE_BUF_SIZE];
    char linebuf [FILE_BUF_SIZE];
    char payload [MAX_PAYLOAD_SIZE]; 
    while((fgets(linebuf, sizeof(linebuf), fd) != NULL) & (strlen(linebuf) < 2));
    strcpy(file_header, linebuf);
    cs165_log(stdout, "load header: %s", file_header);
    strcpy(linebuf, "");
    message send_message;
    message recv_message;
    char* eof;
    ssize_t len;
    do{
        sprintf(payload, "load:\n%s%s", file_header, linebuf);
        while((eof = fgets(linebuf, sizeof(linebuf), fd))){
            if(strlen(linebuf) < 2){
                continue;
            }
            if(strlen(linebuf) + strlen(payload) >= MAX_PAYLOAD_SIZE){
                break;
            }
            strcat(payload, linebuf);
        }
        send_message.payload = payload;
        send_message.length = strlen(payload);
        if(send_message.length > 1){
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
            //receive code.
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                if (recv_message.status != OK_DONE) {
                    ret.code = ERROR;
                    ret.error_message = "Bad response or error from server!";
                    return ret;
                }

            }

        }
    }while(eof != NULL);
    fclose(fd);
    return ret;
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
            // send_message.payload = get_file(read_buffer);
            // if(send_message.payload == NULL){
            //     log_err("Wrong load format!\n");
            //     continue;
            // }
            Status out = send_file_to_server(read_buffer, client_socket);
            if(out.code != OK){
                log_err("FATAL client-side error on load: %s. Will shut down client\n", out.error_message);
                exit(1); //FATAL error if you can't even load a file!
            }
            send_message.payload = NULL;
        }
        if (strncmp(read_buffer, "print", 5) == 0) {
            Status out = print_handler(read_buffer, client_socket);
            if(out.code != OK){
                log_err("Error in Printing! %s\n", out.error_message);
            }
            send_message.payload = NULL;
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = (send_message.payload == NULL)? 0 : strlen(send_message.payload); //"undefined behavior"
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
                if(recv_message.status == SHUTTING_DOWN){
                    break;
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
