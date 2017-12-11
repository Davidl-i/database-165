#define _BSD_SOURCE
#include "client_context.h"
#include "utils.h"
#include "message.h"
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

Table* lookup_table(const char* name) {
	char* table_req = (char*) malloc(sizeof(char) * (1 + strlen(name)));
	char* to_free = table_req;
	strcpy(table_req, name);
	char* table_name_token;
	char* token = strsep(&table_req, ".");
	if(token == NULL){
		log_err("Name is null!\n");
		free(to_free); //Even though it's null, we're still allocating at least 1 byte
		return NULL;
	}
	char* token2 = strsep(&table_req, ".");
	// char* token3 = strsep(&table_req, ".");
	// if(token3 != NULL){
	// 	log_err("Name invalid! (Too long)\n");                /*Not checking name too long for now*/
	// 	free(to_free);
	// 	return NULL;
	// }
	if(token2 == NULL){
		if(current_db == NULL){
			log_err("lookup table with implicit db, but db not loaded\n");
			free(to_free);
			return NULL;
		}
		table_name_token = token;
	}else{
		cs165_log(stdout,"Going to do add_Db\n");
		Status add_status = add_db(token, false);
		if(add_status.code != OK){
			log_err("Error when trying to import %s database: %s\n", token, add_status.error_message);
			free(to_free);
			return NULL;
		}
		table_name_token = token2;
	}

    for(size_t i = 0; i < current_db->tables_size; i++){
    	Table* cur_table = current_db->tables + i;
    	if(strcmp(cur_table->name, table_name_token) == 0){
    		free(to_free);
    		return cur_table;
    	}
    }

    log_err("Requested table does not exist!\n");
    free(to_free);
	return NULL;
}

Column* lookup_column(const char* name){
	char* col_req = (char*) malloc(sizeof(char) * (1 + strlen(name)));
	char* to_free = col_req;
	char* col_name_token;
	strcpy(col_req, name);
	char* token = strsep(&col_req, ".");
	if(token == NULL){
		log_err("Name is null!\n");
		free(to_free);
		return NULL;
	}
	char* token2 = strsep(&col_req, ".");
	if(token2 == NULL){
		log_err("Name too short (Column underspecified)!\n");
		free(to_free);
		return NULL;
	}
	char* token3 = strsep(&col_req, ".");
	char* token4 = strsep(&col_req, ".");
	if(token4 != NULL){
		log_err("Name invalid! (Too long)\n");
		free(to_free);
		return NULL;
	}
	char* table_to_find;
	if(token3 == NULL){ //assumes database is current_db
		table_to_find = (char*) malloc(sizeof(char) * (strlen(current_db->name) + strlen(token) + 2 )); //<currentdb name> . <token> \0
		strcpy(table_to_find, current_db->name);
		strcat(table_to_find, ".");
		strcat(table_to_find, token);
		col_name_token = token2;
	}else{ //Fully specified
		table_to_find = (char*) malloc(sizeof(char) * (strlen(token) + strlen(token2) + 2));
		strcpy(table_to_find, token);
		strcat(table_to_find, ".");
		strcat(table_to_find, token2);
		col_name_token = token3;
	}
	Table* resulting_table = lookup_table(table_to_find);
	if(resulting_table == NULL){
		log_err("Table %s not found!\n", table_to_find);
		free(table_to_find);
		free(to_free);
		return NULL;
	}
	free(table_to_find);
	//cs165_log(stdout, "INFO: There are a total of %i cols \n", resulting_table->num_loaded_cols);
    for(size_t i = 0; i < resulting_table->num_loaded_cols; i++){
    	Column* cur_col = resulting_table->columns + i;
    	if(strcmp(cur_col->name, col_name_token) == 0){
    		free(to_free);
    		return cur_col;
    	}
    }
    free(to_free);
    log_err("Column not found!\n");
    return NULL;
}

Column* lookup_client_context(const char* name){
    for(size_t i = 0; i < client_context->col_count; i++){
        if(strcmp(client_context->columns[i].name, name) == 0){
        	return &(client_context->columns[i]);
        }
    }
    return NULL;
}

void store_client_variable(const char* name, Column* col){
	ssize_t element_to_insert = -1;
	for(size_t i = 0; i < client_context->col_count; i++){
        if(strcmp(client_context->columns[i].name, name) == 0){
        	free(client_context->columns[i].data);
        	//free(&(client_context->columns[i]));
        	element_to_insert = i;
        	goto insert_stage;
        }
    }
    if(client_context->col_count == 0){
        client_context->columns = (Column*) malloc(sizeof(Column));
        client_context->col_count++;
    }else{
        client_context->columns = (Column*) realloc(client_context->columns, sizeof(Column) * (client_context->col_count + 1));
        client_context->col_count++;
    }
    element_to_insert = client_context->col_count - 1;
insert_stage:                           
    cs165_log(stdout,"Memcpying result to variable %s, the %ith element\n", name, element_to_insert);
    memcpy( &client_context->columns[element_to_insert], col, sizeof(Column));
    //client_context->columns[element_to_insert] = *col;
}

message_status serve_print(char* command, int client_socket){
    char* cmd_parse = trim_whitespace(command);
    cs165_log(stdout, "command = %s\n", command );
    //cmd_parse += 5;
    if (strncmp(cmd_parse, "(", 1) == 0) {
        cmd_parse++;
    }else{
        return INCORRECT_FORMAT;
    }
    int last_char = strlen(cmd_parse) - 1;
    if (last_char < 0 || cmd_parse[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    cmd_parse[last_char] = '\0';
    cs165_log(stdout, "Printing: %s\n", cmd_parse);
    Column* target = lookup_client_context(cmd_parse);
    if(target == NULL){
    	return OBJECT_NOT_FOUND;
    }

    message send_message;
    message recv_message;
    print_packet send_packet;
    size_t cur_pos = 0;
    size_t length;

    do{
    	size_t send_length = ((target->column_length - cur_pos) > MAX_PAYLOAD_SIZE) ? MAX_PAYLOAD_SIZE: (target->column_length - cur_pos);
    	send_packet.length = send_length;
    	send_packet.final = ( (cur_pos + send_length) == target->column_length );
    	memset(&(send_packet.payload), 0, MAX_PAYLOAD_SIZE);
    	memcpy(&(send_packet.payload), (target->data) + cur_pos, sizeof(int)*send_length); //pointer arithmetic
    	send_message.status = OK_WAIT_FOR_RESPONSE;
    	send_message.length = sizeof(send_packet);
    	send_message.payload = NULL;
    	// for(size_t i = 0; i < send_packet.length; i++){
    	// 	printf("num = %i\n", send_packet.payload[i]);
    	// }

    	cs165_log(stdout, "Currently sending %i ints to print\n", send_length);
	    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
            log_err("Failed to send message.");
            exit(1);
        }
        if (send(client_socket, &send_packet, send_message.length, 0) == -1) {
            log_err("Failed to send message.");
            exit(1);
        }
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if(length == 0){
            log_err("Failed to receive message.");
            exit(1);        	
        }
        cs165_log(stdout, "recv length: %i\n", recv_message.length);
        char recv_buffer[recv_message.length + 1];
        length = recv(client_socket, recv_buffer, recv_message.length,0);
        recv_buffer[recv_message.length]  = '\0';
        cs165_log(stdout, "Received: %s", recv_buffer);
        if(strcmp(recv_buffer, "OK") != 0){
        	log_err("Corrupt message received during print!");
            exit(1);  
        }
        cur_pos += send_length;
    }while(cur_pos < target->column_length);
    return OK_DONE;
}