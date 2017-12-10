#define _BSD_SOURCE
#include "client_context.h"
#include "utils.h"
#include <string.h>

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