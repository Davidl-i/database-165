/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

void parse_fetch(char* args, char* leftvar){
    message_status status = OK_WAIT_FOR_RESPONSE;

    trim_parenthesis(args);
        char** args_index = &args;

    char* colvar = next_token(args_index, &status);
    char* colpos = next_token(args_index, &status);

    Column* vars = lookup_column(colvar);
    Column positions;
    for(int i = 0; i < client_context->col_count; i++){
        if(strcmp(client_context->columns[i].name, colpos )==0){
            positions = client_context->columns[i];
            break;
        }
    }

    int* result = (int*) malloc(positions.column_length * sizeof(int));
    for(int i = 0; i < positions.column_length; i++){
        result[i] = vars->data[positions.data[i]];
    }
size_t result_index = positions.column_length;

        Column* new_col = (Column*) malloc(sizeof(Column));
        strcpy(new_col->name, leftvar);
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



}



message_status parse_select(char* args, char* leftvar, DbOperator* dbo){ //For now, only supports 3-arg selects!! Needs to be updated to support either
    message_status status = OK_WAIT_FOR_RESPONSE;
    if(leftvar == NULL){
        status = INCORRECT_FORMAT;
        return status;
    }
    if(dbo == NULL){
        status = EXECUTION_ERROR;
        return status;
    }
        trim_parenthesis(args);

    char** args_index = &args;
    char* colname = next_token(args_index, &status);
    char* highval = next_token(args_index, &status);
    char* lowval = next_token(args_index, &status);
    char* overflow = strsep(args_index, ",");
    if(overflow != NULL){
       status = INCORRECT_FORMAT; 
    }
    if(status == INCORRECT_FORMAT){
        return status;
    }
    Column* target_column = lookup_column(colname);
    if(target_column == NULL){
        status = OBJECT_NOT_FOUND;
        return status;
    }
    dbo->operator_fields.select_operator.column = target_column;
    dbo->operator_fields.select_operator.lval = leftvar;
    bool e_lower = (strcmp(lowval, "null") != 0);
    bool e_upper = (strcmp(highval, "null") != 0);
    dbo->operator_fields.select_operator.exists_lower = e_lower;
    dbo->operator_fields.select_operator.exists_upper = e_upper;
    dbo->operator_fields.select_operator.lower = (e_lower ? atoi(lowval): 0 );
    dbo->operator_fields.select_operator.upper = (e_upper ? atoi(highval): 0);

    return status;
}

message_status parse_create_col(char* create_arguments) {
    cs165_log(stdout, "Parsing CreateCol = %s\n", create_arguments);
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* where_put = next_token(create_arguments_index, &status);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }
    col_name = trim_quotes(col_name);

    int last_char = strlen(where_put) - 1;
    if (where_put[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    where_put[last_char] = '\0';
    Table* target;
    target = lookup_table(where_put);
    if(target == NULL){
        log_err("Targeted table not found! \n");
        return OBJECT_NOT_FOUND;
    }
    cs165_log(stdout, "Object was found! \n");

    Status create_status;
    create_column(col_name, target, false, &create_status);  
    if (create_status.code != OK) {
        log_err("Adding a column failed: %s\n", create_status.error_message);
        return EXECUTION_ERROR;
    }
    return status;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

message_status parse_create_tbl(char* create_arguments) {
    cs165_log(stdout, "Parsing CreateTBL = %s\n", create_arguments);
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    // if (current_db == NULL) {
    //     log_err("Can't create a table without having loaded a DB!\n");
    //     return EXECUTION_ERROR;
    // }
    // // check that the database argument is the current active database
    // if (strcmp(current_db->name, db_name) != 0) {                                        //TODO: support multiple databases
    //     log_err("query unsupported. Bad db name\n");
    //     return QUERY_UNSUPPORTED;
    // }
    Status res = add_db(db_name, false);
    if(res.code != OK){
        log_err("Error from add_db: %s\n", res.error_message);
        return OBJECT_NOT_FOUND;
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
    create_table(current_db, table_name, column_cnt, &create_status);
    if (create_status.code != OK) {
        log_err("Adding a table failed: %s\n", create_status.error_message);
        return EXECUTION_ERROR;
    }
    return status;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

message_status parse_create_db(char* create_arguments) {
    cs165_log(stdout, "Parsing CreateDB = %s\n", create_arguments);
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        // replace final ')' with null-termination character. 
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }
        Status create_status = add_db(db_name, true);
        if (create_status.code == OK) {
            return OK_DONE;
        } else {
            log_err("Error encountered: %s\n", create_status.error_message);
            return EXECUTION_ERROR;
        }
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
message_status parse_create(char* create_arguments) {
    cs165_log(stdout, "Parsing Create = %s\n", create_arguments);
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return mes_status;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                mes_status = parse_create_col(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

message_status load_from_client(char* input){
    char* token = NULL;
    char** input_index = &input;
    token = strsep(input_index, "\n");
    if(token == NULL){
        log_err("Load contents are null!\n");
        return INCORRECT_FILE_FORMAT;
    }
    if(strlen(token) <= 1){
        cs165_log(stdout, "advancing 1 token\n");
        token = strsep(input_index, "\n");
    }
    if(token == NULL || strlen(token) <= 1){
        log_err("Cannot find header!\n");
        return INCORRECT_FORMAT;
    }

    cs165_log(stdout, "Header is %s\n", token);

    char* header_copy = (char*) malloc(sizeof(char) * (strlen(token) + 1));
    strcpy(header_copy, token);
    char* col_token = NULL;
    char** header_index = &header_copy; 
    size_t n_cols = 0;
    while ((col_token = strsep(header_index, ",")) != NULL) {
        n_cols++;
    }
    cs165_log(stdout, "Header is of size %i\n", n_cols);
    free(header_copy);

    header_index = &token;
    Table* tables[n_cols]; //Under the assumption only 1 DB referred to in csv. Should modify for multiple.
    Column* columns[n_cols]; //Under the assumption only 1 DB referred to in csv. Should modify for multiple.
    size_t n_cols_written = 0;
    while ((col_token = strsep(header_index, ",")) != NULL) {
        tables[n_cols_written] = lookup_table(col_token);
        columns[n_cols_written] = lookup_column(col_token);
        if(tables[n_cols_written] == NULL || columns[n_cols_written] == NULL){
            log_err("Cannot look up the tables or columns referred to by the CSV\n");
            return OBJECT_NOT_FOUND;
        }
        cs165_log(stdout, "table: %p, col:%p\n", tables[n_cols_written], columns[n_cols_written] );
        n_cols_written++;
    }

    char* line_token = NULL;
    char** line_index = &token;
    size_t i = 0;

    while ((token = strsep(input_index, "\n")) != NULL) {
        if(strlen(token) == 0){
            continue;
        }
        i = 0;
        cs165_log(stdout, "token: %s\n", token);
        while ((line_token = strsep(line_index, ",")) != NULL) {
            cs165_log(stdout, "Writing %i to %s.%s\n", atoi(line_token), tables[i]->name, columns[i]->name);
            if(columns[i]->column_length == 0){ //Do NOT realloc an array of size 0!
                columns[i]->data = (int*) malloc(sizeof(int) * 1);
            }else{
                columns[i]->data = (int*) realloc(columns[i]->data, (sizeof(int) * (columns[i]->column_length + 1)));
            }
            columns[i]->data[columns[i]->column_length] = atoi(line_token);
            columns[i]->column_length++;
            cs165_log(stdout, "Column length %i, tables length %i\n", columns[i]->column_length, tables[i]->table_length);
            if(columns[i]->column_length > tables[i]->table_length){
                cs165_log(stdout, "Incrementing table length to %i\n", tables[i]->table_length + 1 );
                tables[i]->table_length++;
            }
            i++;
        }
    }

    return OK_DONE;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator)); // calloc?

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '='); 
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %.30s\n", query_command);

    send_message->status = UNKNOWN_COMMAND;

        if(strncmp(query_command, "load:", 5) != 0){                    //Such special treatment shouldn't be allowed, but for now, whatever.
            query_command = trim_whitespace(query_command);             //Change for different packet structure
        }
    

    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        send_message->status = parse_create(query_command);
        dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        send_message->status = parse_select(query_command, handle, dbo);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = malloc(sizeof(DbOperator));

        trim_parenthesis(query_command);

        for(size_t i = 0; i < client_context->col_count; i++){
            if(strcmp(client_context->columns[i].name, query_command) == 0){
                for(size_t j = 0; j < client_context->columns[i].column_length; j++){
                    printf("%i\n", client_context->columns[i].data[j]);
                }
            }
        }


    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = malloc(sizeof(DbOperator));
        send_message->status = OK_DONE;
        parse_fetch(query_command, handle);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        send_message->status = OK_WAIT_FOR_RESPONSE;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        send_message->status = SHUTTING_DOWN;
        dbo = malloc(sizeof(DbOperator));
    } else if (strncmp(query_command, "load:", 5) == 0) {
        query_command += 5;
        send_message->status = load_from_client(query_command);
        dbo = malloc(sizeof(DbOperator));
    }
    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
