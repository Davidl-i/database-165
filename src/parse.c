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

message_status parse_avg (char* args, char* leftvar){
    message_status status = OK_DONE;
    if(leftvar == NULL){
        status = INCORRECT_FORMAT;
        return status;
    }
    trim_parenthesis(args);
    char** args_index = &args;
    char* colvar = next_token(args_index, &status);
    cs165_log(stdout, "colvar is %s. Store to is %s\n", colvar, leftvar);
    Column* working_column;
    working_column = lookup_client_context(colvar);
    if(working_column == NULL){
        working_column = lookup_column(colvar);
        if(working_column == NULL){
            status = OBJECT_NOT_FOUND;
            return status;
        }
    }
    double acc = 0;
    for(size_t i = 0; i < working_column->column_length; i++){
        acc += working_column->data[i];
    }
    double final = (acc / (working_column->column_length));
    double* result = (double*) malloc(sizeof(double));
    result[0] = final;
    Column* new_col = (Column*) malloc(sizeof(Column));
    strcpy(new_col->name, leftvar);
    new_col->data = (int*)result;
    new_col->column_length = sizeof(double) / sizeof(int);
    new_col->type = FLOAT;

    cs165_log(stdout, "accumulator %f, average %f\n", acc, final);
    store_client_variable(leftvar, new_col);
    return status;
}

message_status parse_sum (char* args, char* leftvar){
    message_status status = OK_DONE;
    if(leftvar == NULL){
        status = INCORRECT_FORMAT;
        return status;
    }
    trim_parenthesis(args);
    char** args_index = &args;
    char* colvar = next_token(args_index, &status);
    cs165_log(stdout, "colvar is %s. Store to is %s\n", colvar, leftvar);
    Column* working_column;
    working_column = lookup_client_context(colvar);
    if(working_column == NULL){
        working_column = lookup_column(colvar);
        if(working_column == NULL){
            status = OBJECT_NOT_FOUND;
            return status;
        }
    }
    long acc = 0;
    for(size_t i = 0; i < working_column->column_length; i++){
        acc += working_column->data[i];
    }
    cs165_log(stdout, "answer %ld\n",acc);
    long* result = (long*) malloc(sizeof(long));
    result[0] = acc;
    Column* new_col = (Column*) malloc(sizeof(Column));
    strcpy(new_col->name, leftvar);
    new_col->data = (int*)result;
    new_col->column_length = sizeof(long) / sizeof(int);
    new_col->type = LONG;

    store_client_variable(leftvar, new_col);
    return status;
}


message_status min_max (char* args, char* leftvar, bool want_max){
    message_status status = OK_DONE;
    if(leftvar == NULL){
        status = INCORRECT_FORMAT;
        return status;
    }
    trim_parenthesis(args);
    char** args_index = &args;
    char* colvar = next_token(args_index, &status);
    cs165_log(stdout, "colvar is %s. Store to is %s\n", colvar, leftvar);
    Column* working_column;
    working_column = lookup_client_context(colvar);
    if(working_column == NULL){
        working_column = lookup_column(colvar);
        if(working_column == NULL){
            status = OBJECT_NOT_FOUND;
            return status;
        }
    }
    if(working_column->column_length == 0){
        status = EXECUTION_ERROR;
        return status;
    }
    int cur_bound = working_column->data[0];
    if(want_max){  //Look for max val
        for(size_t i = 0; i < working_column->column_length; i++){
            if(working_column->data[i] > cur_bound){
                cur_bound = working_column->data[i];
            }
        }
    } else{   //look for min val
        for(size_t i = 0; i < working_column->column_length; i++){
            if(working_column->data[i] < cur_bound){
                cur_bound = working_column->data[i];
            }
        }
    }
    cs165_log(stdout, "answer %i\n", cur_bound);
    int* result = (int*) malloc(sizeof(int));
    result[0] = cur_bound;
    Column* new_col = (Column*) malloc(sizeof(Column));
    strcpy(new_col->name, leftvar);
    new_col->data = result;
    new_col->column_length = 1;
    new_col->type = INT;

    store_client_variable(leftvar, new_col);
    return status;
}

message_status col_math (char* args, char* leftvar, bool add){
    message_status status = OK_DONE;
    if(leftvar == NULL){
        status = INCORRECT_FORMAT;
        return status;
    }
    trim_parenthesis(args);
    char** args_index = &args;
    char* op_1 = next_token(args_index, &status);
    char* op_2 = next_token(args_index, &status);

    Column* col_1 = lookup_client_context(op_1);
    if(col_1 == NULL){
        col_1 = lookup_column(op_1);
        if(col_1 == NULL){
            status = OBJECT_NOT_FOUND;
            return status;
        }
    }

    Column* col_2 = lookup_client_context(op_2);
    if(col_2 == NULL){
        col_2 = lookup_column(op_2);
        if(col_2 == NULL){
            status = OBJECT_NOT_FOUND;
            return status;
        }
    }
    if(col_1->column_length != col_2->column_length){
        status = EXECUTION_ERROR;
        return status;
    }

    int* result = (int*) malloc(col_1->column_length * sizeof(int));
    if(add){
        for(size_t i = 0; i < col_1->column_length; i++){
            result[i] = col_1->data[i] + col_2->data[i];
        }        
    } else{
        for(size_t i = 0; i < col_1->column_length; i++){
            result[i] = col_1->data[i] - col_2->data[i];
        }             
    }

    Column* new_col = (Column*) malloc(sizeof(Column));
    strcpy(new_col->name, leftvar);
    new_col->data = result;
    new_col->column_length = col_1->column_length;
    new_col->type = INT;

    store_client_variable(leftvar, new_col);
    return status;
}

//Fetching does not go through dbo (function does the fetch itself)
message_status parse_fetch(char* args, char* leftvar){
    message_status status = OK_DONE;
    if(leftvar == NULL){
        status = INCORRECT_FORMAT;
        return status;
    }
    trim_parenthesis(args);
    char** args_index = &args;
    char* colvar = next_token(args_index, &status);
    char* colpos = next_token(args_index, &status);

    Column* vars = lookup_column(colvar);
    if(vars == NULL){
        status = OBJECT_NOT_FOUND;
        return status;
    }
    Column* positions = lookup_client_context(colpos);
    if(positions == NULL){
        status = OBJECT_NOT_FOUND;
        return status;
    }

    int* result = (int*) malloc(positions->column_length * sizeof(int));
    for(size_t i = 0; i < positions->column_length; i++){
        result[i] = vars->data[positions->data[i]];
    }

    Column* new_col = (Column*) malloc(sizeof(Column));
    strcpy(new_col->name, leftvar);
    new_col->data = result;
    new_col->column_length = positions->column_length;
    new_col->type = INT;

    store_client_variable(leftvar, new_col);
    return status;
}

message_status make_dbo_reg_select(char* colname, char* lowval, char* highval, char* leftvar, DbOperator* dbo){
    Column* target_column = lookup_column(colname);
    if(target_column == NULL){
        return OBJECT_NOT_FOUND;
    }
    dbo->operator_fields.select_operator.column = target_column;
    dbo->operator_fields.select_operator.lval = leftvar;
    char* select_lval = (char*) malloc(sizeof(char)*(strlen(leftvar) + 1));
    strcpy(select_lval, leftvar);
    dbo->operator_fields.select_operator.lval = select_lval;
    bool e_lower = (strcmp(lowval, "null") != 0);
    bool e_upper = (strcmp(highval, "null") != 0);
    dbo->operator_fields.select_operator.exists_lower = e_lower;
    dbo->operator_fields.select_operator.exists_upper = e_upper;
    dbo->operator_fields.select_operator.lower = (e_lower ? atoi(lowval): 0 );
    dbo->operator_fields.select_operator.upper = (e_upper ? atoi(highval): 0);
    dbo->operator_fields.select_operator.assoc_pos = NULL;
    return OK_WAIT_FOR_RESPONSE;
}

message_status make_dbo_select_from(char* pos, char* assoc_vals, char* lowval, char* highval, char* leftvar, DbOperator* dbo){
    Column* target_column = lookup_client_context(assoc_vals);
    if(target_column == NULL){
        return OBJECT_NOT_FOUND;
    }
    Column* assoc_pos = lookup_client_context(pos);
    if(assoc_pos == NULL){
        return OBJECT_NOT_FOUND;
    }
    if(target_column->column_length != assoc_pos->column_length){
        log_err("DBO select from operator isn't such that values and their associated positions are of same size!");
        return EXECUTION_ERROR;
    }
    dbo->operator_fields.select_operator.assoc_pos = assoc_pos;
    dbo->operator_fields.select_operator.column = target_column;
    char* select_lval = (char*) malloc(sizeof(char)*(strlen(leftvar) + 1));
    strcpy(select_lval, leftvar);
    dbo->operator_fields.select_operator.lval = select_lval;
    bool e_lower = (strcmp(lowval, "null") != 0);
    bool e_upper = (strcmp(highval, "null") != 0);
    dbo->operator_fields.select_operator.exists_lower = e_lower;
    dbo->operator_fields.select_operator.exists_upper = e_upper;
    dbo->operator_fields.select_operator.lower = (e_lower ? atoi(lowval): 0 );
    dbo->operator_fields.select_operator.upper = (e_upper ? atoi(highval): 0);
    return OK_WAIT_FOR_RESPONSE;
}

message_status parse_select(char* args, char* leftvar, DbOperator* dbo){ //For now, only supports 3-arg selects!! Needs to be updated to support either
    message_status status;
    memset(&status, 0 , sizeof(message_status));
    if(leftvar == NULL){
        return INCORRECT_FORMAT;
    }
    if(dbo == NULL){
        return EXECUTION_ERROR;
    }
    trim_parenthesis(args);

    char** args_index = &args;
    char* first_arg = next_token(args_index, &status);
    char* second_arg = next_token(args_index, &status);
    char* third_arg = next_token(args_index, &status);
    // char* overflow = strsep(args_index, ",");
    // if(overflow != NULL){
    //    return INCORRECT_FORMAT;
    // }
    if(status == INCORRECT_FORMAT){
        return status;
    }
    char* fourth_arg = next_token(args_index, &status);
    if(fourth_arg == NULL){
        cs165_log(stdout, "Going parse as regular select\n");
    }else{
        cs165_log(stdout, "Going to parse as select from\n");
        return make_dbo_select_from(first_arg, second_arg, third_arg, fourth_arg, leftvar, dbo);
    }

    return make_dbo_reg_select(first_arg, second_arg, third_arg, leftvar, dbo);
}

//out_pos1,out_pos2=join(val1,pos1,val2,pos2,<method>)
message_status parse_join(char* args, char* leftvar, DbOperator* dbo){ 
    message_status status;
    memset(&status, 0 , sizeof(message_status));
    if(leftvar == NULL){
        return INCORRECT_FORMAT;
    }
    if(dbo == NULL){
        return EXECUTION_ERROR;
    }
    trim_parenthesis(args);

    char** args_index = &args;
    char* first_arg = next_token(args_index, &status);
    char* second_arg = next_token(args_index, &status);
    char* third_arg = next_token(args_index, &status);
    char* fourth_arg = next_token(args_index, &status);
    //Ignore method.
    if(status == INCORRECT_FORMAT){
        return status;
    }
    char** lval_index = &leftvar;
    char* first_lval = next_token(lval_index, &status);
    char* second_lval = next_token(lval_index, &status);

    if(status == INCORRECT_FORMAT){
        return status;
    }

    Column* target_pos1 = lookup_client_context(second_arg);
    if(target_pos1 == NULL){
        return OBJECT_NOT_FOUND;
    }
    Column* target_val1 = lookup_client_context(first_arg);
    if(target_val1 == NULL){
        return OBJECT_NOT_FOUND;
    }
    if(target_pos1->column_length != target_val1->column_length){
        log_err("(operands 1) Join operands of position and val are not of the same length!");
        return EXECUTION_ERROR;
    }
    Column* target_pos2 = lookup_client_context(fourth_arg);
    if(target_pos2 == NULL){
        return OBJECT_NOT_FOUND;
    }
    Column* target_val2 = lookup_client_context(third_arg);
    if(target_val2 == NULL){
        return OBJECT_NOT_FOUND;
    }
    if(target_pos2->column_length != target_val2->column_length){
        log_err("(operands 2) Join operands of position and val are not of the same length!");
        return EXECUTION_ERROR;
    }
    dbo->operator_fields.join_operator.pos1 = target_pos1;
    dbo->operator_fields.join_operator.val1 = target_val1;
    dbo->operator_fields.join_operator.pos2 = target_pos2;
    dbo->operator_fields.join_operator.val2 = target_val2;


    dbo->operator_fields.join_operator.sto_pos1 = (char*) malloc(sizeof(char)*(strlen(first_lval) + 1) );
    strcpy(dbo->operator_fields.join_operator.sto_pos1, first_lval);
    dbo->operator_fields.join_operator.sto_pos2 = (char*) malloc(sizeof(char)*(strlen(second_lval) + 1) );
    strcpy(dbo->operator_fields.join_operator.sto_pos2, second_lval);
    return OK_WAIT_FOR_RESPONSE;
}


message_status start_batch(){
    if(query_capture_state){
        log_err("Cannot start batch: batch already started");
        return OBJECT_ALREADY_EXISTS;
    }
    memset(query_buffer, 0, sizeof(query_buffer));
    query_capture_state = true;
    query_buffer_count = 0;
    return OK_DONE;
}

message_status end_batch(){
    if(!query_capture_state){
        log_err("Cannot stop batch: batch doesn't even exist!");
        return OBJECT_NOT_FOUND;
    }
    query_capture_state = false;
    return OK_WAIT_FOR_RESPONSE;
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
    memset(&mes_status, 0, sizeof(message_status));
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
                columns[i]->column_max = COL_INCREMENT;
                columns[i]->data = (int*) malloc(sizeof(int) * COL_INCREMENT);
            }else{
                if(columns[i]->column_length >= columns[i]->column_max){
                    columns[i]->column_max += COL_INCREMENT;
                    columns[i]->data = (int*) realloc(columns[i]->data, (sizeof(int) * (columns[i]->column_max)));
                    cs165_log(stdout, "REALLOCATING COL TO %i!\n", columns[i]->column_max);
                }
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
        cs165_log(stdout, "LVAL: %s\n", handle);
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
        dbo->type = NONE;
        send_message->status = serve_print(query_command, client_socket);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = NONE;
        send_message->status = parse_fetch(query_command, handle);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = NONE;
        send_message->status = parse_avg(query_command, handle);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = NONE;
        send_message->status = parse_sum(query_command, handle);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = NONE;
        send_message->status = col_math(query_command, handle, true);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = NONE;
        send_message->status = col_math(query_command, handle, false);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = NONE;
        send_message->status = min_max(query_command, handle, false);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = NONE;
        send_message->status = min_max(query_command, handle, true);
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
    } else if (strncmp(query_command, "batch_queries", 13) == 0) {
        query_command += 13;
        send_message->status = start_batch();
        dbo = malloc(sizeof(DbOperator));
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        query_command += 13;
        send_message->status = end_batch();
        dbo = malloc(sizeof(DbOperator));
        dbo->type = RUN_BATCH;
    } else if (strncmp(query_command, "join", 4) == 0) {
        query_command += 4;
        dbo = malloc(sizeof(DbOperator));
        send_message->status = parse_join(query_command, handle, dbo);
        dbo->type = JOIN;
    } 
    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
