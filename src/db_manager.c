#include "cs165_api.h"
#include <string.h>
#include "utils.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h> //Buffered files; remove if not using buffered files.

// In this class, there will always be only one active database at a time
Db *current_db = NULL;

Column* create_column(char *name, Table *table, bool sorted, Status *ret_status){
	(void) sorted;
	if(table == NULL || name == NULL || ret_status == NULL){
		ret_status->code = NULLPOINTER;
		ret_status->error_message = "Null Pointer Exception";
		return NULL;
	}
	if(strlen(name) > MAX_SIZE_NAME - 1){
		ret_status->code = NAMEPROBLEM;
		ret_status->error_message = "Column name too long!";
		return NULL;
	}

	size_t i;

	for(i = 0; i < table->num_loaded_cols; i++){
    	Column* cur_col = table->columns + i;
    	cs165_log(stdout, "cur_col is %s\n", cur_col->name);
    	if(strcmp(cur_col->name, name) == 0){
    		ret_status->code = CREATED_ALREADY_EXISTS;
    		ret_status->error_message = "Column already exists!";
    		return NULL;
    	}
    }

	if(table->col_count == table->num_loaded_cols){
		ret_status->code = NO_MORE_ROOM;
		ret_status->error_message = "No more room for our new column, which is non-trivial! \n";
		return NULL;
	}


    Column* target_col =  table->columns + i;
	Column* new_col = (Column*) malloc(sizeof(Column));
	strcpy((new_col->name), name);
	new_col->index = NULL;
	new_col->column_length = 0;
	new_col->column_max = 0;
	new_col->type = INT;
	int* new_data = (int*) calloc(table->table_length, sizeof(int));
	new_col->data = new_data;
	memcpy(target_col, new_col, sizeof(Column));
	free(new_col);
	table->num_loaded_cols++;
	cs165_log(stdout, "new column created in table %s with %i size and name %s. There are now %i cols\n", table->name, table->table_length, target_col->name,table->num_loaded_cols);
	ret_status->code = OK;
	return target_col;
}

// typedef struct Column {
//     char name[MAX_SIZE_NAME]; 
//     int* data;
//     // You will implement column indexes later. 
//     void* index;
//     //struct ColumnIndex *index;
//     //bool clustered;
// } Column;


/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	if(db == NULL || name == NULL || ret_status == NULL){
		ret_status->code = NULLPOINTER;
		ret_status->error_message = "Null Pointer Exception";
		return NULL;
	}
	if(strlen(name) > MAX_SIZE_NAME - 1){
		ret_status->code = NAMEPROBLEM;
		ret_status->error_message = "Table name too long!";
		return NULL;
	}
	for(size_t i = 0; i < db->tables_size; i++){
		if(strcmp(db->tables[i].name, name) == 0){
			ret_status->code = CREATED_ALREADY_EXISTS;
			ret_status->error_message = "Table already exists!";
			return NULL;
		}
	}


	Table* new_table = (Table*) malloc(sizeof(Table));
	// memcpy(&(new_table->name), name, strlen(name));
	// (new_table->name)[strlen(name)] = '\0'; //Gotta null terminate!
	strcpy((new_table->name), name);

	new_table->col_count = num_columns;
	new_table->table_length = 0;
	new_table->num_loaded_cols = 0;
	Column* new_cols =	(Column*) calloc(num_columns,sizeof(Column)); //Calloc here to set array to 0
	new_table->columns = new_cols;

	if(db->tables_capacity <= db->tables_size){
		size_t cur_capacity = db->tables_capacity;
		size_t cur_numpage = (PAGE_SIZE % sizeof(Table) == 0) ? (cur_capacity * sizeof(Table)) / PAGE_SIZE: ((cur_capacity) * sizeof(Table)) / PAGE_SIZE + 1;
		db->tables = (Table*) realloc(db->tables, (cur_numpage + 1) * PAGE_SIZE);
		db->tables_capacity = ((cur_numpage + 1) * PAGE_SIZE) / sizeof(Table);
		cs165_log(stdout, "Resized! From %i capacity to %i capacity, (sizeof table is %i)\n", cur_capacity, db->tables_capacity, sizeof(Table));
	}

	(db->tables)[db->tables_size] = *new_table;
	db->tables_size++;
	cs165_log(stdout, "Now there are %i tables! This new table has %i columns and length of %i\n", db->tables_size, db->tables[db->tables_size-1].col_count, db->tables[db->tables_size-1].table_length);
	ret_status->code=OK;
	return new_table;
}

//Persists the DB on file
Status sync_db(Db* db){
	Status ret_status;
	//Iterate over tables
		//Iterate over columns
	if(db == NULL){
		ret_status.code = NULLPOINTER;
		ret_status.error_message = "Db to sync is null.";
		return ret_status;
	}
	char* file_to_open = (char*) malloc(sizeof(char) * (5 + strlen(db->name)) );
	snprintf(file_to_open, sizeof(char) * (5 + strlen(db->name)) , "%s.dat", db->name);
	cs165_log(stdout, "Opening Db file for writing: %s\n", file_to_open);
	int db_fd = open(file_to_open, O_WRONLY | O_TRUNC | O_CREAT , 0666);
	free(file_to_open);
	if(db_fd < 0){
		ret_status.code = IO_ERROR;
		ret_status.error_message = "Cannot open database file to write to.";
		return ret_status;
	}

	for(size_t i = 0; i < db->tables_size; i++){
		Table cur_table = db->tables[i];
		char* db_file_write_buffer = (char*) malloc(sizeof(char) * (strlen(cur_table.name) + 1));
		strcpy(db_file_write_buffer, cur_table.name);
		db_file_write_buffer[strlen(cur_table.name)] = '\n';
		int res = write(db_fd, db_file_write_buffer, strlen(cur_table.name) + 1);
		free(db_file_write_buffer);
		if(res < 0){
			close(db_fd);
			ret_status.code = IO_ERROR;
			ret_status.error_message = "Cannot write to DB file\n";
			return ret_status;
		}
		char* tbl_file_to_open = (char*) malloc(sizeof(char) * (6+ strlen(db->name) + strlen(cur_table.name) ) );
		snprintf(tbl_file_to_open, sizeof(char) * (6+ strlen(db->name) + strlen(cur_table.name) ) , "%s.%s.dat", db->name, cur_table.name);
		cs165_log(stdout, "Opening Table file for writing: %s\n", tbl_file_to_open);
		//int tbl_fd = open(tbl_file_to_open, O_WRONLY | O_TRUNC | O_CREAT , 0666);
		FILE* table_file = fopen(tbl_file_to_open, "w");
		
		free(tbl_file_to_open);
		if(table_file == NULL){
			close(db_fd);
			ret_status.code = IO_ERROR;
			ret_status.error_message = "Cannot open Table file to write to.";
			return ret_status;
		}
		for(size_t j = 0; j < cur_table.col_count; j++){
			Column cur_col = cur_table.columns[j];
			char* wb = (char*) malloc(sizeof(char) * (strlen(cur_col.name) + 3) );
			snprintf(wb, sizeof(char) * (strlen(cur_col.name) + 3), ">%s\n", cur_col.name);
			//res = write(tbl_fd, wb, strlen(wb)); //Don't write the null terminator.
			res = fwrite(wb, sizeof(char), strlen(wb), table_file);
			//if(res < 0){
			if((size_t)res != strlen(wb)*sizeof(char)){
				free(wb);
				close(db_fd);
				//close(tbl_fd);
				fclose(table_file);
				ret_status.code = IO_ERROR;
				ret_status.error_message = "Cannot write to Tbl file\n";
				return ret_status;
			}
			free(wb);
			wb = (char*) calloc(20 , sizeof(char));
			for(size_t k = 0; k < cur_table.table_length; k++){
				snprintf(wb, 20, "%i\n", cur_col.data[k]);
				//res = write(tbl_fd, wb, strlen(wb));
				res = fwrite(wb, sizeof(char), strlen(wb), table_file);
				//if(res < 0){
				if((size_t)res != strlen(wb)*sizeof(char)){
					free(wb);
					close(db_fd);
					//close(tbl_fd);
					fclose(table_file);
					ret_status.code = IO_ERROR;
					ret_status.error_message = "Cannot write an int to the Tbl file\n";
					return ret_status;
				}
			}
			free(wb);
		}
		//close(tbl_fd);
		fclose(table_file);
	}
	close(db_fd);
	ret_status.code = OK;
	return ret_status;
}

/* 
 * Similarly, this method is meant to create a database.
 * As an implementation choice, one can use the same method
 * for creating a new database and for loading a database 
 * from disk, or one can divide the two into two different
 * methods.
 */

//status code = error or message

//Will have to unload the current database when loading in a new one
//bool new indicates whether to bring the data from disk
//Ensure the db does not already exist!
Status add_db(const char* db_name, bool new) {
	cs165_log(stdout, "current db name is %s, and to load is %s\n",current_db->name, db_name);
	struct Status ret_status;
	if(db_name == NULL){
		ret_status.code = NULLPOINTER;
		ret_status.error_message = "Null Pointer: db_name";
		return ret_status;
	}
	if(strlen(db_name) > MAX_SIZE_NAME - 1){
		ret_status.code = NAMEPROBLEM;
		ret_status.error_message = "Database name too long!";
		return ret_status;
	}
	if(current_db != NULL && strcmp(current_db->name, db_name) == 0){ //Nothing to be done.
		if(new){
			ret_status.code = CREATED_ALREADY_EXISTS;
			ret_status.error_message = "Attempted to create a new DB, but it already exists on RAM!";
			return ret_status;
		}else{
			cs165_log(stdout, "Function add_db has nothing to do since db to load is already loaded \n");
			ret_status.code = OK; 
			return ret_status;
		}
	}
	char* db_file = (char*) malloc(sizeof(char) * (5 + strlen(db_name)) );
	snprintf(db_file, sizeof(char) * (5 + strlen(db_name)) ,"%s.dat", db_name);

	if( access( db_file , F_OK ) != -1 && new ) {	//when creating a new db, make sure it does not already exist on backing storage!
		free(db_file);
		ret_status.code = CREATED_ALREADY_EXISTS;
		ret_status.error_message = "Attempted to create a new DB, but it already exists on disk!";
		return ret_status;
	} 
	if( access( db_file , F_OK ) == -1 && !new) {	//when creating a new db, make sure it does not already exist on backing storage!
		free(db_file);
		ret_status.code = RESOURCE_NOT_EXIST;
		ret_status.error_message = "Trying to bring up a DB from disk, but it doesn't exist!";
		return ret_status;
	} 

	Db* new_db = (Db*) malloc(sizeof(Db));
	strcpy((new_db->name), db_name);
	if(new){
		free(db_file); //No need for this anymore.
		new_db->tables_size = 0;
		new_db->tables_capacity = PAGE_SIZE / sizeof(Table);
		new_db->tables = (Table*) malloc(PAGE_SIZE);
		cs165_log(stdout, "DB NAME: %s made Size: %i, Table capacity %i\n", new_db->name, new_db->tables_size, new_db->tables_capacity);
	} else{
		FILE* db_fp = fopen(db_file, "r");
		free(db_file);

		char db_linebuf[256];

		size_t num_tables = 0;
		while(fgets(db_linebuf, sizeof(db_linebuf), db_fp)){
			if(strlen(db_linebuf) <= 1){
				continue;
			}
			num_tables ++;
		}
		cs165_log(stdout, "There are %i tables\n", num_tables);
		new_db->tables_size = num_tables;
		new_db->tables_capacity =  (PAGE_SIZE * ((num_tables * sizeof(Table)) / PAGE_SIZE + (PAGE_SIZE%(num_tables * sizeof(Table)) == 0 ? 0:1))) / sizeof(Table);
		cs165_log(stdout, "Capacity of %i tables\n", new_db->tables_capacity);
		new_db->tables = (Table*) malloc(PAGE_SIZE * ((num_tables * sizeof(Table)) / PAGE_SIZE + (PAGE_SIZE%(num_tables * sizeof(Table)) == 0 ? 0:1)));

		rewind(db_fp);
		size_t table_pointer = 0;
		while(fgets(db_linebuf, sizeof(db_linebuf), db_fp)){
			if(strlen(db_linebuf) <= 1){
				continue;
			}
			trim_whitespace(db_linebuf);
			char* tbl_file = (char*) malloc(sizeof(char) * (6 + strlen(db_name) + strlen(db_linebuf)) );
			snprintf(tbl_file,sizeof(char) * (6 + strlen(db_name) + strlen(db_linebuf)) ,"%s.%s.dat", db_name, db_linebuf);
			cs165_log(stdout, "file to get: %s\n", tbl_file);
			FILE* tbl_fp = fopen(tbl_file, "r");
			free(tbl_file);
			if(tbl_fp == NULL){
				fclose(db_fp);
				free(new_db);
				ret_status.code = IO_ERROR;
				ret_status.error_message = "File linked to by the DB file not available!";
			}
			char tbl_linebuf[256];
			size_t col_elems_since_last_label = 0;
			size_t num_labels = 0;
			size_t num_loaded_labels = 0;
			while(fgets(tbl_linebuf, sizeof(tbl_linebuf), tbl_fp)){
				if(strlen(tbl_linebuf) <= 1){
					continue;
				}
				if(strncmp(tbl_linebuf, ">", 1) == 0){
					num_labels ++;
					num_loaded_labels += (strlen(tbl_linebuf) > 2);
					col_elems_since_last_label = 0;
				}else{
					col_elems_since_last_label++;
				}
			}
			cs165_log(stdout, "There are %i columns of %i size here (%i are loaded...)\n", num_labels, col_elems_since_last_label, num_loaded_labels);
			Table new_table;
			strcpy(new_table.name, db_linebuf);
    		new_table.col_count = num_labels;
    		new_table.num_loaded_cols = num_loaded_labels;
    		new_table.table_length = col_elems_since_last_label;
    		new_table.columns = (Column*) malloc(sizeof(Column) * num_labels);
    		rewind(tbl_fp);

    		int cur_col = -1;
    		size_t cur_pos = 0; //position within column
    		Column new_col;
			while(fgets(tbl_linebuf, sizeof(tbl_linebuf), tbl_fp) && cur_col < (int) num_loaded_labels){
				if(strlen(tbl_linebuf) <= 1){
					continue;
				}
				trim_whitespace(tbl_linebuf);
				if(strncmp(tbl_linebuf, ">", 1) == 0){
					if(cur_col != -1){
						cs165_log(stdout, "Adding to table %s, column %s\n", new_table.name, new_col.name);
						memcpy(&new_table.columns[cur_col], &new_col, sizeof(Column));
					}
					cur_col++;
					cur_pos = 0;
					memset(&new_col, 0, sizeof(Column));
					strcpy(new_col.name, tbl_linebuf + 1);
					new_col.index = NULL;
					new_col.column_length = new_table.table_length;
					new_col.column_max = COL_INCREMENT*(((new_col.column_length) + (COL_INCREMENT) - 1) / (COL_INCREMENT)); //COL_INC * Ceiling(col_length/COL_INCREMENT)
					cs165_log(stdout, "Max columns is %i\n", new_col.column_max);
					new_col.data = (int*) calloc(new_col.column_max, sizeof(int));
				}else{
					new_col.data[cur_pos] = atoi(tbl_linebuf);
					cur_pos++;
				}
			}
			if(cur_col != -1 && cur_col < (int) num_loaded_labels){
				cs165_log(stdout, "Adding to table %s, column %s\n", new_table.name, new_col.name);
				memcpy(&new_table.columns[cur_col], &new_col, sizeof(Column));
			}
			fclose(tbl_fp);
			
			cs165_log(stdout, "Adding to database %s, table %s\n", new_db->name, new_table.name);
			memcpy(&new_db->tables[table_pointer], &new_table, sizeof(Table));
			table_pointer ++;
		}
		fclose(db_fp);
/*
		cs165_log(stdout, "Database name %s\n", new_db->name);
		for(size_t i = 0; i < new_db->tables_size; i++){
	        Table curr_tbl = new_db->tables[i];
			cs165_log(stdout, "Table name %s\n", curr_tbl.name);
	        for(size_t j = 0; j < curr_tbl.col_count; j++){
	            Column curr_col = curr_tbl.columns[j];
	            cs165_log(stdout, "Column name %s, which consists of these:\n", curr_col.name);
	            for(size_t k = 0; k < curr_tbl.table_length; k++){
	            	cs165_log(stdout, "%i\n", curr_col.data[k]);
	            }
	        }
	    }
*/
	}

	//If loading is successful, then unload old one, otherwise, keep.
	if(current_db != NULL){
		Status sync_status = sync_db(current_db);
		if(sync_status.code != OK){
			log_err("Error from sync called inside add_db: %s\n", sync_status.error_message);
			ret_status.code = sync_status.code;
			ret_status.error_message = "Error in Syncing (See copied error code)";
			free(new_db);
			return ret_status;
		}
		shutdown_database(current_db); //Gets rid of all memory holding current_db
	}
	current_db = new_db;
	ret_status.code = OK;
	return ret_status;
}
