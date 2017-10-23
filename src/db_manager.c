#include "cs165_api.h"
#include <string.h>
#include "utils.h"

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
	//Iterate over tables
		//Iterate over columns
	Status ret_status;
	(void)db;
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

	//when creating a new db, make sure it does not already exist on backing storage!

	if(new){
		//If loading is successful, then unload old one, otherwise, keep.
		Db* new_db = (Db*) malloc(sizeof(Db));
		// memcpy(&(new_db->name), db_name, strlen(db_name));
		// (new_db->name)[strlen(db_name)] = '\0'; //gotta null terminate!
		strcpy((new_db->name), db_name);

		new_db->tables_size = 0;
		new_db->tables_capacity = PAGE_SIZE / sizeof(Table);
		new_db->tables = (Table*) malloc(PAGE_SIZE);
		cs165_log(stdout, "DB NAME: %s made Size: %i, Table capacity %i\n", new_db->name, new_db->tables_size, new_db->tables_capacity);
		//sync_db();
		current_db = new_db;
	} else{
		//run db_load from disk

		if(current_db == NULL){
			ret_status.code = ERROR;                 //This will have to be replaced once can be loaded from disk.
			ret_status.error_message = "Getting db from disk no yet implemented! \n";
			return ret_status;
		}

		if(strcmp(current_db->name, db_name) == 0){
			ret_status.code = OK;
			return ret_status;
		}
		ret_status.code = ERROR;
		ret_status.error_message = "Loading databases from disk not yet supported!";
		return ret_status;
	}

	ret_status.code = OK;
	return ret_status;
}
