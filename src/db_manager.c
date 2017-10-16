#include "cs165_api.h"
#include <string.h>
#include "utils.h"

// In this class, there will always be only one active database at a time
Db *current_db = NULL;

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */

/*
typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_length;
} Table;

*/
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	if(db == NULL || name == NULL || ret_status == NULL){
		ret_status->code = ERROR;
		ret_status->error_message = "Null Pointer Exception";
		return NULL;
	}
	if(strlen(name) > MAX_SIZE_NAME){
		ret_status->code = ERROR;
		ret_status->error_message = "Table name too long!";
		return NULL;
	}

	Table* new_table = (Table*) malloc(sizeof(Table));
	memcpy(&(new_table->name), name, strlen(name));
	new_table->col_count = num_columns;
	new_table->table_length = 0;
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
Status add_db(const char* db_name, bool new) {
	struct Status ret_status;
	if(db_name == NULL){
		ret_status.code = ERROR;
		ret_status.error_message = "Null Pointer: db_name";
		return ret_status;
	}
	if(strlen(db_name) > MAX_SIZE_NAME){
		ret_status.code = ERROR;
		ret_status.error_message = "Database name too long!";
		return ret_status;
	}

	if(new){
		//If loading is successful, then unload old one, otherwise, keep.
		Db* new_db = (Db*) malloc(sizeof(Db));
		memcpy(&(new_db->name), db_name, strlen(db_name));
		new_db->tables_size = 0;
		new_db->tables_capacity = PAGE_SIZE / sizeof(Table);
		new_db->tables = (Table*) malloc(PAGE_SIZE);
		cs165_log(stdout, "DB NAME: %s made Size: %i, Table capacity %i\n", new_db->name, new_db->tables_size, new_db->tables_capacity);
		//sync_db();
		current_db = new_db;
	} else{
		//run db_load from disk
	}

	ret_status.code = OK;
	return ret_status;
}
