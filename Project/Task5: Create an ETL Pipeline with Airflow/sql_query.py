# The sql query:  get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database.

last_rowid_sql = "SELECT MAX(ROWID) FROM sales_data"

# The sql query: get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

latest_records_sql = "SELECT * FROM sales_data WHERE rowid > %s"

# The sql query: insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database.

insert_records_sql = "INSERT INTO sales_data(rowid,product_id,customer_id,quatity) VALUES(?,?,?,?);"
