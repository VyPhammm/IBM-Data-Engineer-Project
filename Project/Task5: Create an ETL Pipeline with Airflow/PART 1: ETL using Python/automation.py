# Import libraries required for connecting to mysql
# Import libraries required for connecting to DB2

# Connect to MySQL
# Connect to DB2

import ibm_db
import mysql.connector

# Connect to MySQL

connection = mysql.connector.connect(user='root', password='MjkyNDctdnlwaGdp',host='127.0.0.1',database='sales')

# create cursor

cursor = connection.cursor()

# Connect to DB2

dsn_hostname = "55fbc997-9266-4331-afd3-888b05e734c0.bs2io90l08kqb1od8lcg.databases.appdomain.cloud" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = "xrn23470"        # e.g. "abc12345"
dsn_pwd = "H6BhEGntO2Rvdkvk"      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = "31929"                # e.g. "50000" 
dsn_database = "bludb"            # i.e. "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}" # i.e. "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              # i.e. "SSL"

#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

# create connection
conn = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

# export the fields name and type from collection test into the file data.csv

# Find out the last rowid from DB2 data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database.
def get_last_rowid():
    SQL = "SELECT MAX(ROWID) FROM sales_data"
    stmt = ibm_db.exec_immediate(conn, SQL)
    res = ibm_db.fetch_both(stmt)
    print(res)
    return int(res[0])

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
	SQL = "SELECT * FROM sales_data WHERE rowid > %s"
    cursor.execute(SQL)
    for row in cursor.fetchall():
	    print(row)

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database.

def insert_records(records)
    SQL = "INSERT INTO sales_data(rowid,product_id,customer_id,quatity) VALUES(?,?,?,?);"
    stmt = ibm_db.prepare(conn, SQL)
    for record in records:
        ibm_db.execute(stmt, record)

insert_records(new_records)
print("New row inserted into datawarehouse = ", len(new_records))

# Close connection

connection.close()

ibm_db.close(conn)

#End Program
