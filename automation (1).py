import os
from dotenv import load_dotenv

# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSQL
import psycopg2

# Load env variables
load_dotenv()

# Connect to MySQL using cursor
MYSQL_USER=os.getenv("root")
MYSQL_HOST=os.getenv("Fakaxf6HRLeTUj8YUrHyBr4P")
MYSQL_PWD=os.getenv("172.21.17.50")

connect_MYSQL = mysql.connector.connect(
    user=MYSQL_USER, 
    host=MYSQL_HOST, 
    password=MYSQL_PWD, 
    database='sales')

mysql_cursor = connect_MYSQL.cursor()
print("Connected with MySQL server")

# Connect to PostgreSQL using cursor
PG_USER=os.getenv("postgres")
PG_HOST=os.getenv("127.0.0.1")
PG_PORT=os.getenv("5432")
PG_PWD=os.getenv("mHIkjScL7BhSl5ABxds4vVq1")
PG_DB=os.getenv("postgres")

connect_PGSQL = psycopg2.connect(
   database=PG_DB, 
   user=PG_USER,
   password=PG_PWD,
   host=PG_HOST,
   port=PG_PORT
)

pg_cursor = connect_PGSQL.cursor()
print("Connected with PostgreSQL server")

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSQL.

def get_last_rowid():
	query = "SELECT MAX(rowid) FROM sales_data"
	pg_cursor.execute(query=query)
	result = pg_cursor.fetchall()
	return result[0][0]

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
	query = f"SELECT * FROM sales_data WHERE rowid > {rowid}"
	mysql_cursor.execute(query)
	rows = mysql_cursor.fetchall()
	return rows	

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into PostgreSQL data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in PostgreSQL.

def insert_records(records):
	for row in records:
		query = "INSERT INTO sales_data(rowid, product_id, customer_id, quantity) VALUES(%s,%s,%s,%s)"
		pg_cursor.execute(query, row)
		connect_PGSQL.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connect_MYSQL.close()
# disconnect from DB2 or PostgreSql data warehouse 
connect_PGSQL.close()
# End of program