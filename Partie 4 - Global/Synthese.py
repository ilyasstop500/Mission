from script_creation_db import script_tables
from ConDB import  con_to_db
from CsvImport import Import_All_Csv
from populate_refsql import remplir_ref_sql
from execsql import remplir_cube_final_source




CSVs_directory = "C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables2"
logs_directory = r"C:\Users\ILYASS\Desktop\LOGS\logs_refsql.csv"  #always add r before the path unless you want a unicode error 
username = 'root'
password =  '1234'
ip_address =  '127.0.0.1'
schema_name = 'finalo'
dateref = "202405"


# STEP 1 / CREATING THE TABLES IF THEY DONT EXIST ALREADY ----------------------------------------------------------------------------------------------------------

script_tables(username,password,ip_address,schema_name)

# STEP 2 / UPLOADING THE DATA FROM THE CSV INTO THE DATABASE (Ra-liens,Cols,Rows,Objects,PrmCols,PrmRows,...........) 

cnx = con_to_db(username,password,ip_address,schema_name)
cur = cnx.cursor()
Import_All_Csv(CSVs_directory,cnx,cur,logs_directory)

# STEP 3 / IMPORTING THE DATA FROM THE DB AND CREATING THE REFSQL QUERIES 

remplir_ref_sql(dateref,logs_directory,username,password,ip_address,schema_name)

# STEP 4 / EXECUTING THE REFSQL QUERIES AND UPLOADING THE RESULT TO FINAL CUBE WITH ONLY SOURCE ROWS

remplir_cube_final_source(username,password,ip_address,schema_name)



















