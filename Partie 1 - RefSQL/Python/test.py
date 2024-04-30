from ConDB import con_to_db
from CsvImport import Csv_import 
from CsvImport import Import_All_Csv 
from populate_refsql import populate_ref_sql


cnx = con_to_db("root","1234","127.0.0.1","test1") #con to db 
cur = cnx.cursor()
query = ("SHOW TABLES")
cur.execute(query)
for elem in cur :
    print (elem)

Import_All_Csv("C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables",cnx,cur) #loading all csvs into mysql

populate_ref_sql() #population refsql with the correct data 

