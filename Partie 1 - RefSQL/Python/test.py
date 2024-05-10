from ConDB import con_to_db
from CsvImport import Csv_import
from CsvImport import Import_All_Csv
from CsvImport import edit_csv_refsql
from populate_refsql import populate_ref_sql


dar_ref= "202111"  # est egale à M0N0 doit etre un int 

cnx = con_to_db("root","1234","127.0.0.1","test1") #con to db 
cur = cnx.cursor()
query = ("SHOW TABLES")
cur.execute(query)
for elem in cur :
    print (elem)

#Import_All_Csv("C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables",cnx,cur) #loading all csvs into mysql

populate_ref_sql(dar_ref) #population refsql with the correct data 


