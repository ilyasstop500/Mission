from ConDB import con_to_db
from CsvImport import Csv_import 
from CsvImport import Import_All_Csv 


cnx = con_to_db("root","1234","127.0.0.1","test1")
cur = cnx.cursor()
query = ("SHOW TABLES")
cur.execute(query)
for elem in cur :
    print (elem)

cur.execute(f"SHOW COLUMNS FROM prm_tdb_objets")
columns = [column[0] for column in cur.fetchall()]
print("Table columns:", columns)

#Csv_import(r"CsvTables\\PRM_TDB_OBJETS.csv","prm_tdb_objets",cnx,cur)
Import_All_Csv("C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables",cnx,cur)