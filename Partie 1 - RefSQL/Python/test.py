from ConDB import con_to_db
from CsvImport import Csv_import
from CsvImport import Import_All_Csv
from CsvImport import edit_csv_refsql
from populate_refsql import populate_ref_sql





CSVs_directory = "C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables2"
logs_directory = r"C:\Users\ILYASS\Desktop\LOGS\logs_refsql.csv"  #always add r before the path unless you want a unicode error 




dar_ref= "202405"  

cnx = con_to_db("root","1234","127.0.0.1","test5") #con to db 

cur = cnx.cursor()
query = ("SHOW TABLES")
cur.execute(query)
for elem in cur :
    print (elem)
Import_All_Csv(CSVs_directory,cnx,cur,logs_directory)
populate_ref_sql(dar_ref,CSVs_directory,logs_directory) #population refsql with the correct data 


