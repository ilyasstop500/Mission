import os
import string
import csv
from logs import log
def Csv_import(filename, tablename, cnx, cursor):
    try:
        query1 = f"DELETE FROM {tablename};"
        query2 = f"""LOAD DATA  INFILE '{filename}' INTO TABLE {tablename}
            FIELDS TERMINATED BY ';' ENCLOSED BY ''
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES;"""
        cursor.execute(query1)
        cursor.execute(query2)
        cnx.commit()
        log("CSV data loaded successfully  into database",r"C:\Users\ILYASS\Desktop\LOGS\logs_import.csv")
    except Exception as e:
        log("Error loading CSV data:"+str(e) ,r"C:\Users\ILYASS\Desktop\LOGS\logs_import.csv" )

def Import_All_Csv(directory,cnx,cursor) :
    list_of_csv = os.listdir(directory)
    folder = os.path.basename(os.path.normpath(directory))
    for csv in list_of_csv :
        tablename = os.path.splitext(csv)[0].lower()
        log("importing {csv} into database",r"C:\Users\ILYASS\Desktop\LOGS\logs_import.csv")
        Csv_import(f"{folder}/{csv}",tablename,cnx,cursor)
        #Csv_import(r"CsvTables\\PRM_TDB_OBJETS.csv","prm_tdb_objets",cnx,cur)
        #print(csv + "inserted successfuly")

def edit_csv_refsql(idLigne,idObjet,TDB,PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,mode) :
    if mode == "a" :
        with open("C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables\PRM_REF_SQL.csv", 'a',newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',quotechar='|', quoting=csv.QUOTE_MINIMAL)     
            spamwriter.writerow([idLigne,idObjet,TDB,PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT])
        log("refsql csv has been edited with append option " ,r"C:\Users\ILYASS\Desktop\LOGS\logs_refsql.csv")
    
    elif mode == "w" : 
        with open("C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables\PRM_REF_SQL.csv", 'w',newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',quotechar='|', quoting=csv.QUOTE_MINIMAL)     
            spamwriter.writerow([idLigne,idObjet,TDB,PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT])
        log("refsql csv has benn edited with write option" ,r"C:\Users\ILYASS\Desktop\LOGS\logs_refsql.csv")

    else : 
        log("incorrect mode chosen you can only choose write or append " , r"C:\Users\ILYASS\Desktop\LOGS\logs_refsql.csv" )
         

    

    














