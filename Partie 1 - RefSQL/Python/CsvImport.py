import os
import string
import csv
from logs_refsql import log_refsql as log
def Csv_import(filename, tablename, cnx, cursor,logdirect):
    try:
        query1 = f"DELETE FROM {tablename};"
        query2 = f"""LOAD DATA  INFILE '{filename}' INTO TABLE {tablename}
            FIELDS TERMINATED BY ';' ENCLOSED BY ''
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES;"""
        cursor.execute(query1)
        cursor.execute(query2)
        cnx.commit()
        print("CSV data loaded successfully  into database",logdirect)
    except Exception as e:
        print("Error loading CSV data:"+str(e) ,logdirect)

def Import_All_Csv(directory,cnx,cursor,logdirect) :
    list_of_csv = os.listdir(directory)
    folder = os.path.basename(os.path.normpath(directory))
    for csv in list_of_csv :
        tablename = os.path.splitext(csv)[0].lower()
        print("uploading " + csv + " into  the database",logdirect)
        Csv_import(f"{folder}/{csv}",tablename,cnx,cursor,logdirect)
        #Csv_import(r"CsvTables\\PRM_TDB_OBJETS.csv","prm_tdb_objets",cnx,cur)
        #print(csv + "inserted successfuly")

def edit_csv_refsql(idLigne,idObjet,TDB,PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,mode,logdirect) :
    if mode == "a" :
        with open("C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables\PRM_REF_SQL.csv", 'a',newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',quotechar='|', quoting=csv.QUOTE_MINIMAL)     
            spamwriter.writerow([idLigne,idObjet,TDB,PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT])
        print("refsql csv has been edited with append option " ,logdirect)
    
    elif mode == "w" : 
        with open("C:\ProgramData\MySQL\MySQL Server 8.0\Data\CsvTables\PRM_REF_SQL.csv", 'w',newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',quotechar='|', quoting=csv.QUOTE_MINIMAL)     
            spamwriter.writerow([idLigne,idObjet,TDB,PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT])
        print("refsql csv has benn edited with write option" ,logdirect)

    else : 
        print("incorrect mode chosen you can only choose write or append " , logdirect )
         

    

    














