from ConDB import con_to_db

def remplir_cube_final_source():
    
    cnx = con_to_db("root","1234","127.0.0.1","test5") #con to db 

    cur = cnx.cursor()
    query = ("SHOW TABLES")
    cur.execute(query)
    for elem in cur :
        print (elem)


    list_of_refsql = list()
    query = "SELECT * FROM prm_ref_sql"
    cur.execute(query)
    list_of_refsql = cur.fetchall()
    for refsql in list_of_refsql : 
        idLigne = refsql[0]
        idObjet = refsql[1]
        TBD = refsql[2]
        PAGE = refsql[3]
        OBJET = refsql[4]
        TEMPS = refsql[5]
        PERD = refsql[6]
        RA = refsql[7]
        COL = refsql [8]
        ROW = refsql[9]
        SQL_CODE_SRC = refsql[10]
        SQL_CODE_FINAL = refsql[11]
        PERIMETRE = refsql[12]
        DATE_TRT = refsql[13]
        cur.execute(SQL_CODE_FINAL)
        VALEUR = cur.fetchall()[0][-1]
        print(VALEUR)
        query =("INSERT IGNORE INTO prm_ref_result ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,VALEUR)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)")
        values = (idLigne, idObjet, TBD, PAGE,OBJET,TEMPS, PERD, RA, COL, ROW, SQL_CODE_SRC, SQL_CODE_FINAL, PERIMETRE, DATE_TRT,VALEUR)
        

        # Execute the query with the values
        cur.execute(query,values)
        # Commit the transaction
        cnx.commit()
