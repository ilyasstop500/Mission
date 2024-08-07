from ConDB import con_to_db
import Parametres
from CsvRead import modify_specific_lines

import logging
import json

logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=r"C:\Users\ILYASS\Desktop\Stage\Mission\02_Realisation\Code\Logs\ResultSource.log"
)




def remplir_cube_final_source(dateref,user,pwd,ip,schema):

    try : 

        

        logging.debug(f"populating '{schema}' schema's resultsql source  with dateref '{dateref}'")
        
        cnx = con_to_db(user,pwd,ip,schema) #con to db 

        cur = cnx.cursor()
        query = ("SHOW TABLES")
        cur.execute(query)
        for elem in cur :
            print (elem)
        
        

        # getting all line for the refsql table
        list_of_refsql = list()
        query = "SELECT * FROM prm_ref_sql"
        cur.execute(query)
        list_of_refsql = cur.fetchall()
        idFact = Parametres.next_fact_id
        for refsql in list_of_refsql :  

            idSQLLigne = refsql[0]
            idObjet = refsql[1]
            idCol = refsql[2]
            idRow = refsql[3]
            TEMPS = refsql[4]
            PERD = refsql[5]
            COL = refsql [6]
            ROW = refsql[7]
            SQL_CODE_SRC = refsql[8]
            SQL_CODE_FINAL = refsql[9]
            PERIMETRE = refsql[10]
            DATE_TRT = refsql[11]
            NIV = refsql[12]
            try : 
                cur.execute(SQL_CODE_FINAL)
                results = cur.fetchall()

                if results:
                    VALEUR = results[0][-1]
                    MSG = "GOOD"
                else:
                    VALEUR = 0  # Or any default value you want to use
                    MSG = "NO ENTRIES FOR CHOSEN PERIOD"

                
            # exception in the case of an error that will send the "ERROR" msg 

            except Exception as e : 
                logging.error(f"Can not execute refsql SQL CODE and get Calculate Value for line with id '{idSQLLigne}'"+str(e))
                query =("REPLACE INTO prm_ref_result ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s)")
                values = (idFact,idCol,idRow,idObjet,TEMPS, PERD,COL, ROW,idSQLLigne, PERIMETRE,"STATUS","ERROR","VALIDE","","","",DATE_TRT)
                cur.execute(query,values)
            query =("REPLACE INTO prm_ref_result ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
            values = (idFact,idCol,idRow,idObjet,TEMPS, PERD,COL, ROW,idSQLLigne, PERIMETRE,VALEUR,"STATUS","WOOOOW","VALIDE","","","",DATE_TRT)
            
             
            modify_specific_lines(r"C:\Users\ILYASS\Desktop\Stage\Mission\02_Realisation\Code\Parametres.py", 8 ,f"next_fact_id = {idFact}")
            idFact = idFact + 1
            
            # Execute the query with the values
            cur.execute(query,values)

            query = (f"INSERT INTO PRM_LINEAGE ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
            values = (idCol,idRow,VALEUR,"SOURCE","","")
            cur.execute(query,values)
            # Commit the transaction
            cnx.commit()
            
            # dict containing all value inserted into the db 
            mydict = {
                    "idFact" : idFact , 
                    "idObjet" : idObjet ,
                    "idCol" : idCol,
                    "idRow" : idRow,
                    "DAT_REF" : str(TEMPS) ,
                    "PERD" : PERD ,
                    "COLS_CODE" : COL , 
                    "ROWS_CODE" : ROW,
                    "PERIMETRE" : PERIMETRE ,
                    "DATE_TRT" : str(DATE_TRT) , 
                    "VALEUR" : VALEUR,
                    "MSG" : "PAS DE FORMULE - CALCUL SOURCE",
                    "NIV" : NIV
                }
            
            # saving the dict in the log file
            logging.debug(f"uploaded into database line '{idFact}' with value '{VALEUR}' '{json.dumps(mydict, indent=4)}'")

    
    except Exception as e:
        
                logging.error(f"Can not upload result source for line with id and value " + str(e))




remplir_cube_final_source(Parametres.dateref,Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)
