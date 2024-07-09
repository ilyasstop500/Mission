from ConDB import con_to_db
import Parametres


import logging
import json

logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=r"Mission\02_Realisation\Code\Logs\ResultCalcul.log"
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
            NIV = refsql[14]
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
                logging.error(f"Can not execute refsql SQL CODE and get Calculate Value for line with id '{idLigne}'"+str(e))
                query =("REPLACE INTO prm_ref_result ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,FORMULE,NIV,MSG)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s)")
                values = (idLigne, idObjet, TBD, PAGE,OBJET,TEMPS, PERD, RA, COL, ROW, SQL_CODE_SRC, SQL_CODE_FINAL, PERIMETRE, DATE_TRT,"PAS DE FORMULE - CALCUL SOURCE",NIV,"ERROR")
                cur.execute(query,values)
            query =("REPLACE INTO prm_ref_result ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,VALEUR,FORMULE,NIV,MSG)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s)")
            values = (idLigne, idObjet, TBD, PAGE,OBJET,TEMPS, PERD, RA, COL, ROW, SQL_CODE_SRC, SQL_CODE_FINAL, PERIMETRE, DATE_TRT,VALEUR,"PAS DE FORMULE - CALCUL SOURCE",NIV,MSG)
            
            
            # Execute the query with the values
            cur.execute(query,values)
            # Commit the transaction
            cnx.commit()
            # dict containing all value inserted into the db 
            mydict = {
                    "idLigne" : idLigne , 
                    "idObjet" : idObjet ,
                    "TBD" : TBD,
                    "PAGE" : PAGE ,
                    "OBJET" : OBJET ,
                    "DAT_REF" : str(TEMPS) ,
                    "PERD" : PERD ,
                    "RA_CODE" : RA ,
                    "COLS_CODE" : COL , 
                    "ROWS_CODE" : ROW,
                    "SQL_CODE_SRC" : SQL_CODE_SRC ,
                    "SQL_CODE_FINAL" : SQL_CODE_FINAL , 
                    "PERIMETRE" : PERIMETRE ,
                    "DATE_TRT" : str(DATE_TRT) , 
                    "VALEUR" : VALEUR,
                    "MSG" : "PAS DE FORMULE - CALCUL SOURCE",
                    "NIV" : NIV
                }
            
            # saving the dict in the log file
            logging.debug(f"uploaded into database line '{idLigne}' with value '{VALEUR}' '{json.dumps(mydict, indent=4)}'")

    except Exception as e:
                logging.error(f"Can not upload result source for line with id '{idLigne}' and value '{VALEUR}'"+str(e))




remplir_cube_final_source(Parametres.dateref,Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)
