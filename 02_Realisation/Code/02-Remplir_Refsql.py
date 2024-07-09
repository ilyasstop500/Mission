############################################################################################################
# Module P1 : générateur SQL
# Auteur : Ilyass
# date : Avril 2024 
############################################################################################################

from ConDB import con_to_db
from timecode import code_to_date
from datetime import datetime
import Parametres
import json

import logging


logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=r"Mission\02_Realisation\Code\Logs\Refsql.log"
)


def remplir_ref_sql (dateref,user,pwd,ip,schema) :
    try  :

        logging.debug(f"populating '{schema}' schema's refsql  with dateref '{dateref}'")

        #connexion a la bdd
        cnx = con_to_db(user,pwd,ip,schema)
        cur = cnx.cursor()

        #test affichage des tables
        query = ("SHOW TABLES")
        cur.execute(query)  
        for elem in cur :
            print (elem)

        
        query = ("SELECT DISTINCT r.idRACible,r.idObjet,r.idRA,r.RA_Code,r.idColsCib,r.idRowsCib,r.COLS_CODE,r.ROWS_CODE,r.LIEN_VALIDE,c.COLS_NATURE FROM PRM_RA_LIENS r JOIN PRM_COLS_FILTRE c ON r.idColsCib = c.idCols WHERE r.LIEN_VALIDE ='OUI\r' AND c.COLS_NATURE = 'SOURCE'" )
        cur.execute(query)
        list_of_links = [] # list_of_links contain the list of (idObjet,idRA,RA_CODE,idColsCib,idRowsCib,COLS_CODE,ROW_CODE,LIEN_VALIDE)
        for elem in cur : 
            list_of_links.append(elem)
            print("link : " , elem)
        
        # test to make sure the list of links isnt empty 
        if len(list_of_links) == 0 : 
            logging.critical("list of valid links is empty , pls check the RA_LIENS CSV file and make sure that it has at least 1 valid link ")
            return
        

        for link in list_of_links : # loop that iterates over all the links between columns and rows and contructs the correct sqlref query for each one 

            #-------------------------------------------------------------------- FIRST STEP : GET THE CORRECT SQL MODEL -------------------------------------------------------------------------------

            idLigne = str(link[0]) + '/' + Parametres.dateref

            idCol = link[4]
            if len(str(idCol)) == 0 : 
                logging.error(f"No Column id for the link with id '{idLigne}' ")
                continue

            idRow = link[5] 
            if len(str(idRow)) == 0 : 
                logging.error(f"No Row id for the link with id '{idLigne}' ")
                continue

            idObjet = link[1] 
            if len(str(idObjet)) == 0 : 
                logging.error(f"No Column id for the link with id '{idLigne}' ")
                continue
                

            # fetching the datamart  
            query = (f"SELECT COLS_DATAMART FROM PRM_COLS_FILTRE WHERE idCols = '{idCol}' LIMIT 1 ") 
            cur.execute(query)
            for elem in cur :
                datamart = elem[0]



            if datamart ==  ""  : 
                logging.error(f"could not find the datamart for the column with id '{idCol}' ")
                continue

            
            # fetching the sql_model that corresponds to the datamart 
            query = (f"SELECT TEXT_SQL FROM PRM_SQL_MODEL WHERE INDI_CODE_SQL = '{datamart}'")
            cur.execute(query)
            for elem in cur :
                sql_code = elem[0].strip()

            if len(sql_code) == 0 : 
                logging.error(f"could not find the sql code for the datamart , pls check that the PRM_SQL_MODEL csv for potential errors ")
                continue
            
            sql_code = sql_code.replace('""','"')[0:-1] # removes the extra "" added by the mysql db 


            #-------------------------------------------------------------------- SECOND STEP : FILLING THE SQL MODEL WITH PARAMETRES ------------------------------------------------------------------
            
            SQL_CODE_SRC = sql_code
            
            RA = link[3] 
            COL = link[6]
            ROW = link[7]
            PERD = "M0N0"
            PERIMETRE = "SOURCE"
            DATE_TRT = now = datetime.now()

            if len(str(RA)) == 0 :
                logging.warning("RACODE field is Empty")
            if len(str(ROW)) == 0 :
                logging.warning("ROWCODE field is Empty")
            if len(str(COL)) == 0 :
                logging.warning("COLCODE field is Empty")
            if len(str(PERD)) == 0 :
                logging.error("PERD field is Empty")
                continue


            query = (f" SELECT ROWS_NIV FROM PRM_ROWS WHERE ROWS_CODE = '{ROW}'AND idObjet = '{idObjet}'")
            cur.execute(query)
            NIV = cur.fetchone()[0]

            if len(str(NIV)) == 0 :
                logging.warning("NIV field is Empty")
                continue


            query = (f" SELECT TDB,PAGE,OBJET FROM PRM_TDB_OBJETS WHERE idObjet = {idObjet}")
            cur.execute(query)
            prop_list = []
            for elem in cur :
                prop_list.append(elem)
            
            TBD = prop_list[0][0]
            PAGE = prop_list[0][1]
            OBJET = prop_list[0][2]

            #warning logs
            if len(str(TBD)) == 0 :
                logging.warning("TBD field is Empty")
            if len(str(PAGE)) == 0 :
                logging.warning("PAGE field is Empty")
            if len(str(OBJET)) == 0 :
                logging.warning("OBJET field is Empty")

                
            
            #----------------------------------extracting cols parametres--------------------------------------------------#
            #we start by sorting all the rows in the prm_cols_filtre so that we only get the filtres bound to our column
            query = (f"SELECT COLS_NATURE,RA_CODE,idCols,COLS_CODE,COLS_DATAMART,COLS_FILTRE_DOMAINE,DIM_VAL_CODE,idObjet,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_COLS_FILTRE WHERE  idCols ='{idCol}'")
            cur.execute(query)
            list_of_filters = []
            for elem in cur :
                list_of_filters.append(elem)
            

            #afterwards we check all the filre_cha and add them to a list
            query = (f"SELECT DISTINCT FILTRE_CHA FROM PRM_COLS_FILTRE WHERE  idCols ='{idCol}'")
            cur.execute(query)
            list_of_cha = []
            for elem in cur : 
                list_of_cha.append(elem)
            
            total_condition_col = ""

            #and then we go through this list of filtre_cha one by one writing the condition to respect of each one depending on their nature , this will help us in the case where we want to sort a
            #field by including data from more that 1 departement using the   "WHERE departement in (val1,val2,...)" 
            for cha in list_of_cha : 
                
                #for one specific cha we will extract only the cols filters that have it and then write the condition depending on the nature of the value (time,dimension,value) and the sens (include or not )
                query = (f"SELECT COLS_NATURE,RA_CODE,idCols,COLS_CODE,COLS_DATAMART,COLS_FILTRE_DOMAINE,DIM_VAL_CODE,idObjet,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_COLS_FILTRE WHERE  idCols ='{idCol}'  AND FILTRE_CHA = '{cha[0]}' ")
                cur.execute(query)
                list_of_filters_same_cha = []
                for elem in cur : 
                    list_of_filters_same_cha.append(elem)
                
                if list_of_filters_same_cha[0][5] == "TEMPS" and list_of_filters_same_cha[0][11].strip().lower() == "inclure" : 
                
                    condition_temps = f" AND {cha[0]} = "
                    for filtre_same_cha in list_of_filters_same_cha :
                        PERD = filtre_same_cha[10]
                        condition_temps = condition_temps + code_to_date(filtre_same_cha[10],dateref) +","
                    total_condition_col += condition_temps[0:-1] + ""

                elif list_of_filters_same_cha[0][5] == "TEMPS" and list_of_filters_same_cha[0][11].strip().lower() == "exclure" : 
                
                    condition_temps = f" AND NOT {cha[0]} = "
                    for filtre_same_cha in list_of_filters_same_cha :
                        PERD = filtre_same_cha[10]
                        condition_temps = condition_temps + code_to_date(filtre_same_cha[10],dateref) +","
                    total_condition_col += condition_temps[0:-1] + ""
                
                elif list_of_filters_same_cha[0][5] == "DIMENSION" and list_of_filters_same_cha[0][11].strip().lower() == "inclure": 
                    
                    condition_dim = f" AND {cha[0]} IN ("
                    for filtre_same_cha in list_of_filters_same_cha :
                        condition_dim = condition_dim +"'"+filtre_same_cha[10] +"'"+","
                    total_condition_col += condition_dim[0:-1] + ")"

                elif list_of_filters_same_cha[0][5] == "DIMENSION" and list_of_filters_same_cha[0][11].strip().lower() == "exclure": 
                    
                    condition_dim = f" AND NOT {cha[0]} IN ("
                    for filtre_same_cha in list_of_filters_same_cha :
                        condition_dim = condition_dim +"'"+filtre_same_cha[10] +"'"+","
                    total_condition_col += condition_dim[0:-1] + ")"
                
                elif list_of_filters_same_cha[0][5] == "MESURE":
                    INDI = cha[0]
                    
                
                

            #---------------------------------extracting rows parametres ----------------------------------------# .
            #rows follow basically the same logic as the cols just without checking the nature of the value
            query = (f"SELECT idObjet,idRows,ROWS_CODE,DIM_VAL_CODE,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_ROWS_FILTRE WHERE  idRows ='{idRow}'")
            cur.execute(query)
            list_of_filters = []
            for elem in cur :
                list_of_filters.append(elem)

            query = (f"SELECT DISTINCT FILTRE_CHA FROM PRM_ROWS_FILTRE WHERE  idRows ='{idRow}'")
            cur.execute(query)
            list_of_cha = []
            for elem in cur : 
                list_of_cha.append(elem)
            
            total_condition_row = ""
            for cha in list_of_cha :    

                query = (f"SELECT * FROM PRM_ROWS_FILTRE WHERE  idRows ='{idRow}'  AND FILTRE_CHA = '{cha[0]}' ")
                cur.execute(query)
                list_of_filters_same_cha = []
                for elem in cur : 
                    list_of_filters_same_cha.append(elem)
                
                if list_of_filters_same_cha[0][7].strip().lower() == "inclure" : 
                    condition_dim = f" AND {cha[0]} IN ("
                    for filtre_same_cha in list_of_filters_same_cha :
                        condition_dim = condition_dim +"'"+ filtre_same_cha[6] +"'"+","
                    total_condition_row += condition_dim[0:-1] + ")"
                else : 
                    condition_dim = f" AND NOT {cha[0]} IN ("
                    for filtre_same_cha in list_of_filters_same_cha :
                        condition_dim = condition_dim +"'"+ filtre_same_cha[6] +"'"+","
                    total_condition_row += condition_dim[0:-1] + ")"
                    
                
            



            #here we create the final sqlref query by substuting the fields with the correct parametres 
            sql_code=sql_code.replace("[p_DAR_REF]",code_to_date("[M0N0]",dateref)) # code_to_date() is a function that returns a yyyymm date using a code and the today's date
            sql_code=sql_code.replace("[p_objet]",str(idObjet))
            sql_code=sql_code.replace("[p_RA]",RA)
            sql_code=sql_code.replace("[p_ROW]",ROW)
            sql_code=sql_code.replace("[p_COL]",COL)
            sql_code=sql_code.replace("[p_PERD]",PERD)
            sql_code=sql_code.replace("[p_CUBE]",datamart)
            sql_code=sql_code.replace("[p_INDI]",INDI)

            position = sql_code.find('GROUP BY')

            sql_code = sql_code[:position] + total_condition_col +total_condition_row + ' ' + sql_code[position:]
            SQL_CODE_FINAL = sql_code +"'" # the final sqlref query
            
            
            
            

    



            #upload to database ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            try : 
                TEMPS = code_to_date("[M0N0]",dateref)
                query =("REPLACE INTO prm_ref_sql ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,NIV)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s,%s)")
                values = (idLigne, idObjet, TBD, PAGE,OBJET,TEMPS, PERD, RA, COL, ROW, SQL_CODE_SRC, SQL_CODE_FINAL, PERIMETRE, DATE_TRT,NIV)
                
                # Execute the query with the values
                cur.execute(query, values)

                # Commit the transaction
                cnx.commit()
                #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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
                    "DATE_TRT" : str(DATE_TRT)  
                }

                logging.debug(f"uploaded into database'{json.dumps(mydict, indent=4)}'")

            except Exception as e:
                logging.error(f"Can not upload refsql for line with id '{idLigne}':"+str(e))
    except Exception as e:
                logging.error(f"Can not synthesize refsql for line with id '{idLigne}':"+str(e))           






        
        
remplir_ref_sql(Parametres.dateref,Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)   
    







        


    