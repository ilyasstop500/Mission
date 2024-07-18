############################################################################################################
# Module P1 : générateur SQL
# Auteur : Ilyass
# date : Avril 2024 
############################################################################################################



from CsvRead import modify_specific_lines
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
    
   

    logging.debug(f"populating '{schema}' schema's refsql  with dateref '{dateref}'")

    #connexion a la bdd
    cnx = con_to_db(user,pwd,ip,schema)
    cur = cnx.cursor()

    #test affichage des tables
    query = ("SHOW TABLES")
    cur.execute(query)  
    for elem in cur :
        print (elem)
  
    query = ("SELECT  idObjet,idCols,COLS_CODE,COLS_DATAMART FROM PRM_COLS  WHERE COLS_NATURE = 'SOURCE' ")
    cur.execute(query)
    list_of_cols = cur.fetchall()
    value = Parametres.next_sql_id
    for col in list_of_cols : 
        query = (f"SELECT idObjet,idRows,ROWS_CODE,ROWS_NIV,ROWS_ORDR FROM PRM_ROWS WHERE idObjet = '{col[0]}'")
        cur.execute(query)
        list_of_rows = cur.fetchall()
        print("list_of_rows", list_of_rows)
        for row in list_of_rows :
            # --------------------- fetching datamart ----------------------------------------------------------------------
            datamart = col[3]
            idCol = col[1]
            idRow = row[1]
            # fetching the sql_model that corresponds to the datamart 
            query = (f"SELECT TEXT_SQL FROM PRM_SQL_MODEL WHERE INDI_CODE_SQL = '{datamart}'")
            cur.execute(query)
            for elem in cur :
                sql_code = elem[0].strip()
            sql_code = sql_code.replace('""','"')[0:-1] # removes the extra "" added by the mysql db 
            
            SQL_CODE_SRC = sql_code
            idObjet = col[0]
            COL = col[2]
            ROW = row[2]
            PERD = "M0N0"
            PERIMETRE = "SOURCE"
            DATE_TRT = now = datetime.now()

            if len(str(ROW)) == 0 :
                logging.warning("ROWCODE field is Empty")
            if len(str(COL)) == 0 :
                logging.warning("COLCODE field is Empty")
            if len(str(PERD)) == 0 :
                logging.error("PERD field is Empty")
                continue


            NIV = row[3]

            if len(str(NIV)) == 0 :
                logging.warning("NIV field is Empty")
                continue


            query = (f" SELECT RAPPR_CODE,PAGE_CODE,OBJT_CODE,OBJT_TYPE,OBJT_LIBL,TITRE_OBJET FROM PRM_TDB_OBJETS WHERE idObjet = {idObjet}")
            cur.execute(query)
            
            objet_vars = cur.fetchone()[0] 

            #----------------------------------extracting cols parametres--------------------------------------------------#
            #we start by sorting all the rows in the prm_cols_filtre so that we only get the filtres bound to our column
            query = (f"SELECT idObjet,idCols,COLS_CODE,COLS_FILTRE_DOMAINE,DIM_CODE,DIM_VAL_CODE,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_COLS_FILTRE WHERE  idCols ='{idCol}'")
            cur.execute(query)
            list_of_filters = []
            for elem in cur :
                list_of_filters.append(elem)
            
            print(idCol)
            #afterwards we check all the filre_cha and add them to a list
            query = (f"SELECT DISTINCT FILTRE_CHA FROM PRM_COLS_FILTRE WHERE  idCols ='{idCol}'")
            cur.execute(query)
            list_of_cha = []
            for elem in cur : 
                list_of_cha.append(elem)

            print (list_of_cha)
            
            total_condition_col = ""

            #and then we go through this list of filtre_cha one by one writing the condition to respect of each one depending on their nature , this will help us in the case where we want to sort a
            #field by including data from more that 1 departement using the   "WHERE departement in (val1,val2,...)" 
            for cha in list_of_cha : 
                
                #for one specific cha we will extract only the cols filters that have it and then write the condition depending on the nature of the value (time,dimension,value) and the sens (include or not )
                query = (f"SELECT idObjet,idCols,COLS_CODE,COLS_FILTRE_DOMAINE,DIM_CODE,DIM_VAL_CODE,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_COLS_FILTRE WHERE  idCols ='{idCol}'  AND FILTRE_CHA = '{cha[0]}' ")
                cur.execute(query)
                list_of_filters_same_cha = []
                for elem in cur : 
                    list_of_filters_same_cha.append(elem)

                print (list_of_filters_same_cha[0][3],list_of_filters_same_cha[0][7])
                
                if list_of_filters_same_cha[0][3] == "TEMPS" and list_of_filters_same_cha[0][9].strip().lower() == "inclure" : 
                
                    condition_temps = f" AND {cha[0]} = "
                    for filtre_same_cha in list_of_filters_same_cha :
                        PERD = filtre_same_cha[8]
                        condition_temps = condition_temps + code_to_date(filtre_same_cha[8],dateref) +","
                    total_condition_col += condition_temps[0:-1] + ""

                elif list_of_filters_same_cha[0][3] == "TEMPS" and list_of_filters_same_cha[0][9].strip().lower() == "exclure" : 
                
                    condition_temps = f" AND NOT {cha[0]} = "
                    for filtre_same_cha in list_of_filters_same_cha :
                        PERD = filtre_same_cha[8]
                        condition_temps = condition_temps + code_to_date(filtre_same_cha[10],dateref) +","
                    total_condition_col += condition_temps[0:-1] + ""
                
                elif list_of_filters_same_cha[0][3] == "DIMENSION" and list_of_filters_same_cha[0][9].strip().lower() == "inclure": 

                    if list_of_filters_same_cha[0][7] == 'CODE_INDC'    :   
                        INDI = list_of_filters_same_cha[0][8] 
                    else   :   
                        condition_dim = f" AND {cha[0]} IN ("
                        for filtre_same_cha in list_of_filters_same_cha :
                            condition_dim = condition_dim +"'"+filtre_same_cha[8] +"'"+","
                        total_condition_col += condition_dim[0:-1] + ")"

                elif list_of_filters_same_cha[0][3] == "DIMENSION" and list_of_filters_same_cha[0][9].strip().lower() == "exclure": 
                    if list_of_filters_same_cha[0][7] == 'CODE_INDC'    :   
                        INDI = list_of_filters_same_cha[0][8] 
                    else :
                        condition_dim = f" AND NOT {cha[0]} IN ("
                        for filtre_same_cha in list_of_filters_same_cha :
                            condition_dim = condition_dim +"'"+filtre_same_cha[8] +"'"+","
                        total_condition_col += condition_dim[0:-1] + ")"

                
                
            #---------------------------------extracting rows parametres ----------------------------------------# .
            #rows follow basically the same logic as the cols just without checking the nature of the value
            query = (f"SELECT idObjet,idRows,ROWS_CODE,DIM_CODE,DIM_VAL_CODE,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_ROWS_FILTRE WHERE  idRows ='{idRow}'")
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
                
                if list_of_filters_same_cha[0][8].strip().lower() == "inclure" : 
                    condition_dim = f" AND {cha[0]} IN ("
                    for filtre_same_cha in list_of_filters_same_cha :
                        condition_dim = condition_dim +"'"+ filtre_same_cha[7] +"'"+","
                    total_condition_row += condition_dim[0:-1] + ")"
                else : 
                    condition_dim = f" AND NOT {cha[0]} IN ("
                    for filtre_same_cha in list_of_filters_same_cha :
                        condition_dim = condition_dim +"'"+ filtre_same_cha[7] +"'"+","
                    total_condition_row += condition_dim[0:-1] + ")"
                
            
        



            #here we create the final sqlref query by substuting the fields with the correct parametres 
            if datamart == "vw_CUBE_VBPCE_APC_FAITS" : 
                sql_code=sql_code.replace("[p_DAR_REF]",code_to_date("[M-00]",dateref)) # code_to_date() is a function that returns a yyyymm date using a code and the today's date
                sql_code=sql_code.replace("[p_objet]",str(idObjet))
                sql_code=sql_code.replace("[p_ROW]",ROW)
                sql_code=sql_code.replace("[p_COL]",COL)
                sql_code=sql_code.replace("[p_PERD]",PERD)
                sql_code=sql_code.replace("[p_CUBE]",datamart)
                print(sql_code)
                sql_code =sql_code.replace("[p_INDI]",INDI)


            position = sql_code.find('GROUP BY')

            sql_code = sql_code[:position] + total_condition_col +total_condition_row + ' ' + sql_code[position:]
            SQL_CODE_FINAL = sql_code +"'" # the final sqlref query
            print(SQL_CODE_FINAL)    
        
            
            

        
        
        
        
        





            #upload to database ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            
            TEMPS = code_to_date("[M-00]",dateref)
            query =("REPLACE INTO prm_ref_sql ""(idSQLLigne,idObjet,idCol,idRow,DAR_REF,PERD,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,NIV)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s,%s)")
            values = (value, idObjet,idCol,idRow,TEMPS, PERD,COL,ROW,SQL_CODE_SRC, SQL_CODE_FINAL, PERIMETRE, DATE_TRT,NIV)
            
            # Execute the query with the values
            cur.execute(query, values)

            # Commit the transaction
            cnx.commit()
            modify_specific_lines(r"C:\Users\ILYASS\Desktop\Stage\Mission\02_Realisation\Code\Parametres.py", 9 ,f"next_sql_id = {value}")
            value = value + 1
            #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

            mydict = {
                "idLigne" : Parametres.next_fact_id , 
                "idObjet" : idObjet ,
                "DAT_REF" : str(TEMPS) ,
                "PERD" : PERD ,
                "COLS_CODE" : COL , 
                "ROWS_CODE" : ROW,
                "SQL_CODE_SRC" : SQL_CODE_SRC ,
                "SQL_CODE_FINAL" : SQL_CODE_FINAL , 
                "PERIMETRE" : PERIMETRE ,
                "DATE_TRT" : str(DATE_TRT)  
            }

            logging.debug(f"uploaded into database'{json.dumps(mydict, indent=4)}'")


        





        
        
remplir_ref_sql(Parametres.dateref,Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)   
    







        


    