############################################################################################################
# Module P1 : générateur SQL
# Auteur : Ilyass
# date : Avril 2024 
############################################################################################################

from ConDB import con_to_db
from CsvImport import Csv_import 
from CsvImport import Import_All_Csv 
from CsvImport import edit_csv_refsql
from timecode import code_to_date
from datetime import datetime
from logs_refsql import log_refsql as log


def remplir_ref_sql (dateref,csvdirect,logsdirect) :

    dict = {
        "FILE" : "LOG OF THE REFSQL PROCESS CONTAINING ALL THE LINES OF SQL THAT HAVE BEEN PROCESSED "
    }
    log(dict,logsdirect,'w')

    #connexion a la bdd
    cnx = con_to_db("root","1234","127.0.0.1","test5")
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

    #we use the edit_csv_refsql to remove all existing rows in the file then we set the first line with the columns names so that we can inserted all the date afterwards
    edit_csv_refsql('idLigne','idObjet','TDB','PAGE','OBJET','DAR_REF','PERD','RA_CODE','COLS_CODE','ROWS_CODE','SQL_CODE_SRC','SQL_CODE_FINAL','PERIMETRE','DATE_TRT','w',logsdirect) 

    idLigne = 0
    for link in list_of_links : # loop that iterates over all the links between columns and rows and contructs the correct sqlref query for each one 

        #-------------------------------------------------------------------- FIRST STEP : GET THE CORRECT SQL MODEL -------------------------------------------------------------------------------


        # fetching the datamart  
        query = (f"SELECT COLS_DATAMART FROM PRM_COLS_FILTRE WHERE idCols = '{link[4]}' LIMIT 1 ") 
        cur.execute(query)
        for elem in cur :
            datamart = elem[0]

        
        # fetching the sql_model that corresponds to the datamart 
        query = (f"SELECT TEXT_SQL FROM PRM_SQL_MODEL WHERE INDI_CODE_SQL = '{datamart}'")
        cur.execute(query)
        for elem in cur :
            sql_code = elem[0]
        sql_code = sql_code.replace('""','"')[0:-1] # removes the extra "" added by the mysql db 


        #-------------------------------------------------------------------- SECOND STEP : FILLING THE SQL MODEL WITH PARAMETRES ------------------------------------------------------------------
        
        SQL_CODE_SRC = sql_code
        idLigne = link[0]
        idObjet = link[1] 
        RA = link[3] 
        COL = link[6]
        ROW = link[7]
        PERD = "M0N0"
        PERIMETRE = "SOURCE"
        DATE_TRT = now = datetime.now()

        query = (f" SELECT TDB,PAGE,OBJET FROM PRM_TDB_OBJETS WHERE idObjet = {idObjet}")
        cur.execute(query)
        prop_list = []
        for elem in cur :
            prop_list.append(elem)
        
        TBD = prop_list[0][0]
        PAGE = prop_list[0][1]
        OBJET = prop_list[0][2]

            
        
        #----------------------------------extracting cols parametres--------------------------------------------------#
        #we start by sorting all the rows in the prm_cols_filtre so that we only get the filtres bound to our column
        query = (f"SELECT COLS_NATURE,RA_CODE,idCols,COLS_CODE,COLS_DATAMART,COLS_FILTRE_DOMAINE,DIM_VAL_CODE,idObjet,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_COLS_FILTRE WHERE  idCols ='{link[4]}'")
        cur.execute(query)
        list_of_filters = []
        for elem in cur :
            list_of_filters.append(elem)
        

        #afterwards we check all the filre_cha and add them to a list
        query = (f"SELECT DISTINCT FILTRE_CHA FROM PRM_COLS_FILTRE WHERE  idCols ='{link[4]}'")
        cur.execute(query)
        list_of_cha = []
        for elem in cur : 
            list_of_cha.append(elem)
        
        total_condition_col = ""

        #and then we go through this list of filtre_cha one by one writing the condition to respect of each one depending on their nature , this will help us in the case where we want to sort a
        #field by including data from more that 1 departement using the   "WHERE departement in (val1,val2,...)" 
        for cha in list_of_cha : 
            
            #for one specific cha we will extract only the cols filters that have it and then write the condition depending on the nature of the value (time,dimension,value) and the sens (include or not )
            query = (f"SELECT COLS_NATURE,RA_CODE,idCols,COLS_CODE,COLS_DATAMART,COLS_FILTRE_DOMAINE,DIM_VAL_CODE,idObjet,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_COLS_FILTRE WHERE  idCols ='{link[4]}'  AND FILTRE_CHA = '{cha[0]}' ")
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
        query = (f"SELECT idObjet,idRows,ROWS_CODE,DIM_VAL_CODE,FILTRE_TAB,FILTRE_CHA,FILTRE_VAL,FILTRE_SENS FROM PRM_ROWS_FILTRE WHERE  idRows ='{link[5]}'")
        cur.execute(query)
        list_of_filters = []
        for elem in cur :
            list_of_filters.append(elem)

        query = (f"SELECT DISTINCT FILTRE_CHA FROM PRM_ROWS_FILTRE WHERE  idRows ='{link[5]}'")
        cur.execute(query)
        list_of_cha = []
        for elem in cur : 
            list_of_cha.append(elem)
        
        total_condition_row = ""
        for cha in list_of_cha :    

            query = (f"SELECT * FROM PRM_ROWS_FILTRE WHERE  idRows ='{link[5]}'  AND FILTRE_CHA = '{cha[0]}' ")
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
        SQL_CODE_FINAL = sql_code # the final sqlref query
        
        
        

        edit_csv_refsql(idLigne,idObjet,TBD,PAGE,OBJET,code_to_date("[M0N0]",dateref),PERD,RA,COL,ROW,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,"a",logsdirect) #we use this function to write all the info into the csv file using the apppend option 'a'
        #print(SQL_CODE_FINAL)




        #upload to database ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        TEMPS = code_to_date("[M0N0]",dateref)
        query =("INSERT INTO prm_ref_sql ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s)")
        values = (idLigne, idObjet, TBD, PAGE,OBJET,TEMPS, PERD, RA, COL, ROW, SQL_CODE_SRC, SQL_CODE_FINAL, PERIMETRE, DATE_TRT)

        # Execute the query with the values
        cur.execute(query, values)

        # Commit the transaction
        cnx.commit()
        #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
        #writing result in log --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        dict = {
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
        log(dict,logsdirect,'a')
    
    







        


    