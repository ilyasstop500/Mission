from ConDB import con_to_db
from CsvImport import Csv_import 
from CsvImport import Import_All_Csv 
from CsvImport import edit_csv_refsql
from timecode import code_to_date
from datetime import datetime


#connexion a la bdd
cnx = con_to_db("root","1234","127.0.0.1","test1")
cur = cnx.cursor()

#test affichage des tables
query = ("SHOW TABLES")
cur.execute(query)
for elem in cur :
    print (elem)



#read the prm ra liens row by row for each row we calculate a cell 
#for each row  Check if lien valide equals oui 
#then take the RACode  or COLS CODE  
#this depends if we want to use the same Datamart for a col  we use the COlS_CODE 
#then we go and take the good sql model  INDI_COLS_SQL = COLS DATAMART 

query = ("SELECT * FROM PRM_RA_LIENS ")
cur.execute(query)
list_of_links = [] # list_of_links contain the list of (idObjet,idRA,RA_CODE,idColsCib,idRowsCib,COLS_CODE,ROW_CODE,LIEN_VALIDE)
for elem in cur : 
    list_of_links.append(elem)

edit_csv_refsql('idLigne','idObjet','TDB','PAGE','OBJET','DAR_REF','PERD','RA_CODE','COLS_CODE','ROWS_CODE','SQL_CODE_SRC','SQL_CODE_FINAL','PERIMETRE','DATE_TRT','w')

idLigne = 0
for link in list_of_links : 
    #first step : get the correct sql model

    query = (f"SELECT COLS_DATAMART FROM PRM_COLS_FILTRE WHERE idCols = '{link[4]}' LIMIT 1 ")
    cur.execute(query)
    for elem in cur :
        datamart = elem[0]
    #print (datamart)
    query = (f"SELECT TEXT_SQL FROM PRM_SQL_MODEL WHERE INDI_CODE_SQL = '{datamart}'")
    cur.execute(query)
    for elem in cur :
        sql_code = elem[0]
    sql_code = sql_code.replace('""','"')[1:-2] # removes the extra "" added by the mysql db 
    #print (sql_code)

    #second step : set the correct parametres 
    SQL_CODE_SRC = sql_code
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
    
    TDB = prop_list[0][0]
    PAGE = prop_list[0][1]
    OBJET = prop_list[0][2]

        
    
    

    
    #cols
    query = (f"SELECT * FROM PRM_COLS_FILTRE WHERE  idCols ='{link[4]}'")
    cur.execute(query)
    list_of_filters = []
    for elem in cur :
        list_of_filters.append(elem)

    query = (f"SELECT DISTINCT FILTRE_CHA FROM PRM_COLS_FILTRE WHERE  idCols ='{link[4]}'")
    cur.execute(query)
    list_of_cha = []
    for elem in cur : 
        list_of_cha.append(elem)
    
    total_condition_col = ""
    for cha in list_of_cha : 
        

        query = (f"SELECT * FROM PRM_COLS_FILTRE WHERE  idCols ='{link[4]}'  AND FILTRE_CHA = '{cha[0]}' ")
        cur.execute(query)
        list_of_filters_same_cha = []
        for elem in cur : 
            list_of_filters_same_cha.append(elem)
        
        if list_of_filters_same_cha[0][5] == "TEMPS" : 
            
            condition_temps = f" AND {cha[0]} = "
            for filtre_same_cha in list_of_filters_same_cha :
                PERD = filtre_same_cha[10]
                condition_temps = condition_temps + code_to_date(filtre_same_cha[10]) +","
            total_condition_col += condition_temps[0:-1] + ""
        
        elif list_of_filters_same_cha[0][5] == "DIMENSION": 
            
            condition_dim = f" AND {cha[0]} IN ("
            for filtre_same_cha in list_of_filters_same_cha :
                condition_dim = condition_dim + filtre_same_cha[10] +","
            total_condition_col += condition_dim[0:-2] + ")"
        
        elif list_of_filters_same_cha[0][5] == "MESURE":
            INDI = cha[0]
            
        
        


    #rows
    query = (f"SELECT * FROM PRM_ROWS_FILTRE WHERE  idRows ='{link[5]}'")
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
        
        condition_dim = f" AND {cha[0]} IN ("
        for filtre_same_cha in list_of_filters_same_cha :
            condition_dim = condition_dim + filtre_same_cha[6] +","
        total_condition_row += condition_dim[0:-1] + ")"
            
        
    
    #print("this is the final statement" + total_condition_col + total_condition_row)

    sql_code=sql_code.replace("[p_DAR_REF]",code_to_date("[M0N0]"))
    sql_code=sql_code.replace("[p_objet]",str(idObjet))
    sql_code=sql_code.replace("[p_RA]",RA)
    sql_code=sql_code.replace("[p_ROW]",ROW)
    sql_code=sql_code.replace("[p_COL]",COL)
    sql_code=sql_code.replace("[p_PERD]",PERD)
    sql_code=sql_code.replace("[p_CUBE]",datamart)
    sql_code=sql_code.replace("[p_INDI]",INDI)

    position = sql_code.find('GROUP BY')

    sql_code = sql_code[:position] + total_condition_col +total_condition_row + ' ' + sql_code[position:]
    SQL_CODE_FINAL = sql_code
    
    

    edit_csv_refsql(idLigne,idObjet,TDB,PAGE,OBJET,code_to_date("[M0N0]"),PERD,RA,COL,ROW,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,"a")
    idLigne += 1

    







        


    
