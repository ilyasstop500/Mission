############################################################################################################
# Module 2 : Generateur du lineage des résultats
# Auteur : Ilyass
# dernier Maj : 07/08/2024 
############################################################################################################

from ConDB import con_to_db
from operator import itemgetter
from datetime import datetime
import Parametres
import json

def get_linegage(idCol,idRow,cnx,cur) : 

    
    
    query =(f"SELECT DISTINCT PERIMETRE  FROM dmrc_fact WHERE idCols = {idCol}" )
    cur.execute(query)
    Perimetre_check = cur.fetchone()[0]
    
    if Perimetre_check == 'SOURCE' : 
        
        query = (f"SELECT Value,Origine,Formule_valo,liste_composants FROM dmrc_lineage WHERE idCols ='{idCol}' AND idRows = '{idRow}'")
        print(query)
        cur.execute(query)
        list_lineage = cur.fetchall()[0]
        print(list_lineage)
        Value = list_lineage[0]
        orgn = list_lineage[1]
        Form = list_lineage[2]
        compo = list_lineage[3]
        
        query =(f"INSERT INTO dmrc_lineage_FINAL ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
        values = (idCol,idRow,Value,orgn,Form,compo)
        cur.execute(query,values)
        cnx.commit()
        
    else : 
        
        query = (f"SELECT Value,Origine,Formule_valo,liste_composants FROM dmrc_lineage WHERE idCols ='{idCol}' AND idRows = '{idRow}'")
        cur.execute(query)
        list_lineage = cur.fetchall()[0]
        print(list_lineage)
        Value = list_lineage[0]
        orgn = list_lineage[1]
        Form = list_lineage[2]
        compo = list_lineage[3]
        
        query =(f"INSERT INTO dmrc_lineage_FINAL ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
        values = (idCol,idRow,Value,orgn,Form,compo)
        cur.execute(query,values)
        cnx.commit()
        
        query =(f"SELECT DISTINCT idColsSrc FROM PRM_COLS_COMPOSANT WHERE idColsCib = {idCol}" )
        cur.execute(query)
        list_of_components = cur.fetchall()
        for i in range(len(list_of_components)) : 
            list_of_components [i] = list_of_components[i][0]
        print("list_comp",list_of_components)
        
        for comp in list_of_components :
            
            try : 
                  
                query = (f"SELECT idObjet FROM PRM_COLS WHERE idCols = '{comp}'")
                cur.execute(query)
                test1 = cur.fetchone()[0]
                print(test1)
                
                query = (f"SELECT idObjet FROM PRM_COLS WHERE idCols = '{idCol}'")
                cur.execute(query)
                test2 = cur.fetchone()[0]
                print(test2)
                
                if test1 != test2 :
                    if idRow == 4 : 
                        idRow = 0
                    elif idRow == 5 : 
                        idRow = 1
                    elif idRow == 6 : 
                        idRow = 2
                    elif idRow == 7 : 
                        idRow = 3
                print(idRow)
                print(comp)
                get_linegage(comp,idRow,cnx,cur)
            
            except :
                continue
        
        
        
        
        
        
        
        
        
    
    
    

    


cnx = con_to_db(Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name) #con to db 
cur = cnx.cursor()




query = ("SELECT DISTINCT idCols,idRows FROM dmrc_fact")
cur.execute(query)
list_cellule = cur.fetchall()


for cellule in list_cellule :
    print(cellule)
    query =(f"INSERT INTO dmrc_lineage_FINAL ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
    values =(None,None,None,None,None,None)
    cur.execute(query,values)
    cnx.commit()
    get_linegage(cellule[0],cellule[1],cnx,cur)
    


    