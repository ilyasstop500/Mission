

from ConDB import con_to_db
from operator import itemgetter
from datetime import datetime
import Parametres
import json

def get_linegage(idCol) : 

    cnx = con_to_db(Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name) #con to db 
    cur = cnx.cursor()

    query =(f"SELECT DISTINCT idColsSrc FROM PRM_COLS_COMPOSANT WHERE idColsCib = {idCol}" )
    print(query)
    cur.execute(query)

    list_of_components = cur.fetchall()

    if len(list_of_components) == 0 : 


        query = f"""
        INSERT INTO PRM_LINEAGE_FINAL (idCols, idRows, Value, Origine, Formule_valo, liste_composants)
        SELECT  idCols, idRows, Value, Origine, Formule_valo, liste_composants 
        FROM PRM_LINEAGE 
        WHERE idCols = {idCol}
        """
        cur.execute(query)
        cnx.commit()
        print("no lineage anymore")
    else :
        print(f"lineage for Column {idCol} is  : " , list_of_components)
        query = f"""
        INSERT INTO PRM_LINEAGE_FINAL (idCols, idRows, Value, Origine, Formule_valo, liste_composants)
        SELECT  idCols, idRows, Value, Origine, Formule_valo, liste_composants 
        FROM PRM_LINEAGE 
        WHERE idCols = {idCol}
        """
        print(query)
        cur.execute(query)
        cnx.commit()

        for component in list_of_components :
            if component == None : 
                break
            get_linegage(component[0])



cnx = con_to_db(Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name) #con to db 
cur = cnx.cursor()


query = ("SELECT DISTINCT idCols FROM PRM_COLS WHERE COLS_NATURE ='CALCUL' ")
cur.execute(query)
list_cols = cur.fetchall()

for col in list_cols :
    print(col[0])
    get_linegage(col[0])


