import os
import string
import csv
from ConDB import  con_to_db
import Parametres
import logging
import random


logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=r"Mission\02_Realisation\Code\Logs\Cols_order.log"
)


def  cols_order(user,pwd,ip,schema) : 

    cnx = con_to_db(user,pwd,ip,schema)
    cur = cnx.cursor()

    query = ("SHOW TABLES")
    cur.execute(query)  
    for elem in cur :
            print (elem)

    query = ("SELECT idCol FROM PRM_COLS ")
    cur.execute(query)

    Columns = cur.fetchall() 
    for i in range(len(Columns)) : 
          Columns[i] = Columns[i][0]   

    
    print(Columns)


    query = ("SELECT idObjet FROM PRM_TDB_OBJETS ")
    cur.execute(query)
    Objet = cur.fetchall() 
    for i in range(len(Objet)) : 
          Objet[i] = Objet[i][0]     
    print(Objet)

    query = ("SELECT DISTINCT idObjetSrc ,idObjetCib FROM PRM_COLS_COMPOSANT")  
    cur.execute(query)
    rules = cur.fetchall()  
    print(rules)

    #Objet_order = list()
    #for rule in rules : 
    #      Objet_order.append(rule[0])
    #      Objet_order.append(rule[1])
    #Objet_order = list(set(Objet_order))
    #print(Objet_order)


    # 5od parent ou trier m3a ga3 wlado 
    # sayeb tous les substrings
    # gadhoum kamlin fla5er
    

    Columns_order = list()
    


    
              
        
              

    
    
    
  
        
    
    


    
              


cols_order(Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)
