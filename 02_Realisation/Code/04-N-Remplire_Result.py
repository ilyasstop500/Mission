############################################################################################################
# Module 4 : Calcul des colonnes composées
# Auteur : Ilyass
# dernier Maj : 07/08/2024 
############################################################################################################


from ConDB import con_to_db
from operator import itemgetter
from datetime import datetime
import Parametres
import json


import logging


logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=r"C:\Users\ILYASS\Desktop\Stage\Mission\02_Realisation\Code\Logs\ResultCalcul.log"
)






def calculate(formula, list_values):
    try : 
        result = formula
        for item in list_values:
            result = result.replace(item[0], item[1])
        return (eval(result),"GOOD","O K",result)
    except ZeroDivisionError as e : 
        print("cant divide by 0")
        return (0,"cant divide by 0","K O",result)
    except :
        print("error")
        return (0,"cant eval","K O",result) 

def remplir_cube_final_calcul(dateref,user,pwd,ip,schema):

    DARREF = dateref
    
    

    # Connexion a la bdd
    cnx = con_to_db(user,pwd,ip,schema) #con to db 

    cur = cnx.cursor()
    query = ("SHOW TABLES")
    cur.execute(query)
    for elem in cur :
        print (elem)

    query = ("SELECT DISTINCT COLS_ORDRE FROM prm_cols WHERE COLS_NATURE = 'CALCUL'")
    cur.execute(query)
    max_order = max(list(cur.fetchall()))[0]
    print(max_order)
    Factid = int(str(Parametres.next_fact_id))
    for order in range (1,max_order+1) :  

        query = (f"SELECT  idObjet,idCols,COLS_CODE,COLS_DATAMART FROM PRM_COLS  WHERE COLS_NATURE = 'CALCUL' AND COLS_ORDRE = {order}")
        cur.execute(query)
        list_of_cols = cur.fetchall()

        for col in list_of_cols :
            query = (f"SELECT idObjet,idRows,ROWS_CODE,ROWS_NIV,ROWS_ORDR FROM PRM_ROWS WHERE idObjet = '{col[0]}'")
            cur.execute(query)
            list_of_rows = cur.fetchall()
            for row in list_of_rows :
                Factid = Factid+ 1 
                idObjet = col[0]
                idColscib = col[1]
                ColCode = col[2]
                RowCOde = row[2]
                idRow = row[1]
                RowNiv = row[3]
                PERD = 'NO'
                TimeTRT = datetime.now()

                # SELECTING THE CORRECT FORMULA
                query =(f"SELECT COLS_FORMULE FROM PRM_COLS WHERE idCols = '{idColscib}' ")
                cur.execute(query)        
                COLS_FORMULE= cur.fetchone()[0].strip()

                # GETTING ADDITIONAL INFO ABOUT THE FORMULA
                query =(f"SELECT  CALC_FORMULE,CALC_FAMILLE,CALC_TYPE FROM PRM_COLS_CALCUL WHERE CALC_CODE = '{COLS_FORMULE}'")
                cur.execute(query)
                formule_info = [] # list_of_links contain the list of (idObjet,idRA,RA_CODE,idColsCib,idRowsCib,COLS_CODE,ROW_CODE,LIEN_VALIDE)
                for elem in cur : 
                    formule_info.append(elem)
                FORMULE = formule_info[0][0]
                FAMILLE = formule_info[0][1]
                TYPE = formule_info[0][2].strip()    

                #-------------------------------------------------------------------------------FORMULE GENERALE TYPE COLONNE ---------------------------------------------------------------------------------------------------------------

                if TYPE == "COLONNE"  : 

                    # GETTING ALL COMPONENTS 
                    list_composants = list()
                    query =(f"SELECT  CODE_COMPOSANT,idColsSrc FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{idColscib}'")
                    cur.execute(query)
                    list_composants = cur.fetchall()
                    print("list compo" , list_composants,idColscib)
                    print(type(list_composants))
                    FORMULE_REF = str(list_composants)
                    list_valeurs = list()
                    # getting values of each components then putting them in a list
                    for composant in list_composants : 
                        query =(f"SELECT  VALEUR FROM dmrc_fact WHERE idCols = '{composant[1]}' AND DAR_REF = '{Parametres.dateref}'")
                        cur.execute(query)
                        rows_values = list()
                        for elem in cur : 
                            rows_values.append(elem[0])
                        print(rows_values)
                        if len(rows_values) < 4 :
                            rows_values.append('0')
                        if idRow - 4 < 0 : 
                            AN = rows_values[idRow]
                        else : 
                            AN = rows_values[idRow-4]
                        list_valeurs.append((composant[0],AN))
                    print(list_valeurs)
                    
                    #calling the calculate function
                    
                    RESULTAT = calculate(FORMULE,list_valeurs)[0]
                    MSG = calculate(FORMULE,list_valeurs)[1]
                    STATUS = calculate(FORMULE,list_valeurs)[2]
                    FORMULE_VALO = calculate(FORMULE,list_valeurs)[3]
                    print("RESULTAT" , RESULTAT)

                    # uploading the result into the DMRC_FACT table
                    query =("REPLACE INTO dmrc_fact ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                    values = (Factid,idColscib,idRow,idObjet,DARREF, PERD,ColCode,RowCOde," ", "CALCUL",RESULTAT,STATUS,MSG,"VALIDE",FAMILLE,FORMULE_VALO,FORMULE_REF,TimeTRT)

                    # Execute the query with the values
                    cur.execute(query,values)

                    query = (f"INSERT INTO dmrc_lineage ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
                    values = (idColscib,idRow,RESULTAT,"CALCUL",FORMULE_VALO,FORMULE_REF)
                    cur.execute(query,values)


                    # Commit the transaction
                    cnx.commit()

                    # dict containing all value inserted into the db 
                    mydict = {
                        "idFact" : Factid , 
                        "idObjet" : idObjet ,
                        "idCol" : idColscib,
                        "idRow" : idRow,
                        "DAT_REF" : DARREF ,
                        "PERD" : PERD ,
                        "COLS_CODE" : ColCode , 
                        "ROWS_CODE" : RowCOde,
                        "PERIMETRE" : "CALCUL" ,
                        "DATE_TRT" : str(TimeTRT) , 
                        "VALEUR" : RESULTAT,
                        "MSG" : FORMULE
                        }
    
                    
                    # saving the dict in the log file
                    logging.debug(f"uploaded into database line '{Factid}' with value '{RESULTAT}' '{json.dumps(mydict, indent=4)}'")
                    
                    
                    
                # ----------------------------------------------------------------------- MAX -------------------------------------------------------------------------------------------------------------------------------------------
                elif TYPE == "LIGNE" and FAMILLE == "MAX":

                    query = (f"SELECT ROWS_NIV FROM PRM_ROWS WHERE idRows = {idRow}")
                    cur.execute(query)
                    test = cur.fetchone()[0]

                    #testing if the niv of the row is not 1 
                    if int(test) == 1:
                        poids = 1
                        continue
                    
                    
                    #getting the source column
                    query =(f"SELECT  idColsSrc FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{idColscib}'")
                    cur.execute(query)
                    composant = cur.fetchone()[0]

                    #fetching all the values of the source column and putting them in a list
                    query =(f"SELECT  a.VALEUR FROM dmrc_fact a JOIN PRM_ROWS b ON a.idRows = b.idRows WHERE a.idCols = '{composant}' AND a.DAR_REF = '{Parametres.dateref}' AND b.ROWS_NIV = '0'")
                    cur.execute(query)
                    list_valeurs = list()
                    for elem in cur : 
                        list_valeurs.append(elem[0])
                        
                    #max
                    RESULTAT = max(list_valeurs)

                    query =("REPLACE INTO dmrc_fact ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                    values = (Factid,idColscib,idRow,idObjet,DARREF, PERD,ColCode,RowCOde," ", "CALCUL",RESULTAT,"STATUS","WOOOOW","VALIDE",FAMILLE,FORMULE,"",TimeTRT)



                    query = (f"INSERT INTO dmrc_lineage ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
                    values = (idColscib,idRow,RESULTAT,"CALCUL",FAMILLE,FORMULE_REF)
                    cur.execute(query,values)


                    # Execute the query with the values
                    cur.execute(query,values)
                    # Commit the transaction
                    cnx.commit()

                    # dict containing all value inserted into the db 
                    mydict = {
                        "idFact" : Factid , 
                        "idObjet" : idObjet ,
                        "idCol" : idColscib,
                        "idRow" : idRow,
                        "DAT_REF" : DARREF ,
                        "PERD" : PERD ,
                        "COLS_CODE" : ColCode , 
                        "ROWS_CODE" : RowCOde,
                        "PERIMETRE" : "CALCUL" ,
                        "DATE_TRT" : str(TimeTRT) , 
                        "VALEUR" : RESULTAT,
                        "MSG" : FORMULE
                        }
                # ----------------------------------------------------------------------- MIN -------------------------------------------------------------------------------------------------------------------------------------------   
                elif TYPE == "LIGNE" and FAMILLE == "MIN":


                    query = (f"SELECT ROWS_NIV FROM PRM_ROWS WHERE idRows = {idRow}")
                    cur.execute(query)
                    test = cur.fetchone()[0]
                    
                    #testing if the niv of the row is not 1
                    if int(test) == 1:
                        poids = 1
                        continue
                    
                    #getting the source column
                    query =(f"SELECT  idColsSrc FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{idColscib}'")
                    cur.execute(query)
                    composant = cur.fetchone()[0]
                    
                    #fetching all the values of the source column and putting them in a list
                    query =(f"SELECT  a.VALEUR FROM dmrc_fact a JOIN PRM_ROWS b ON a.idRows = b.idRows WHERE a.idCols = '{composant}' AND a.DAR_REF = '{Parametres.dateref}' AND b.ROWS_NIV = '0'")
                    cur.execute(query)
                    list_valeurs = list()
                    for elem in cur : 
                        list_valeurs.append(elem[0])
                    #min
                    RESULTAT = max(list_valeurs)

                    query =("REPLACE INTO dmrc_fact ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                    values = (Factid,idColscib,idRow,idObjet,DARREF, PERD,ColCode,RowCOde," ", "CALCUL",RESULTAT,"STATUS","WOOOOW","VALIDE",FAMILLE,FORMULE,"",TimeTRT)

                    # Execute the query with the values
                    cur.execute(query,values)

                    query = (f"INSERT INTO dmrc_lineage ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
                    values = (idColscib,idRow,RESULTAT,"CALCUL",FAMILLE,FORMULE_REF)
                    cur.execute(query,values)

                    # Commit the transaction
                    cnx.commit()

                    # dict containing all value inserted into the db 
                    mydict = {
                        "idFact" : Factid , 
                        "idObjet" : idObjet ,
                        "idCol" : idColscib,
                        "idRow" : idRow,
                        "DAT_REF" : DARREF ,
                        "PERD" : PERD ,
                        "COLS_CODE" : ColCode , 
                        "ROWS_CODE" : RowCOde,
                        "PERIMETRE" : "CALCUL" ,
                        "DATE_TRT" : str(TimeTRT) , 
                        "VALEUR" : RESULTAT,
                        "MSG" : FORMULE
                        }
                # ----------------------------------------------------------------------- RANG-------------------------------------------------------------------------------------------------------------------------------------------   
                elif TYPE == "LIGNE" and FAMILLE == 'RANG':

                    query = (f"SELECT ROWS_NIV FROM PRM_ROWS WHERE idRows = {idRow}")
                    cur.execute(query)
                    test = cur.fetchone()[0]

                    if int(test) == 1:
                        poids = 1
                        continue

                    query =(f"SELECT  idColsSrc FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{idColscib}'")
                    cur.execute(query)
                    composant = cur.fetchone()[0]

                    query =(f"SELECT  a.VALEUR,a.idRows FROM dmrc_fact a JOIN PRM_ROWS b ON a.idRows = b.idRows  WHERE a.idCols = '{composant}' AND a.DAR_REF = '{Parametres.dateref}' AND b.ROWS_NIV = '0'")
                    cur.execute(query)
                    list_valeurs = list()
                    for elem in cur : 
                        list_valeurs.append((idColscib,elem[1],float(elem[0])))
                    list_valeurs.sort(key=itemgetter(2))
                    list_valeurs.reverse()
                    for i in range (len(list_valeurs)) :
                        if list_valeurs[i][0] == idColscib and list_valeurs[i][1] == idRow :
                            RESULTAT = i+1
                    
                    query =("REPLACE INTO dmrc_fact ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                    values = (Factid,idColscib,idRow,idObjet,DARREF, PERD,ColCode,RowCOde," ", "CALCUL",RESULTAT,"STATUS","WOOOOW","VALIDE",FAMILLE,FORMULE,"",TimeTRT)

                    # Execute the query with the values
                    cur.execute(query,values)

                    query = (f"REPLACE INTO dmrc_lineage ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
                    values = (idColscib,idRow,RESULTAT,"CALCUL",FAMILLE,composant)
                    cur.execute(query,values)

                    # Commit the transaction
                    cnx.commit()

                    # dict containing all value inserted into the db 
                    mydict = {
                        "idFact" : Factid , 
                        "idObjet" : idObjet ,
                        "idCol" : idColscib,
                        "idRow" : idRow,
                        "DAT_REF" : DARREF ,
                        "PERD" : PERD ,
                        "COLS_CODE" : ColCode , 
                        "ROWS_CODE" : RowCOde,
                        "PERIMETRE" : "CALCUL" ,
                        "DATE_TRT" : str(TimeTRT) , 
                        "VALEUR" : RESULTAT,
                        "MSG" : FORMULE
                        }
                # ----------------------------------------------------------------------- POIDS -------------------------------------------------------------------------------------------------------------------------------------------    
                elif TYPE == "LIGNE" and FAMILLE == 'POIDS':

                    query = (f"SELECT ROWS_NIV FROM PRM_ROWS WHERE idRows = {idRow}")
                    cur.execute(query)
                    test = cur.fetchone()[0]

                    if int(test) == 1:
                        poids = 1
                        continue

                    query =(f"SELECT  idColsSrc FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{idColscib}'")
                    cur.execute(query)
                    composant = cur.fetchone()[0]

                    if idRow - 4 < 0 : 
                        idRow2 = idRow
                    else : 
                        idRow2 = idRow - 4

                    query =(f"SELECT  a.VALEUR FROM dmrc_fact a JOIN PRM_ROWS b ON a.idRows = b.idRows  WHERE a.idCols = '{composant}' AND a.DAR_REF = '{Parametres.dateref}' AND b.ROWS_NIV = '0' AND a.idRows = '{idRow2}'")
                    cur.execute(query)
                    numerateur = cur.fetchone()[0]


                    query =(f"SELECT  a.VALEUR FROM dmrc_fact a JOIN PRM_ROWS b ON a.idRows = b.idRows  WHERE a.idCols = '{composant}' AND a.DAR_REF = '{Parametres.dateref}' AND b.ROWS_NIV = '1' ")
                    cur.execute(query)
                    denominateur = cur.fetchone()[0]


                    # Calculate the weight (poids)
                    try:
                        poids = float(numerateur) / float(denominateur)
                        print("le poids est", poids)
                    except ZeroDivisionError:
                        raise ValueError("Denominator is zero, cannot divide by zero")
                    except ValueError as e:
                        raise ValueError(f"Error in conversion to float: {e}")
                    
                    RESULTAT = poids

                    query =("REPLACE INTO dmrc_fact ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                    values = (Factid,idColscib,idRow,idObjet,DARREF, PERD,ColCode,RowCOde," ", "CALCUL",RESULTAT,"STATUS","WOOOOW","VALIDE",FAMILLE,FORMULE,"",TimeTRT)

                    # Execute the query with the values
                    cur.execute(query,values)

                    query = (f"REPLACE INTO dmrc_lineage ""(idCols,idRows,Value,Origine,Formule_valo,liste_composants)"" VALUES (%s,%s,%s,%s,%s,%s)")
                    values = (idColscib,idRow,RESULTAT,"CALCUL",FAMILLE,composant)
                    cur.execute(query,values)


                    # Commit the transaction
                    cnx.commit()

                    # dict containing all value inserted into the db 
                    mydict = {
                        "idFact" : Factid , 
                        "idObjet" : idObjet ,
                        "idCol" : idColscib,
                        "idRow" : idRow,
                        "DAT_REF" : DARREF ,
                        "PERD" : PERD ,
                        "COLS_CODE" : ColCode , 
                        "ROWS_CODE" : RowCOde,
                        "PERIMETRE" : "CALCUL" ,
                        "DATE_TRT" : str(TimeTRT) , 
                        "VALEUR" : RESULTAT,
                        "MSG" : FORMULE
                        }

                     


                    
                    











                    
                    


                
                
                    
                





remplir_cube_final_calcul(Parametres.dateref,Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)






