############################################################################################################
# Module P1 : Calcul SQL
# Auteur : Ilyass
# date : Mai 2025 
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
    filename=r"Mission\02_Realisation\Code\Logs\ResultCalcul.log"
)






def calculate(formula, list_values):
    result = formula
    for item in list_values:
        result = result.replace(item[0], item[1])
    return eval(result)




def remplir_cube_final_calcul(dateref,user,pwd,ip,schema):

    DARREF = dateref
    
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

    for order in range (1,max_order+1) :  

        query = ("SELECT  idObjet,idCols,COLS_CODE,COLS_DATAMART FROM PRM_COLS  WHERE COLS_NATURE = 'CALCUL' ")
        cur.execute(query)
        list_of_cols = cur.fetchall()
         
        Factid = int(str(Parametres.next_fact_id))
        for col in list_of_cols : 
            query = (f"SELECT idObjet,idRows,ROWS_CODE,ROWS_NIV,ROWS_ORDR FROM PRM_ROWS WHERE idObjet = '{col[0]}'")
            cur.execute(query)
            list_of_rows = cur.fetchall()
            print("list_of_rows", list_of_rows)
            for row in list_of_rows :

                
                    Factid = Factid+ 1 
                    idObjet = col[0]
                    COLCIB = col[1]
                    ROWCIB = row[1]
                    COL = col[2]
                    ROWCODE = row[2]
                    ROW = ROWCODE
                    DATE_TRT = now = datetime.now()
                    PERIMETRE = "CALCUL"

                    
        

    
                    PERD = "NO"

                    query =(f"SELECT COLS_FORMULE FROM PRM_COLS WHERE idCols = '{COLCIB}' ")
                    cur.execute(query)
                    
                        
                    COLS_FORMULE= cur.fetchone()[0].strip()

                    query =(f"SELECT  CALC_FORMULE,CALC_FAMILLE,CALC_TYPE FROM PRM_COLS_CALCUL WHERE CALC_CODE = '{COLS_FORMULE}'")
                    cur.execute(query)
                    placeholder = [] # list_of_links contain the list of (idObjet,idRA,RA_CODE,idColsCib,idRowsCib,COLS_CODE,ROW_CODE,LIEN_VALIDE)
                    for elem in cur : 
                        placeholder.append(elem)
                    print('placeholder' , placeholder)
                    print('placeholder' , placeholder[0])
                    print('placeholder' , placeholder[0])
                    FORMULE = placeholder[0][0]
                    FAMILLE = placeholder[0][1]
                    TYPE = placeholder[0][2].strip()

                    if TYPE == "COLONNE"  : 
                    
                        list_composants = list()
                         
                        cur.execute(query)
                        list_composants = cur.fetchall()
                        #print(list_composants)
                        #print('next')
                        placeholder = list()
                        for composant in list_composants : 
                            idobjet = composant[0]
                            CODE_COMPOSANT = composant[1].strip()
                            COLCODE= composant[2].strip()
                            placeholder.append([idobjet,CODE_COMPOSANT,COLCODE,ROWCODE])
                        print(len(placeholder))
                        list_valeurs = list()
                        for elem in placeholder : 
                            query =(f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{elem[0]}' AND  COLS_CODE = '{elem[2]}' AND ROWS_CODE = '{elem[3]}' AND  DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            print(elem[0],elem[2],elem[3],Parametres.dateref)
                            value = cur.fetchone()[0]
                            list_valeurs.append([elem[1],value])
                            print("LIST VALS ----------------------------------------------" ,len(list_valeurs))
                            if len(list_valeurs) != len(placeholder) :
                                 continue
                            RESULTAT = calculate(FORMULE,list_valeurs)
                        

                        
                
                            query =("REPLACE INTO prm_ref_result ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                            values = (Factid,COLCIB,ROWCIB,idObjet,DARREF, PERD,COL, ROW," ", PERIMETRE,RESULTAT,"STATUS","WOOOOW","VALIDE",FORMULE,FAMILLE,"",DATE_TRT)

                            # Execute the query with the values
                            cur.execute(query,values)
                            # Commit the transaction
                            cnx.commit()

                            # dict containing all value inserted into the db 
                            mydict = {
                                "idFact" : Factid , 
                                "idObjet" : idObjet ,
                                "idCol" : COLCIB,
                                "idRow" : ROWCIB,
                                "DAT_REF" : DARREF ,
                                "PERD" : PERD ,
                                "COLS_CODE" : COL , 
                                "ROWS_CODE" : ROW,
                                "PERIMETRE" : PERIMETRE ,
                                "DATE_TRT" : str(DATE_TRT) , 
                                "VALEUR" : RESULTAT,
                                "MSG" : FORMULE
                                }
            
                            
                            # saving the dict in the log file
                            logging.debug(f"uploaded into database line '{Factid}' with value '{RESULTAT}' '{json.dumps(mydict, indent=4)}'")
                            
                        

                    elif TYPE == "LIGNE" and FAMILLE == "MAX":

                 

                            query = (f"SELECT idObjetSrc, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]

                            query = (f"SELECT idObjet, COLS_CODE, ROWS_CODE, VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND ROWS_NIV = 0 AND  DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            placeholder = list(cur.fetchall())
                            list_lignes = list(placeholder)

                            print(list_lignes)

                            list_valeurs = [float(ligne[3]) for ligne in list_lignes]

                            for i in range(len(list_lignes)):
                                temp_list = list(list_lignes[i])  # Convert tuple to list
                                temp_list[3] = str(max(list_valeurs))  # Modify the element
                                list_lignes[i] = tuple(temp_list)  # Convert list back to tuple
                
                            print(list_lignes)
                            RESULTAT = temp_list[3]  

                        

                    
                            query =("REPLACE INTO prm_ref_result ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                            values = (Factid,COLCIB,ROWCIB,idObjet,DARREF, PERD,COL, ROW," ", PERIMETRE,RESULTAT,"STATUS","WOOOOW","VALIDE",FORMULE,FAMILLE,"",DATE_TRT)

                            # Execute the query with the values
                            cur.execute(query,values)
                            # Commit the transaction
                            cnx.commit()

                            # dict containing all value inserted into the db 
                            mydict = {
                                "idFact" : Factid , 
                                "idObjet" : idObjet ,
                                "idCol" : COLCIB,
                                "idRow" : ROWCIB,
                                "DAT_REF" : DARREF ,
                                "PERD" : PERD ,
                                "COLS_CODE" : COL , 
                                "ROWS_CODE" : ROW,
                                "PERIMETRE" : PERIMETRE ,
                                "DATE_TRT" : str(DATE_TRT) , 
                                "VALEUR" : RESULTAT,
                                "MSG" : FORMULE
                                }
                            
                            # saving the dict in the log file
                            logging.debug(f"uploaded into database line '{Factid}' with value '{RESULTAT}' '{json.dumps(mydict, indent=4)}'")

                    elif TYPE == "LIGNE" and FAMILLE == "MIN":

                       
                            query = (f"SELECT idObjetSrc, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]

                            query = (f"SELECT r.idObjet,r.COLS_CODE,r.ROWS_CODE,r.VALEUR FROM PRM_REF_RESULT r JOIN PRM_COLS c ON r.idCols = c.idCols  WHERE r.idObjet = '{composant[0]}' AND r.COLS_CODE = '{composant[2].strip()}' AND c.ROWS_NIV = 0 AND  r.DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            placeholder = list(cur.fetchall())
                            list_lignes = list(placeholder)

                            print(list_lignes)

                            list_valeurs = [float(ligne[3]) for ligne in list_lignes]

                            for i in range(len(list_lignes)):
                                temp_list = list(list_lignes[i])  # Convert tuple to list
                                temp_list[3] = str(min(list_valeurs))  # Modify the element
                                list_lignes[i] = tuple(temp_list)  # Convert list back to tuple

                            print(list_lignes)
                            RESULTAT = temp_list[3]

                        

                            query =("REPLACE INTO prm_ref_result ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                            values = (Factid,COLCIB,ROWCIB,idObjet,DARREF, PERD,COL, ROW," ", PERIMETRE,RESULTAT,"STATUS","WOOOOW","VALIDE",FORMULE,FAMILLE,"",DATE_TRT)

                            # Execute the query with the values
                            cur.execute(query,values)
                            # Commit the transaction
                            cnx.commit()

                            # dict containing all value inserted into the db 
                            mydict = {
                                "idFact" : Factid , 
                                "idObjet" : idObjet ,
                                "idCol" : COLCIB,
                                "idRow" : ROWCIB,
                                "DAT_REF" : DARREF ,
                                "PERD" : PERD ,
                                "COLS_CODE" : COL , 
                                "ROWS_CODE" : ROW,
                                "PERIMETRE" : PERIMETRE ,
                                "DATE_TRT" : str(DATE_TRT) , 
                                "VALEUR" : RESULTAT,
                                "MSG" : FORMULE
                                }
                            # saving the dict in the log file
                            logging.debug(f"uploaded into database line '{Factid}' with value '{RESULTAT}' '{json.dumps(mydict, indent=4)}'")

                    


                    elif TYPE == "LIGNE" and FAMILLE == "RANG":

                        

                            query = (f"SELECT ROWS_NIV FROM PRM_ROWS WHERE idRows = {ROWCIB}")
                            cur.execute(query)
                            test = cur.fetchone()[0]

                            if int(test) == 1:
                                poids = 1
                                continue

                            

                            # Query to get composant details
                            query = (f"SELECT idObjetSrc, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]

                            # Query to get list of lines
                            query = (f"SELECT a.idObjet, a.COLS_CODE, a.ROWS_CODE, a.VALEUR FROM PRM_REF_RESULT a JOIN PRM_ROWS b ON a.idRows = b.idRows  WHERE a.idObjet = '{composant[0]}' AND a.COLS_CODE = '{composant[2].strip()}' AND b.ROWS_NIV = 0 AND  a.DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            list_lignes = list(cur.fetchall())

                            print(list_lignes)

                            # Sort list_lignes based on the 4th element (VALEUR)
                            list_lignes.sort(key=itemgetter(3))

                            # Convert tuples to lists, modify, and convert back to tuples
                            for i in range(len(list_lignes)):
                                temp_list = list(list_lignes[i])  # Convert tuple to list
                                temp_list[3] = int(i + 1)  # Modify the 4th element
                                list_lignes[i] = tuple(temp_list)  # Convert list back to tuple
                            print(list_lignes)

                            for i in range(len(list_lignes)):
                                
                                if list_lignes[i][2] == ROW :    
                                    testo = list_lignes[i][3]  # Ensure RESULTAT is correctly set for each line

                        

                            query =("REPLACE INTO prm_ref_result ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                            values = (Factid,COLCIB,ROWCIB,idObjet,DARREF, PERD,COL, ROW," ", PERIMETRE,RESULTAT,"STATUS","WOOOOW","VALIDE",FORMULE,FAMILLE,"",DATE_TRT)

                            # Execute the query with the values
                            cur.execute(query,values)
                            # Commit the transaction
                            cnx.commit()

                            # dict containing all value inserted into the db 
                            mydict = {
                                "idFact" : Factid , 
                                "idObjet" : idObjet ,
                                "idCol" : COLCIB,
                                "idRow" : ROWCIB,
                                "DAT_REF" : DARREF ,
                                "PERD" : PERD ,
                                "COLS_CODE" : COL , 
                                "ROWS_CODE" : ROW,
                                "PERIMETRE" : PERIMETRE ,
                                "DATE_TRT" : str(DATE_TRT) , 
                                "VALEUR" : RESULTAT,
                                "MSG" : FORMULE
                                }
                            
                            # saving the dict in the log file
                            logging.debug(f"uploaded into database line '{Factid}' with value '{RESULTAT}' '{json.dumps(mydict, indent=4)}'")



                                
                            
                    
                    

                        
                    elif TYPE == "LIGNE" and FAMILLE == "POIDS":
                       

                            query = (f"SELECT ROWS_NIV FROM PRM_ROWS WHERE idRows = {ROWCIB}")
                            cur.execute(query)
                            test = cur.fetchone()[0]

                            if int(test) == 1:
                                poids = 1
                                continue

                            # Fetch composant details
                            query = (f"SELECT idObjetSrc, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]
                            
                            if not composant:
                                raise ValueError("No composant found for the given idColsCib")

                            # Fetch the numerator (NIV = 0)
                            query = (f"SELECT a.VALEUR FROM PRM_REF_RESULT a JOIN PRM_ROWS b ON a.idRows = b.idRows  WHERE a.idObjet = '{composant[0]}' AND a.COLS_CODE = '{composant[2].strip()}' AND a.ROWS_CODE = '{ROWCODE}' AND b.ROWS_NIV = 0 AND  a.DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            numerateur = cur.fetchone()
                            
                            if numerateur:
                                numerateur = numerateur[0]
                                print("niv 0", numerateur)
                            else:
                                print( "COMPOSANTS : ",composant[0],composant[2].strip() ,Parametres.dateref,ROWCODE)
                                raise ValueError("No result found for the numerator query")

                            # Fetch the denominator (NIV = 1)
                            query = (f"SELECT a.VALEUR FROM PRM_REF_RESULT a JOIN PRM_ROWS b ON a.idRows = b.idRows  WHERE a.idObjet = '{composant[0]}' AND a.COLS_CODE = '{composant[2].strip()}' AND b.ROWS_NIV = 1 AND  a.DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            denumerateur = cur.fetchone()
                            
                            if denumerateur:
                                denumerateur = denumerateur[0]
                                print("niv 1", denumerateur)
                            else:
                                print( "COMPOSANTS : ",composant[0],composant[2].strip() ,Parametres.dateref)
                                
                                

                            # Calculate the weight (poids)
                            try:
                                poids = float(numerateur) / float(denumerateur)
                                print("le poids est", poids)
                            except ZeroDivisionError:
                                raise ValueError("Denominator is zero, cannot divide by zero")
                            except ValueError as e:
                                raise ValueError(f"Error in conversion to float: {e}")
                            
                            RESULTAT = poids

                       
                            query =("REPLACE INTO prm_ref_result ""(idFACTLigne, idCols,idRows,idObjet,DAR_REF,PERD,COLS_CODE,ROWS_CODE,idSQLLigne,PERIMETRE,VALEUR,STATUS,MSG,LIEN_INVALIDE,FORMULE,FORMULE_VALORISE,FORMULE_REF,DATE_TRT)"" VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s,%s,%s)")
                            values = (Factid,COLCIB,ROWCIB,idObjet,DARREF, PERD,COL, ROW," ", PERIMETRE,RESULTAT,"STATUS","WOOOOW","VALIDE",FORMULE,FAMILLE,"",DATE_TRT)

                            # Execute the query with the values
                            cur.execute(query,values)
                            # Commit the transaction
                            cnx.commit()

                            # dict containing all value inserted into the db 
                            mydict = {
                                "idFact" : Factid , 
                                "idObjet" : idObjet ,
                                "idCol" : COLCIB,
                                "idRow" : ROWCIB,
                                "DAT_REF" : DARREF ,
                                "PERD" : PERD ,
                                "COLS_CODE" : COL , 
                                "ROWS_CODE" : ROW,
                                "PERIMETRE" : PERIMETRE ,
                                "DATE_TRT" : str(DATE_TRT) , 
                                "VALEUR" : RESULTAT,
                                "MSG" : FORMULE
                                }
                            # saving the dict in the log file
                            logging.debug(f"uploaded into database line '{Factid}' with value '{RESULTAT}' '{json.dumps(mydict, indent=4)}'")

                                    

                    

        
                
            





remplir_cube_final_calcul(Parametres.dateref,Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)
                    
                    
                    
                    
                    

                    

                    
                    

                    

                    
                    



                    









                

                


      


        
