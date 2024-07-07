############################################################################################################
# Module P1 : Calcul SQL
# Auteur : Ilyass
# date : Mai 2025 
############################################################################################################


from ConDB import con_to_db
from operator import itemgetter
from datetime import datetime
import Parametres


import logging


logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="ResultCalcul.log"
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

        query = (f"SELECT DISTINCT r.idRACible,r.idObjet,r.idRA,r.RA_Code,r.idColsCib,r.idRowsCib,r.COLS_CODE,r.ROWS_CODE,r.LIEN_VALIDE,c.COLS_NATURE FROM PRM_RA_LIENS r JOIN PRM_COLS c ON r.idColsCib = c.idCol WHERE r.LIEN_VALIDE ='OUI\r' AND c.COLS_NATURE = 'CALCUL' AND c.COLS_ORDRE = {order} " )
        cur.execute(query)
        list_of_links = [] # list_of_links contain the list of (idObjet,idRA,RA_CODE,idColsCib,idRowsCib,COLS_CODE,ROW_CODE,LIEN_VALIDE)
        for elem in cur : 
            list_of_links.append(elem)
            print("link : " , elem)

        try : 
        
            for link in list_of_links : #
                try : 
                    idLigne = str(link[0]) + "/" +Parametres.dateref
                    idObjet = link[1]
                    RA = link[2]
                    COLCIB = link[4]
                    ROWCIB = link[5]
                    COL = link[6]
                    ROWCODE = link[7]
                    ROW = ROWCODE
                    DATE_TRT = now = datetime.now()
                    PERIMETRE = "CALCUL"

                    query = (f" SELECT TDB,PAGE,OBJET FROM PRM_TDB_OBJETS WHERE idObjet = {idObjet}")
                    cur.execute(query)
                    prop_list = []
                    for elem in cur :
                        prop_list.append(elem)
                    
                    TBD = prop_list[0][0]
                    PAGE = prop_list[0][1]
                    OBJET = prop_list[0][2]

                    query = (f" SELECT FILTRE_VAL FROM PRM_COLS_FILTRE WHERE FILTRE_CHA = 'PERD_ARRT_INFO' AND idCols = '{COLCIB}'")
                    cur.execute(query)
                    PERD = cur.fetchone()[0]

                    query =(f"SELECT COLS_FORMULE FROM PRM_COLS WHERE idCol = '{COLCIB}' ")
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
                        try  :
                            list_composants = list()
                            query =(f"SELECT  idObjet,CODE_COMPOSANT,COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
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
                            
                            list_valeurs = list()
                            for elem in placeholder : 
                                query =(f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{elem[0]}' AND  COLS_CODE = '{elem[2]}' AND ROWS_CODE = '{elem[3]}' AND  DAR_REF = '{Parametres.dateref}'")
                                cur.execute(query)
                                value = cur.fetchone()[0]
                                list_valeurs.append([elem[1],value])
                            
                            
                                RESULTAT = calculate(FORMULE,list_valeurs)
                        except Exception as e :  
                            logging.error(f"Can not calculate value for line with id '{idLigne}'"+str(e))
                            continue

                        
                
                        query =("REPLACE INTO prm_ref_result ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,VALEUR,FORMULE,NIV)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s)")
                        values = (idLigne, idObjet, TBD, PAGE,OBJET,DARREF,PERD, RA, COL, ROW, "", "", PERIMETRE, DATE_TRT,RESULTAT,FORMULE+FAMILLE,1)
                    

                        # Execute the query with the values
                        cur.execute(query,values)
                        # Commit the transaction
                        cnx.commit()
                            
                        

                    elif TYPE == "LIGNE" and FAMILLE == "MAX":

                        try : 

                            query = (f"SELECT idObjet, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]

                            query = (f"SELECT idObjet, COLS_CODE, ROWS_CODE, VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND NIV = 0 AND  DAR_REF = '{Parametres.dateref}'")
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

                        except Exception as e :  
                            logging.error(f"Can not calculate value for line with id '{idLigne}'"+str(e))
                            continue

                    
                        query =("REPLACE INTO prm_ref_result ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,VALEUR,FORMULE,NIV)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s)")
                        values = (idLigne, idObjet, TBD, PAGE,OBJET,DARREF,PERD, RA, COL, ROW, "", "", PERIMETRE, DATE_TRT,RESULTAT,FAMILLE,1)
                    

                        # Execute the query with the values
                        cur.execute(query,values)
                        # Commit the transaction
                        cnx.commit()

                    elif TYPE == "LIGNE" and FAMILLE == "MIN":

                        try : 
                            query = (f"SELECT idObjet, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]

                            query = (f"SELECT idObjet, COLS_CODE, ROWS_CODE, VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND NIV = 0 AND  DAR_REF = '{Parametres.dateref}'")
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

                        except Exception as e :  
                            logging.error(f"Can not calculate value for line with id '{idLigne}'"+str(e))
                            continue

                        query =("REPLACE INTO prm_ref_result ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,VALEUR,FORMULE,NIV)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s)")
                        values = (idLigne, idObjet, TBD, PAGE,OBJET,DARREF,PERD, RA, COL, ROW, "", "", PERIMETRE, DATE_TRT,RESULTAT,FAMILLE,1)
                    

                        # Execute the query with the values
                        cur.execute(query,values)
                        # Commit the transaction
                        cnx.commit()

                    


                    elif TYPE == "LIGNE" and FAMILLE == "RANG":

                        try : 

                            # Fetch NIV for the given ROWCODE
                            query = (f"SELECT NIV FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND ROWS_CODE = '{ROWCODE}' AND  DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            test = cur.fetchone()
                            
                            if test:
                                test = test[0]
                                print('test', test)
                            else:
                                raise ValueError("No result found for the given idObjet, COLS_CODE, and ROWS_CODE")

                            if int(test) == 1:
                                poids = 1
                                continue

                            # Query to get composant details
                            query = (f"SELECT idObjet, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]

                            # Query to get list of lines
                            query = (f"SELECT idObjet, COLS_CODE, ROWS_CODE, VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND NIV = 0 AND  DAR_REF = '{Parametres.dateref}'")
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

                        except Exception as e :  
                            logging.error(f"Can not calculate value for line with id '{idLigne}'"+str(e))
                            continue

                        query = ("REPLACE INTO prm_ref_result "
                                "(idLigne, idObjet, TBD, PAGE, OBJET, DAR_REF, PERD, RA_CODE, COLS_CODE, ROWS_CODE, SQL_CODE_SRC, SQL_CODE_FINAL, PERIMETRE, DATE_TRT, VALEUR, FORMULE, NIV) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s)")
                        values = (idLigne, idObjet, TBD, PAGE, OBJET, DARREF, PERD, RA, COL, ROW, "", "", PERIMETRE, DATE_TRT, testo, FAMILLE , 1)

                        # Execute the query with the values
                        cur.execute(query, values)
                        # Commit the transaction
                        cnx.commit()



                                
                            
                    
                    

                        
                    elif TYPE == "LIGNE" and FAMILLE == "POIDS":
                        try : 

                            # Fetch NIV for the given ROWCODE
                            query = (f"SELECT NIV FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND ROWS_CODE = '{ROWCODE}' AND  DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            test = cur.fetchone()
                            
                            if test:
                                test = test[0]
                                print('test', test)
                            else:
                                raise ValueError("No result found for the given idObjet, COLS_CODE, and ROWS_CODE")

                            if int(test) == 1:
                                poids = 1
                                continue

                            # Fetch composant details
                            query = (f"SELECT idObjet, CODE_COMPOSANT, COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
                            cur.execute(query)
                            composant = cur.fetchall()[0]
                            
                            if not composant:
                                raise ValueError("No composant found for the given idColsCib")

                            # Fetch the numerator (NIV = 0)
                            query = (f"SELECT VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND ROWS_CODE = '{ROWCODE}' AND NIV = 0 AND  DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            numerateur = cur.fetchone()
                            
                            if numerateur:
                                numerateur = numerateur[0]
                                print("niv 0", numerateur)
                            else:
                                raise ValueError("No result found for the numerator query")

                            # Fetch the denominator (NIV = 1)
                            query = (f"SELECT VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND COLS_CODE = '{composant[2].strip()}' AND NIV = 1 AND  DAR_REF = '{Parametres.dateref}'")
                            cur.execute(query)
                            denumerateur = cur.fetchone()
                            
                            if denumerateur:
                                denumerateur = denumerateur[0]
                                print("niv 1", denumerateur)
                            else:
                                raise ValueError("No result found for the denominator query")

                            # Calculate the weight (poids)
                            try:
                                poids = float(numerateur) / float(denumerateur)
                                print("le poids est", poids)
                            except ZeroDivisionError:
                                raise ValueError("Denominator is zero, cannot divide by zero")
                            except ValueError as e:
                                raise ValueError(f"Error in conversion to float: {e}")
                            
                            RESULTAT = poids

                        except Exception as e :  
                            logging.error(f"Can not calculate value for line with id '{idLigne}'"+str(e))
                            continue
                        
                        query =("REPLACE INTO prm_ref_result ""(idLigne, idObjet, TBD, PAGE,OBJET,DAR_REF,PERD,RA_CODE,COLS_CODE,ROWS_CODE,SQL_CODE_SRC,SQL_CODE_FINAL,PERIMETRE,DATE_TRT,VALEUR,FORMULE,NIV)"" VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s , %s,%s)")
                        values = (idLigne, idObjet, TBD, PAGE,OBJET,DARREF,PERD, RA, COL, ROW, "", "", PERIMETRE, DATE_TRT,RESULTAT,FAMILLE,1)
                    

                        # Execute the query with the values
                        cur.execute(query,values)
                        # Commit the transaction
                        cnx.commit()

                except Exception as e :  
                    logging.error(f"Can not calculate value for line with id '{idLigne}'"+str(e))
                    continue

        except Exception as e :  
                    logging.error(f"Can not calculate value for line with id '{idLigne}'"+str(e))
                
            





remplir_cube_final_calcul(Parametres.dateref,Parametres.username,Parametres.password,Parametres.ip_address,Parametres.schema_name)
                    
                    
                    
                    
                    

                    

                    
                    

                    

                    
                    



                    









                

                


      


        
