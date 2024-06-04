############################################################################################################
# Module P1 : Calcul SQL
# Auteur : Ilyass
# date : Mai 2025 
############################################################################################################


from ConDB import con_to_db
from Calcs_copy import calculate

def remplir_cube_final_calcul(user,pwd,ip,schema):
    
    cnx = con_to_db(user,pwd,ip,schema) #con to db 

    cur = cnx.cursor()
    query = ("SHOW TABLES")
    cur.execute(query)
    for elem in cur :
        print (elem)

    query = ("SELECT DISTINCT r.idRACible,r.idObjet,r.idRA,r.RA_Code,r.idColsCib,r.idRowsCib,r.COLS_CODE,r.ROWS_CODE,r.LIEN_VALIDE,c.COLS_NATURE FROM PRM_RA_LIENS r JOIN PRM_COLS c ON r.idColsCib = c.idCol WHERE r.LIEN_VALIDE ='OUI\r' AND c.COLS_NATURE = 'CALCUL'" )
    cur.execute(query)
    list_of_links = [] # list_of_links contain the list of (idObjet,idRA,RA_CODE,idColsCib,idRowsCib,COLS_CODE,ROW_CODE,LIEN_VALIDE)
    for elem in cur : 
        list_of_links.append(elem)
        print("link : " , elem)
    
    for link in list_of_links : #
        COLCIB = link[4]
        ROWCIB = link[5]
        ROWCODE = link[7]

        query =(f"SELECT COLS_FORMULE FROM PRM_COLS WHERE idCol = '{COLCIB}' ")
        cur.execute(query)
        
            
        COLS_FORMULE= cur.fetchone()[0].strip()

        query =(f"SELECT  CALC_FORMULE,CALC_FAMILLE,CALC_TYPE FROM PRM_COLS_CALCUL WHERE CALC_CODE = '{COLS_FORMULE}'")
        cur.execute(query)
        placeholder = [] # list_of_links contain the list of (idObjet,idRA,RA_CODE,idColsCib,idRowsCib,COLS_CODE,ROW_CODE,LIEN_VALIDE)
        for elem in cur : 
            placeholder.append(elem)
        print(" CALC FORMULE ? CALC FAMILLE ? CALC TYPE ?      "  ,placeholder)
        FORMULE = placeholder[0][0]
        FAMILLE = placeholder[0][1]
        TYPE = placeholder[0][2].strip()

        if TYPE == "COLONNE"  : 
            list_composants = list()
            query =(f"SELECT  idObjet,CODE_COMPOSANT,COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
            cur.execute(query)
            list_composants = cur.fetchall()
            #print(list_composants)
            #print('next')
            placeholder = list()
            for composant in list_composants : 
                OBJET = composant[0].strip()
                CODE_COMPOSANT = composant[1].strip()
                COLCODE= composant[2].strip()
                placeholder.append([OBJET,CODE_COMPOSANT,COLCODE,ROWCODE])
            
            list_valeurs = list()
            for elem in placeholder : 
                query =(f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{elem[0]}' AND  COLS_CODE = '{elem[2]}' AND ROWS_CODE = '{elem[3]}'")
                cur.execute(query)
                value = cur.fetchone()[0]
                list_valeurs.append([elem[1],value])
            
            
            #print('FORMULE :    ' , FORMULE)
            #print('RESULTAT ', calculate(FORMULE,list_valeurs))

        elif TYPE == "LIGNE" and FAMILLE == "MAX"  :
            query =(f"SELECT  idObjet,CODE_COMPOSANT,COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
            cur.execute(query)
            composant = cur.fetchone()
            query = (f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND  COLS_CODE = '{composant[2].strip()}' AND NIV = 0")
            cur.execute(query)
            list_valeurs  = cur.fetchall()
            print(list_valeurs)
            print(max(list_valeurs))

        elif TYPE == "LIGNE" and FAMILLE == "MIN"  :
            query =(f"SELECT  idObjet,CODE_COMPOSANT,COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
            cur.execute(query)
            composant = cur.fetchone()
            query = (f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND  COLS_CODE = '{composant[2].strip()}' AND NIV = 0 ")
            cur.execute(query)
            list_valeurs  = cur.fetchall()
            print(list_valeurs)
            print(min(list_valeurs))
        
        elif TYPE == "LIGNE" and FAMILLE == "RANG"  :
            print("heyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
            query =(f"SELECT  idObjet,CODE_COMPOSANT,COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
            cur.execute(query)
            composant = cur.fetchone()
            query = (f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND  COLS_CODE = '{composant[2].strip()}'")
            cur.execute(query)
            list_valeurs  = cur.fetchall()
            print(list_valeurs)
            print(min(list_valeurs))


        elif TYPE == "LIGNE" and FAMILLE == "POIDS"  :

            query =(f"SELECT  NIV FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND  COLS_CODE = '{composant[2].strip()}' AND ROWS_CODE = '{ROWCODE}' ")
            cur.execute(query)
            test = cur.fetchone()[0]
            print('test' , test)
            if int(test) == 1 :
                poids = 1
                break

            query =(f"SELECT  idObjet,CODE_COMPOSANT,COLS_CODE_SRC FROM PRM_COLS_COMPOSANT WHERE idColsCib = '{COLCIB}'")
            cur.execute(query)
            composant = cur.fetchone()
            query = (f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND  COLS_CODE = '{composant[2].strip()}' AND ROWS_CODE = '{ROWCODE}' AND NIV = 0 ")
            cur.execute(query)
            numerateur  =cur.fetchone()[0]
            print("niv 0 " , numerateur)
            query = (f"SELECT  VALEUR FROM PRM_REF_RESULT WHERE idObjet = '{composant[0]}' AND  COLS_CODE = '{composant[2].strip()}' AND NIV = 1 ")
            cur.execute(query)
            denumerateur =cur.fetchone()[0]
            print("niv 1 " , denumerateur)
            poids = float(numerateur)/float(denumerateur)
            print("le poids est " ,poids)
            
            
            
            
            
            

            

            
            

            

            
            



            









        

        


      


        
