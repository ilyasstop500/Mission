import csv
from ConDB import  con_to_db
import Parametres
import os
import string
import csv
from ConDB import  con_to_db
import Parametres
import logging


logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=r"Mission\02_Realisation\Code\Logs\CsvUpload.log"
)


# Definition of Csv_import a function that uploads the content of one csv file to the it's corresponding table that has the exact same name 

def Csv_import(filename, tablename, cnx, cursor):
    try:
        query1 = f"DELETE FROM {tablename};"
        query2 = f"""LOAD DATA  INFILE '{filename}' INTO TABLE {tablename}
            FIELDS TERMINATED BY ';' ENCLOSED BY ''
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES;"""
        cursor.execute(query1)
        cursor.execute(query2)
        cnx.commit()
        logging.debug(f"'{filename}' csv uploaded successfully")
    except Exception as e:
        logging.critical(f"Can not load CSV data from '{filename}':"+str(e) )




# Definition of Csv_import_all a function that uploads the contents of all the csvs in a directory to the corresponding tables 
def Import_All_Csv(directory):
    logging.debug(f"Initiating Csv upload from '{directory}'")
    cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
    cur = cnx.cursor()
    folder = os.path.basename(os.path.normpath(directory))
    query = "SHOW TABLES"
    cur.execute(query)
    list_of_tables = cur.fetchall()
    # Define the order in which to process tables
    table_order = [
        "prm_cols_calcul",
        "prm_sql_model",
        "vw_cube_dplt_icare_bpce_encr",
        "vw_cube_vbpce_apc_faits"
        

        # Add more tables in the desired order if needed
    ]
    
    for tablename in table_order:
        for table in list_of_tables:
            if table[0] == tablename:
                csvname = f"{tablename}.csv"
                Csv_import(f"{folder}/{csvname}", tablename, cnx, cur)
                break

    cur.close()
    cnx.close()






Import_All_Csv(Parametres.CSVs_directory)





         

    

    











class Col :

    def __init__(self,objet :'objet',idCol,Cols_Code,Nature,ordre,Formule=""):
        
        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()

        self.idObjet = objet.id
        self.idCol = idCol
        self.Cols_Code = Cols_Code
        self.Nature = Nature
        self.ordre = ordre
        self.formule = Formule
        self.filtres = list()
        self.composants = list()

        query = ("REPLACE INTO PRM_COLS VALUES (%s,%s,%s,%s,%s)")
        values = (idCol,Cols_Code,Nature,Formule,ordre)


        cur.execute(query,values)
        cnx.commit()
        cnx.close()

        objet.link_col(self)


    
    def add_filtre (self,filtre :'Col_filtre') : 

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()


        query = ("REPLACE INTO PRM_COLS_FILTRE VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        values = (self.Nature,"RA",self.idCol,self.Cols_Code,"vw_CUBE_DPLT_ICARE_BPCE_ENCR",filtre.domain,"",self.idObjet,"TAB",filtre.cha,filtre.val,filtre.sens)


        cur.execute(query,values)
        cnx.commit()
        cnx.close()

        self.filtres.append(filtre)


    def add_component(self,comp : 'Col_composant') :

        if self.Nature == "CALCUL" :

            cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
            cur = cnx.cursor()


            query = ("REPLACE INTO PRM_COLS_COMPOSANT VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
            values = (comp.idcomposant,self.idObjet,0,self.idCol,self.Cols_Code,comp.code,comp.col_src.idCol,comp.col_src.Cols_Code)
            
            cur.execute(query,values)
            cnx.commit()
            cnx.close()

            self.composants.append(comp)

        else : 

            print("impossible to add compo to a source column")


class Col_filtre : 

    def __init__(self,domain,cha,val,sens):

        self.domain = domain
        self.cha = cha
        self.val= val
        self.sens = sens


class Col_composant :

    def __init__(self,id_comp,code,col_src :'Col') :

        self.idcomposant = id_comp
        self.code = code
        self.col_src = col_src





        

# ------------------------------------------------------- R O W S ----------------------------------------------------------------------------------------------------------
class Row : 

    def __init__(self,objet : 'objet',Rows_Code,Rows_Niv,idRow) :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()

        self.idObjet = objet.id
        self.Row_Code = Rows_Code
        self.niv = Rows_Niv
        self.id = idRow
        self.filtres = list()


        query = ("REPLACE INTO PRM_ROWS VALUES (%s,%s,%s,%s)")
        values = (idRow,objet.id,Rows_Code,Rows_Niv)

        cur.execute(query,values)
        cnx.commit()
        cnx.close()

        objet.link_row(self)

    def add_filtre(self, filtre : 'Row_filtre') :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()


        query = ("REPLACE INTO PRM_ROWS_FILTRE VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
        values = (self.idObjet,self.id,self.Row_Code,"","",filtre.cha,filtre.val,filtre.sens)


        cur.execute(query,values)
        cnx.commit()
        cnx.close()

        self.filtres.append(filtre)




class Row_filtre : 

    def __init__(self,cha,val,sens):

        self.cha = cha
        self.val= val
        self.sens = sens  

    








# --------------------------------------------------------------- O B J E T ------------------------------------------------------------------------

class objet : 
    
    def __init__(self,id,tdb,page,nom) :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()

        self.id = id
        self.tdb = tdb
        self.page = page
        self.nom = nom 
        self.rows = list()
        self.cols = list()


        query = ("REPLACE INTO PRM_TDB_OBJETS VALUES (%s,%s,%s,%s,%s)")
        values = (id,tdb,page,nom,"")


        cur.execute(query,values)
        cnx.commit()
        cnx.close()


    def link_row(self,row) : 
        self.rows.append(row)
    def link_col(self,col) : 
        self.cols.append(col) 


    def link_rows_cols(self,id) :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()
        ID = id
        for row in self.rows :
            for col  in self.cols :
                query = ("REPLACE INTO PRM_RA_LIENS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                values = (ID,self.id,99,"RACODE",col.idCol,row.id,col.Cols_Code, row.Row_Code,"OUI\r")
                cur.execute(query,values)
                cnx.commit()
                ID = ID + 1 

        cnx.close()
            




        




# STEP 1 / CREATE OBJECT

Objet0 = objet(0,"Maroc",1,"Maroc")


# STEP 2 / CREATE COLUMNS

Col0 = Col(Objet0,0,"VERST","SOURCE",0)
Col1 = Col(Objet0,1,"REMB","SOURCE",0)
Col2 = Col(Objet0,2,"MAX","CALCUL",1,"MAX")
Col3 = Col(Objet0,3,"MIN","CALCUL",1,"MIN")
Col4 = Col(Objet0,4,"RANG","CALCUL",1,"RANG")
Col5 = Col(Objet0,5,"POIDS","CALCUL",1,"POIDS")

# STEP 3 / CREATE COLUMN FILTERS

filtrec1 = Col_filtre("TEMPS","PERD_ARRT_INFO","[M-11]","INCLURE")
filtrec2 = Col_filtre("MESURE","MT_VERST","","INCLURE")
filtrec3 = Col_filtre("MESURE","MT_REMB","","INCLURE")
filtrec4 = Col_filtre("DIMENSION","CODE_NIV3_ICARE","3_EPAR_SAL","INCLURE")
filtrec5 = Col_filtre("DIMENSION","CODE_NIV3_ICARE","3_EPAR_BIL","INCLURE")
filtrec6 = Col_filtre("DIMENSION","CODE_NIV3_ICARE","3_EQUIP","INCLURE")

# STEP 4 / CREATE COLUMN COMPONENTS

compo1 = Col_composant(0,"[A]",Col0)
compo2 = Col_composant(1,"[A]",Col0)
compo3 = Col_composant(2,"[A]",Col0)
compo4 = Col_composant(3,"[A]",Col0)

# STEP 5 / ADD FILTERS AND COMPONENTS

Col0.add_filtre(filtrec1)
Col0.add_filtre(filtrec2)
Col0.add_filtre(filtrec4)
Col0.add_filtre(filtrec5)
Col0.add_filtre(filtrec6)


Col1.add_filtre(filtrec1)
Col1.add_filtre(filtrec3)
Col1.add_filtre(filtrec4)
Col1.add_filtre(filtrec5)
Col1.add_filtre(filtrec6)

Col2.add_filtre(filtrec1)
Col2.add_component(compo1)

Col3.add_filtre(filtrec1)
Col3.add_component(compo2)

Col4.add_filtre(filtrec1)
Col4.add_component(compo3)

Col5.add_filtre(filtrec1)
Col5.add_component(compo4)

# STEP 6 / CREATE ROWS 

Row1 = Row(Objet0,"CASABLANCA",0,0) 
Row2 = Row(Objet0,"RABAT",0,1) 
Row3 = Row(Objet0,"TOTAL",1,2) 



# STEP 7 / CREATE ROWS FILTRE

filtrer1 = Row_filtre("CODE_ORGN_FINN","111","INCLURE")
filtrer2 = Row_filtre("CODE_ORGN_FINN","112","INCLURE")

# STEP 8 / ADD FILTERS TO ROWS

Row1.add_filtre(filtrer1)
Row2.add_filtre(filtrer2)
Row3.add_filtre(filtrer1)
Row3.add_filtre(filtrer2)

# STEP 9 / LINK ROWS AND COLS FOR EACH OBJECT

Objet0.link_rows_cols(0)


        




















































# STEP 1 / CREATE OBJECT

Objet1 = objet(1,"Algerie",1,"Algerie")


# STEP 2 / CREATE COLUMNS

Col6 = Col(Objet1,6,"VERST","SOURCE",0)
Col7 = Col(Objet1,7,"REMB","SOURCE",0)
Col8 = Col(Objet1,8,"MAX","CALCUL",1,"MAX")
Col9 = Col(Objet1,9,"MIN","CALCUL",1,"MIN")
Col10 = Col(Objet1,10,"RANG","CALCUL",1,"RANG")
Col11 = Col(Objet1,11,"POIDS","CALCUL",1,"POIDS")

# STEP 3 / CREATE COLUMN FILTERS

filtrec6 = Col_filtre("TEMPS","PERD_ARRT_INFO","[M-11]","INCLURE")
filtrec7 = Col_filtre("MESURE","MT_VERST","","INCLURE")
filtrec8 = Col_filtre("MESURE","MT_REMB","","INCLURE")
filtrec9 = Col_filtre("DIMENSION","CODE_NIV3_ICARE","3_EPAR_SAL","INCLURE")
filtrec10 = Col_filtre("DIMENSION","CODE_NIV3_ICARE","3_EPAR_BIL","INCLURE")
filtrec11 = Col_filtre("DIMENSION","CODE_NIV3_ICARE","3_EQUIP","INCLURE")

# STEP 4 / CREATE COLUMN COMPONENTS

compo4 = Col_composant(4,"[A]",Col6)
compo5 = Col_composant(5,"[A]",Col6)
compo6 = Col_composant(6,"[A]",Col6)
compo7 = Col_composant(7,"[A]",Col6)

# STEP 5 / ADD FILTERS AND COMPONENTS

Col6.add_filtre(filtrec6)
Col6.add_filtre(filtrec7)
Col6.add_filtre(filtrec9)
Col6.add_filtre(filtrec10)
Col6.add_filtre(filtrec11)


Col7.add_filtre(filtrec6)
Col7.add_filtre(filtrec8)
Col7.add_filtre(filtrec9)
Col7.add_filtre(filtrec10)
Col7.add_filtre(filtrec11)

Col8.add_filtre(filtrec6)
Col8.add_component(compo4)

Col9.add_filtre(filtrec6)
Col9.add_component(compo5)

Col10.add_filtre(filtrec6)
Col10.add_component(compo6)

Col11.add_filtre(filtrec6)
Col11.add_component(compo7)

# STEP 6 / CREATE ROWS 

Row1 = Row(Objet1,"ALGER",0,3) 
Row2 = Row(Objet1,"TIZI",0,4) 
Row3 = Row(Objet1,"TOTAL",1,5) 



# STEP 7 / CREATE ROWS FILTRE

filtrer3 = Row_filtre("CODE_ORGN_FINN","121","INCLURE")
filtrer4 = Row_filtre("CODE_ORGN_FINN","122","INCLURE")

# STEP 8 / ADD FILTERS TO ROWS

Row1.add_filtre(filtrer3)
Row2.add_filtre(filtrer4)
Row3.add_filtre(filtrer3)
Row3.add_filtre(filtrer4)

# STEP 9 / LINK ROWS AND COLS FOR EACH OBJECT

Objet1.link_rows_cols(18)




























    
        








    






    
import subprocess


subprocess.run(["python3", r"Mission\02_Realisation\Code/02-Remplir_Refsql.py"])
subprocess.run(["python3", r"Mission\02_Realisation\Code/03-Remplir_Result_Source.py"])
subprocess.run(["python3", r"Mission\02_Realisation\Code/04-Remplir_Result_Calcul.py"]) 