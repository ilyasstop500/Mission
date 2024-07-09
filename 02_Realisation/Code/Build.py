import csv
from ConDB import  con_to_db
import Parametres


class Col :

    def __init__(self,idObjet,idCol,Cols_Code,Nature,ordre,Formule=""):
        
        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()

        self.idObjet = idObjet
        self.idCol = idCol
        self.Cols_Code = Cols_Code
        self.Nature = Nature
        self.ordre = ordre
        self.formule = Formule

        query = ("REPLACE INTO PRM_COLS VALUES (%s,%s,%s,%s,%s)")
        values = (idCol,Cols_Code,Nature,Formule,ordre)

        cur.execute(query,values)
        cnx.commit()
        cnx.close()

    def add_filter(self,domain,cha,val,sens) :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()


        query = ("REPLACE INTO PRM_COLS_FILTRE VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        values = (self.Nature,"RA",self.idCol,self.Cols_Code,"CUBE",domain,"",self.idObjet,"TAB",cha,val,sens)


        cur.execute(query,values)
        cnx.commit()
        cnx.close()

    def add_component(self,id_comp,code,col_src :'Col') :

        if self.Nature == "CALCUL" :

            cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
            cur = cnx.cursor()


            query = ("REPLACE INTO PRM_COLS_COMPOSANT VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
            values = (id_comp,self.idObjet,0,self.idCol,self.Cols_Code,code,col_src.idCol,col_src.Cols_Code)
            
            cur.execute(query,values)
            cnx.commit()
            cnx.close()

        else : 

            print("impossible to add compo to a source column")

        

Col_1 = Col(0,200,"TRACER","SOURCE",55)
Col_2 = Col(0,201,"GENJI","CALCUL",55,"BLADE")

Col_1.add_filter("JJK","AURA","RIZZ","INCLURE")
Col_1.add_component(9000,"[A]",27)

Col_2.add_filter("JJK","AURA","RIZZ","INCLURE")
Col_2.add_component(9000,"[A]",Col_1)




class Row : 

    def __init__(self,idObjet,Rows_Code,Rows_Niv,idRow) :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()

        self.idObjet = idObjet
        self.Row_Code = Rows_Code
        self.niv = Rows_Niv
        self.id = idRow


        query = ("REPLACE INTO PRM_ROWS VALUES (%s,%s,%s,%s)")
        values = (idRow,idObjet,Rows_Code,Rows_Niv)

        cur.execute(query,values)
        cnx.commit()
        cnx.close()


    def add_filter(self,cha,val,sens) :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()


        query = ("REPLACE INTO PRM_ROWS_FILTRE VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
        values = (self.idObjet,self.id,self.Row_Code,"","",cha,val,sens)


        cur.execute(query,values)
        cnx.commit()
        cnx.close()


Row1 = Row(0,"REE",0,898989)
Row2 = Row(0,"BE",1,1444646)


Row1.add_filter("code","sbe3","INCLURE")
Row1.add_filter("sba7","hakalooz","INCLURE")

Row2.add_filter("fffffff","fff","EXCLURE")



class objet : 
    
    def __init__(self,id,tdb,page,nom) :

        cnx = con_to_db(Parametres.username, Parametres.password, Parametres.ip_address, Parametres.schema_name)
        cur = cnx.cursor()

        self.id = id
        self.tdb = tdb
        self.page = page
        self.nom = nom 


        query = ("REPLACE INTO PRM_TDB_OBJETS VALUES (%s,%s,%s,%s,%s)")
        values = (id,tdb,page,nom,"")


        cur.execute(query,values)
        cnx.commit()
        cnx.close()



objet(77,"JUPITER",3,"TESST")




        






    
        








    






    
