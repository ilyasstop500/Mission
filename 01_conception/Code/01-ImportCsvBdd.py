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
    filename="CsvUpload.log"
)



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
        "prm_tdb_objets",
        "prm_cols_calcul",
        "prm_sql_model",
        "vw_cube_dplt_icare_bpce_encr",
        "vw_cube_vbpce_apc_faits",
        "prm_rows",
        "prm_cols",
        "prm_cols_filtre",
        "prm_ra_liens",
        "prm_rows_filtre",
        "prm_cols_composant"
        

        # Add more tables in the desired order if needed
    ]
    
    # Import prm_tdb_objets.csv first
    # Import other tables in specified order
    for tablename in table_order:
        for table in list_of_tables:
            if table[0] == tablename:
                csvname = f"{tablename}.csv"
                Csv_import(f"{folder}/{csvname}", tablename, cnx, cur)
                break

    cur.close()
    cnx.close()






Import_All_Csv(Parametres.CSVs_directory)





         

    

    














