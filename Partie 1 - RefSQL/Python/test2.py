import mysql.connector
from ConDB import con_to_db

# Connect to the database
cnx = con_to_db("root", "1234", "127.0.0.1", "test1")
cur = cnx.cursor()

# Define the values
idLigne = 1
idObjet = 123
TDB = 'TDB_Value'
PAGE = 'Page_Value'
OBJET = 'Object_Value'
TEMPS = 'jsp'  # Example datetime value
PERD = 10
RA = 'RA_Value'
COL = 5
ROW = 10
SQL_CODE_SRC = 'SQL_SRC'
SQL_CODE_FINAL = 'SQL_FINAL'
PERIMETRE = 'Perimet'
DATE_TRT = '2024-05-23'

# Define the query with placeholders


# Close the cursor and connection
cur.close()
cnx.close()
