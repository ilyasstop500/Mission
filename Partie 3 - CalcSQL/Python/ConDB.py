import mysql.connector
from mysql.connector import errorcode


def con_to_db(username,pwd,hostip,dbname) :

  try:
    cnx = mysql.connector.connect(user=username, password=pwd,
                                   host=hostip,
                                   database=dbname)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)
  else:
    print("all good")
    return cnx 
    