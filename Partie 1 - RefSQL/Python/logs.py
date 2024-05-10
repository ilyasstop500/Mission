import os
import string
import csv
from datetime import datetime


def log(text,filepath) :
    print(text)
    with open(filepath, 'a',newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=';',quotechar='|', quoting=csv.QUOTE_MINIMAL)     
        spamwriter.writerow([datetime.now(),text]) 



log('test',r"C:\Users\ILYASS\Desktop\logs.csv") 


