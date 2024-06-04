from sympy import var
from sympy import sympify
#from logs_refsql import log
#from logs_refsql import log
import math
import numpy as np

  

def calculate(formula, list_values):
    result = formula
    for item in list_values:
        result = result.replace(item[0], item[1])
    return eval(result)






    










    




#def removeDuplicates(lst):
#    # Convert the list of tuples to a dictionary using dict.fromkeys
#    # This automatically removes duplicates because dict keys must be unique
#    dict_without_duplicates = dict.fromkeys(lst)
#     
#    # Return the list of keys from the dictionary, which will be the original tuples
#    return list(dict_without_duplicates.keys())


#def get_cols_rows(tableau): 
#    list_cols_rows = list()
#    for elem in tableau : 
#        list_cols_rows.append((elem[0],elem[1]))
#    list_cols_rows = removeDuplicates(list_cols_rows)
#    return list_cols_rows











    









