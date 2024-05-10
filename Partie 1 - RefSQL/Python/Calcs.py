from sympy import var
from sympy import sympify
from logs import log





# this functions reads the formula and extracts all the variable names in play 
def get_variables(func) :

    list_var = list()

    for i in range (len(func)) :
        if func[i].isalpha() :
             # we check every char of the string to see if its a digit or an alphabet if it's neither it could be an operator
             list_var.append(func[i])

    list_var = list(set(list_var)) # remove duplicates 
    return list_var

# this function returns the index of a value in the the tabval
def get_index(table,var):

    for i in range (len(table)) :
        if table[i][2] == var: 
            return (i)
        
    return(-1)

    

def calculate(func,tabval,col,row):

    placeholder = func 
    list_var = get_variables(func)  # get all variables in play

    for i in range (len(list_var)) : # iterate over all variable 
        if get_index (tabval,list_var[i]) == -1 : # check if the varibale has values in the tabval
            log("the col is " + col + " the row is " + row + " and the function is  : " + func  +" but cant find value for variable : " + list_var[i] +" there also might be more missing values ",r"C:\Users\ILYASS\Desktop\LOGS\logs_calcul.csv") # if the variable is not found an error message is shown to the user and a saved in the log 
            return ("m","error cant find variable")   # we also return  an error message to add in the csv afterwards
        replacement = tabval[get_index(tabval,list_var[i])][3]
        placeholder=placeholder.replace(list_var[i],replacement) #replace the varibale with its numerical value in the string 

    log('the col is ' + col + ' the row is ' + row + ' and the functions is : ' + func + ' and the result is  : ' + str((eval(placeholder))), r"C:\Users\ILYASS\Desktop\LOGS\logs_calcul.csv") # in the end we use the eval func to calcute the result of the final string 
    return ("v",eval(placeholder))
    


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

def get_tableval(col,row,tabval) :
    result = list()
    for i in range (len(tabval)):
        if (tabval[i][0] == col and tabval[i][1]==row):
            result.append(tabval[i])
    return(result)



list_of_functions = [('C01','R01','a+b'),('C01','R02','a+b'),('C02','R01','a-b+c/3'),('C02','R02','a-b+c/3'),('C03','R01','((x+y)*2)/(z+1)')]

tabval2 = [['C01','R01','a','3'],
          ['C01','R01','b','5'],
          ['C01','R02','a','1'],
          ['C01','R02','x','3'],
          ['C02','R01','a','7'],
          ['C02','R01','b','7'],
          ['C02','R01','c','3'],
          ['C02','R02','a','7'],
          ['C02','R02','b','7'],
          ['C02','R02','c','7'],
          ['C03','R01','x','7'],
          ['C03','R01','y','7'],
          ['C03','R01','z','7']]



def calculate_all(list_of_functions,tabval):
    for i in range (len(list_of_functions)) : 
        col =list_of_functions[i][0] 
        row = list_of_functions[i][1]
        func = list_of_functions[i][2]
        tabval_colrow = get_tableval(col,row,tabval)
        calculate(func,tabval_colrow,col,row)
        

calculate_all (list_of_functions,tabval2)











    









