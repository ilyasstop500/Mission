import json

def Log_refsql (object,filepath,mode) :
    json_object = json.dumps(object, indent = 4)
    with open(filepath,mode) as outfile : 
        outfile.write(json_object)


def Log_Import_Csv () :

    return 0 

def Log_Creation_Bdd () : 

    return 0 


def Log_Result() : 

    return 0 


 