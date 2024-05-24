import os
import string
import csv
from datetime import datetime
import json


def log_refsql(object,filepath,type) :
    json_object = json.dumps(object, indent=4)
    with open(filepath, type) as outfile:
        outfile.write(json_object)

    






