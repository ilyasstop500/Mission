import csv 
import Parametres
import pandas
# Open file 
	

def modify_specific_lines(file_path, line_num1, new_value1):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Modify the specific lines
    lines[line_num1 - 1] = f"{new_value1}\n"

    with open(file_path, 'w') as file:
        file.writelines(lines)
		
