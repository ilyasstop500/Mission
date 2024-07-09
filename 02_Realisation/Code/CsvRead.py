import csv 
import Parametres
import pandas
# Open file 
with open(r'C:\Users\ILYASS\Desktop\PRM_REF_SQL.csv') as file_obj: 
    df = pandas.read_csv(file_obj)
    print(df.to_string())
	


		
			
            
			
				


			

















def modify_specific_lines(file_path, line_num1, new_value1, line_num2, new_value2):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Modify the specific lines
    lines[line_num1 - 1] = f"{new_value1}\n"
    lines[line_num2 - 1] = f"{new_value2}\n"

    with open(file_path, 'w') as file:
        file.writelines(lines)
		
modify_specific_lines(r'C:\Users\ILYASS\Desktop\Stage\Mission\02_Realisation\Code\Parametres.py', 8 , 'next_col_id = 600 ', 9, 'next_row_id = 200')
