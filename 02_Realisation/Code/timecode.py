from datetime import date
 
def  code_to_date(code,dateref) :
    dat = date.today()
    final_date = 0
    year_ref = int(dateref[0:4])
    month_ref =  int(dateref[4:])

    if len(code) == 6 : 
        diff =  int (code[3:5])
        months_diff = diff % 12
        years_diff = diff // 12 
        if (month_ref-months_diff) >= 0 : 
            final_date = (year_ref - years_diff ) * 100 + (month_ref-months_diff)
        else : 
            final_date = (year_ref - years_diff-1 ) * 100 + (12 + month_ref- months_diff)
    

    elif len(code) == 7 :
       final_date = year_ref*100 + int(code[2])*10 + int(code[3])  - int(code[5])*100
    
    return str(final_date)

    



            

print(code_to_date('[M05N2]',"202405"))



