from datetime import date
 
def  code_to_date(code,dateref) :
    dat = date.today()
    final_date = 0
    year_ref = int(dateref[0:4])
    month_ref =  int(dateref[4:])
    if len(code) == 6 : 
        if code[2]  == '-' :
            month_diff = month_ref - int(code[3])*10 - int(code[4])
            if month_diff < 0 :
                final_date = year_ref*100 -100 + month_diff +12
            else :
                final_date = year_ref*100 + month_diff
        else : 
            final_date = int(dateref) - int(code[4])*100
    elif len(code) == 5 :
        month_diff = month_ref - int(code[3])
        if month_diff < 0 :
            final_date = year_ref*100 - 100 + month_diff +12
        else :
            final_date = year_ref*100 + month_diff 

    elif len(code) == 7 :
        final_date = year_ref*100 + int(code[2])*10 + int(code[3])  - int(code[5])*100
    
    return str(final_date)
            


#print(code_to_date('[M-1]'))



