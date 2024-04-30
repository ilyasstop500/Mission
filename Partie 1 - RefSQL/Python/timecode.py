from datetime import date
 
def  code_to_date(code) :
    dat = date.today()
    final_date = 0
    date_ref = dat.year*100 + dat.month
    if len(code) == 6 : 
        if code[2]  == '-' :
            month_diff = dat.month - int(code[3])*10 - int(code[4])
            if month_diff < 0 :
                final_date = dat.year*100 -100 + month_diff +12
            else :
                final_date = dat.year*100 + month_diff
        else : 
            final_date = date_ref - int(code[4])*100
    elif len(code) == 5 :
        month_diff = dat.month - int(code[3])
        if month_diff < 0 :
            final_date = dat.year*100 - 100 + month_diff +12
        else :
            final_date = dat.year*100 + month_diff 

    elif len(code) == 7 :
        final_date = dat.year*100 + int(code[2])*10 + int(code[3])  - int(code[5])*100
    
    return str(final_date)
            


print(code_to_date('[M-1]'))



