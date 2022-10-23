from datetime import datetime

def join_2_strings(string1,string2):
    return f"{string1} {string2}"

def obt_gender(gen):
    if gen == 'M':
        return 'Masculino'
    elif gen == 'F':
        return 'Femenino'
    else:
        return 'NO DEFINIDO'

def obt_date(date_string):
    return datetime.strptime(date_string, "%d-%b-%y")

def obt_month_number(string_month):
    month = datetime.strptime(string_month, "%m")
    return month.strftime("%B")
