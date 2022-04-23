import csv
from re import I
from numpy import number
import pandas as pd

in_csv = 'crash_data_2019.csv'
filename = 'crash_data_2019.csv'

def validate_record_type(crashdata):
    #crashdata = pd.read_csv(filename)
    record_type = crashdata['Record Type'].values.tolist()
    print(type(record_type))
    print(type(record_type[0]))
    ones, twos, threes, other = 0,0,0,0
    for val in record_type:
        if val == 1:
            ones += 1
        elif val == 2:
            twos += 1
        elif val == 3:
            threes += 1
        else:
            other += 1
        if other > 0:
            print(f'There are {other} other record types labeled other than 1, 2, or 3. ')
    print(f'Record Types:\nOne: {ones}\nTwo: {twos}\nThree: {threes}')

def validate_dates(crashdata):
    months = crashdata['Crash Month'].values.tolist()
    days = crashdata['Crash Day'].values.tolist()
    for i in range(len(months)):
        if months[i] == '':
            continue
        else:
            if months[i] == 1 or months[i] == 3 or months[i] == 5 or months[i] == 7 or months[i] == 8 or months[i] == 10:
                if days[i] < 1 or days[i] > 31:
                    return False
            elif months[i] == 2:
                if days[i] < 1 or days[i] > 28:
                    return False
            else:
                if days[i] < 1 or days[i] > 30:
                    return False
    return True

def validate_month_counts(crashdata):
    months = crashdata['Crash Month'].values.tolist()
    jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec = 0,0,0,0,0,0,0,0,0,0,0,0
    total = 0
    for month in months:
        if str(month) == '' or str(month) == 'nan':
            continue
        else:
            total += 1
            if month == 1:
                jan += 1
            elif month == 2:
                feb += 1
            elif month == 3:
                mar += 1
            elif month == 4:
                apr += 1
            elif month == 5:
                may += 1
            elif month == 6:
                jun += 1
            elif month == 7:
                jul += 1
            elif month == 8:
                aug += 1
            elif month == 9:
                sep += 1
            elif month == 10:
                oct += 1
            elif month == 11:
                nov += 1
            elif month == 12:
                dec += 1
            else:
                print(f'Something must be wrong {month}')
    average = total / 12
    print(f'Total is: {total}')
    print(f'Average over all months is: {average}')
    smax = average * 1.5
    if jan > smax or feb > smax or mar > smax or apr > smax or may > smax or jun > smax or aug > smax or sep > smax or oct > smax or nov > smax or dec > smax:
        print("One of these months is statistically too much and likely error exists")
    print(f'Jan: {jan}\nFeb: {feb}\nMar: {mar}\nApr: {apr}\nMay: {may}\nJune: {jun}\nJul: {jul}\nAug: {aug}\nSep: {sep}\nOct: {oct}\nNov: {nov}\nDec: {dec}')
    
def validate_alcohol(crashdata):
    alcohol = crashdata['Alcohol-Involved Flag'].values.tolist()
    total = 0
    ones = 0
    for val in alcohol:
        if val == 0.0 or val == 1.0:
            total += 1
            if val == 1.0:
                ones += 1
    print(f'Total number of alcohol related entries: {total}\nNumber flagged: {ones} for an average of {ones/total*100:.2f}% \n')
    
def validate_days_of_week(crashdata):
    day_of_week = crashdata['Week Day Code'].values.tolist()
    sunday, monday, tuesday, wednesday, thursday, friday, saturday = 0,0,0,0,0,0,0
    total = 0
    for val in day_of_week:
        if str(val) == '' or str(val) == 'nan':
            continue
        else:
            if val == 1:
                sunday += 1
            elif val == 2:
                monday += 1
            elif val == 3:
                tuesday += 1
            elif val == 4:
                wednesday += 1
            elif val == 5:
                thursday += 1
            elif val == 6:
                friday += 1
            elif val == 7:
                saturday += 1
            else:
                print("Something incorrect")
            total += 1
    print(f'Number of Crashes by Day of Week:')
    print(f'Sunday: {sunday}\nMonday: {monday}\nTuesday: {tuesday}\nWednesday: {wednesday}\nThursday: {thursday}\nFriday: {friday}\nSaturday: {saturday}\n')

def validate_urban_code(crashdata):
    urban_codes = crashdata['Urban Area Code'].values.tolist()
    code_totals = {}
    for code in urban_codes:
        if code == 0 or str(code) == 'nan':
            continue
        else:
            if code in code_totals:
                code_totals[code] += 1
            else:
                code_totals[code] = 1
    print(code_totals)
    

if __name__ == "__main__":
    filename = 'crash_data_2019.csv'
    crashdata = pd.read_csv(filename)
    
    validate_record_type(crashdata)
    if validate_dates(crashdata):
        print("All of the dates reported do exist")
    else:
        print("There are dates reported that do not exist")
        
    validate_month_counts(crashdata)
    validate_alcohol(crashdata)
    validate_days_of_week(crashdata)
    validate_urban_code(crashdata)
    
        
    
