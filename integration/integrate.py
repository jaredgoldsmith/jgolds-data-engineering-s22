from re import A, I
import pandas
import csv
from datetime import datetime

def acs_data_transform():
    file = 'acs2017_census_tract_data.csv'

    df = pandas.read_csv(file)
    counties = df['County'].values.tolist()
    states = df['State'].values.tolist()

    rows = []
    for i in range(len(counties)):
        if counties[i] == 'Loudon County':
            rows.append(df.loc[[i]])

        
    county_info = df[['County', 'State', 'TotalPop', 'Poverty', 'IncomePerCap']]
    county_data = pandas.DataFrame(columns=['County', 'State', 'Population', 'Poverty', 'PerCapitaIncome', 'ID'])
    county_ids = set()
    for i in range(1000, 5000):
        county_ids.add(i)
        
    rows = []
    county_names = county_info['County'].values.tolist()
    i = 0
    while i < len(county_names):
        repeat = False
        row_info = county_info.loc[[i]]
        county_row = list()
        unique_county = row_info['County'].values[0]
        county_row.append(row_info['County'].values[0])
        county_row.append(row_info['State'].values[0])
        county_row.append(0)
        county_row.append(float(0.0))
        county_row.append(float(0.0))
        while i < len(county_names) and unique_county == county_info.loc[[i]]['County'].values[0]:
            row_data = county_info.loc[[i]]
            population = row_data['TotalPop'].values[0]
            pov = row_data['Poverty'].values[0]
            
            if str(row_data['Poverty'].values[0]) != 'nan':
                totalPoverty = float(population * row_data['Poverty'].values[0] / 100)
            else:
                totalPoverty = 0.0 
            if str(row_data['IncomePerCap'].values[0]) != 'nan':
                totalIncome = float(population * row_data['IncomePerCap'].values[0])
            else:
                totalIncome = 0.0
            county_row[2] += population
            county_row[3] += totalPoverty
            county_row[4] += totalIncome
            i += 1
            repeat = True
        if repeat:
            id = county_ids.pop()
            poverty_rate = county_row[3] / county_row[2] * 100
            poverty_rate = f'{poverty_rate:.2f}'
            income_capita = county_row[4] / county_row[2]
            income_capita = f'{income_capita:.2f}'
            df2 = pandas.DataFrame({'County' : county_row[0], 'State' : county_row[1], 'Population' : county_row[2], 'Poverty' : poverty_rate, 'PerCapitaIncome' : income_capita, 'ID' : [id]})
            county_data = pandas.concat([county_data,df2], ignore_index=True)
            i -= 1
            county_row.clear()
        i += 1
        if i == len(county_names) -1:
            break
        

    #county_data.to_pickle('county_data.pkl')
    return county_data


def get_previous_key(key):
    date = key.split(' ')[-1]
    date = int(date)
    if date == 1:
        prev_date = str(12)
    else:
        prev_date = str(date - 1)
    date = str(date)
    return key.replace(date,prev_date)


def covid_monthly_transform(df2):
    filename = 'COVID_county_data.csv'
    df = pandas.read_csv(filename)
    covid_month = pandas.DataFrame(columns=['ID', 'Month', 'Cases', 'Deaths'])
    row = df.loc[[0]]
    date = str(row['county'])
    dates = df['date'].values.tolist()
    counties = df['county'].values.tolist()
    states = df['state'].values.tolist()
    cases = df['cases'].values.tolist()
    deaths = df['deaths'].values.tolist()
    
   
    results = {}
    general = {}
    
    
    dif_county_name = 0
    for i in range(len(dates)):
        year, month, day = dates[i].split('-')
        year = int(year)
        month = int(month)
        day = int(day)
        key = set()
        string1 = f'{counties[i]} {states[i]} {month}'
        key2 = f'{counties[i]} {states[i]}'
        county = f'{counties[i]} County'
        row = df2.loc[(df2['County'] == county) & (df2['State'] == states[i])]
        try:
            id = row['ID'].values[0]
        except:
            dif_county_name += 1
            continue
        if string1 in results:
            results[string1][3] = cases[i]
            results[string1][5] = deaths[i]
            if general[key2][3] > 1:
                prev_key = get_previous_key(string1)
                prev_total = results[prev_key][3]
                current_total = results[string1][3]
                prev_deaths = results[prev_key][5]
                current_deaths = results[string1][5]
                results[string1][2] = current_total - prev_total
                results[string1][4] = current_deaths - prev_deaths
            else:
                results[string1][2] = cases[i]
                results[string1][4] = deaths[i]
            general[key2][1] = cases[i]
            general[key2][2] = deaths[i]
        else:
            if key2 in general:
                num_months = general[key2][3]
                num_months = int(num_months)
                if num_months == 1:
                    results[string1] = [id, month, cases[i], cases[i], deaths[i], deaths[i]]
                else:
                    prev_key = get_previous_key(string1)
                    two_back_key = get_previous_key(prev_key)
                    total_cases = results[prev_key][3]
                    previous_total = results[two_back_key][3]
                    results[prev_key][2] = total_cases - previous_total
                        
                    results[string1] = [id, month, cases[i], cases[i], deaths[i], deaths[i]]
                general[key2][3] = num_months + 1
            else:
                results[string1] = [id, month, cases[i], cases[i], deaths[i], deaths[i]]
                general[key2] = [id, cases[i], deaths[i], 1]
                
            
    for key in results:
        id, month, month_cases, total_cases, month_deaths, total_deaths = results[key]
        filler = pandas.DataFrame({'ID' : id,'Month': month, 'Cases': month_cases, 'Deaths' : month_deaths }, index=[0])  
        covid_month = pandas.concat([covid_month,filler], ignore_index=True)
    
        
    #covid_month.to_pickle('covid_data3.pkl')
    return covid_month
    
    
def parse_covid_frame(df, df2):
    county = 'Malheur County'
    state = 'Oregon'
    month = 2
    key = f'{county} {state} {month}'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    row2 = df2.loc[(df2['ID'] == id) & (df2['Month'] == month)]
    #df3 = df2.groupby('ID', as_index=False)[['Cases']].sum()
    #df4 = df2.groupby('ID', as_index=False)[['Deaths']].sum()
    df3 = df2.groupby('ID', as_index=False).agg(
        {'Cases': sum}
    )
    df4 = df2.groupby('ID', as_index=False).agg(
        {'Deaths': sum}
    )
    
    row3 = df3.loc[(df3['ID'] == id)]
    ids = df3['ID'].values.tolist()
    cases = df3['Cases'].values.tolist()
    deaths = df4['Deaths'].values.tolist()
    covid_summary = pandas.DataFrame(columns=['ID', 'Population', 'Poverty', 'PerCapitaIncome', 'TotalCases', 'TotalDeaths', \
        'TotalCasesPer100k', 'TotalDeathsPer100k'])
    
    for i in range(len(cases)):
        row = df.loc[(df['ID'] == ids[i])]
        population = row['Population'].values[0]
        poverty = row['Poverty'].values[0]
        percap = row['PerCapitaIncome'].values[0]
        totalCasesPer100k = cases[i] * 100000/ population
        totalCasesPer100k = "{:.2f}".format(totalCasesPer100k)
        totalDeathsPer100k = deaths[i] * 100000 / population
        totalDeathsPer100k = "{:.2f}".format(totalDeathsPer100k)
        filler = pandas.DataFrame({'ID' : ids[i],'Population': population, 'Poverty': poverty, 'PerCapitaIncome' : percap, 'TotalCases' : \
            cases[i], 'TotalDeaths' : deaths[i], 'TotalCasesPer100k' : totalCasesPer100k, 'TotalDeathsPer100k' : totalDeathsPer100k}, index=[0])  
        covid_summary = pandas.concat([covid_summary,filler], ignore_index=True)
    return covid_summary

 
def covid_summary_query(df, covid_summary): 
    county = 'Washington County'
    state = 'Oregon'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    print(f'\n\nCovid summary data for {county} {state}')
    row2 = covid_summary.loc[(covid_summary['ID'] == id)]
    print(row2)
    county = 'Malheur County'
    state = 'Oregon'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    row2 = covid_summary.loc[(covid_summary['ID'] == id)]
    print(f'\n\nCovid summary data for {county} {state}')
    print(row2)
    county = 'Loudoun County'
    state = 'Virginia'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    row2 = covid_summary.loc[(covid_summary['ID'] == id)]
    print(f'\n\nCovid summary data for {county} {state}')
    print(row2)
    county = 'Harlan County'
    state = 'Kentucky'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    row2 = covid_summary.loc[(covid_summary['ID'] == id)]
    print(f'\n\nCovid summary data for {county} {state}')
    print(row2)
    print('\n\n')
    return covid_summary
    
      
def county_query(df):
    county = 'Loudoun County'
    state = 'Virginia'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    print(f'\nCounty data for {county} {state}:')
    print(row)
    county = 'Washington County'
    state = 'Oregon'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    print(f'\nCounty data for {county} {state}:')
    print(row)
    county = 'Harlan County'
    state = 'Kentucky'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    print(f'\nCounty data for {county} {state}:')
    print(row)
    county = 'Malheur County'
    state = 'Oregon'
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    print(f'\nCounty data for {county} {state}:')
    print(row)
    df2 = df['Population'].max()
    df3 = df[df.Population == df.Population.max()]
    df4 = df[df.Population == df.Population.min()]
    print(df2)
    print(df3)
    print(df4)
    
def covid_query(df,df2):
    county = 'Malheur County'
    state = 'Oregon'
    month = 2
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    row2 = df2.loc[(df2['Month'] == month) & (df2['ID'] == id)]
    print(f'\n\nCovid data for {county} {state}:')
    print(row2)
    county = 'Malheur County'
    state = 'Oregon'
    month = 8
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    row2 = df2.loc[(df2['Month'] == month) & (df2['ID'] == id)]
    print(row2)
    county = 'Malheur County'
    state = 'Oregon'
    month = 1
    row = df.loc[(df['County'] == county) & (df['State'] == state)]
    id = row['ID'].values[0]
    row2 = df2.loc[(df2['Month'] == month) & (df2['ID'] == id)]
    print(row2)
    

if __name__ == '__main__':
    county_info = pandas.read_pickle('county_data.pkl')
    #county_info = acs_data_transform()
    
    covid_monthly = pandas.read_pickle('covid_data3.pkl')
    #covid_monthly = covid_monthly_transform(county_info)
    
    covid_summary = parse_covid_frame(county_info, covid_monthly) 
    
    county_query(county_info)
    covid_query(county_info, covid_monthly)
    covid_summary_query(county_info, covid_summary)
    
