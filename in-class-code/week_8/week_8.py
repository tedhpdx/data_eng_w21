import pandas as pd


#covid_df = pd.read_csv('COVID_county_data.csv')
acs_df = pd.read_csv('acs2017_census_tract_data.csv')

acs_df = acs_df.drop(acs_df.columns[[0,4,5,6,7,8,9,10,11,12,13,14,16,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36]], axis=1)
acs_df = acs_df.groupby('County').agg({'State':'first', 'TotalPop':sum, 'IncomePerCap':'mean', 'Poverty':'mean'})


state_list = ['Virginia', 'Oregon', 'Kentucky', 'Oregon']
county_list = ['Loudon County', 'Washington County', 'Harlan County', 'Malheur County']

print(acs_df)


'''
for i in county_list:
    x = acs_df.loc[i]
    if x['State'] == 'Virginia':
        print(x)
'''


