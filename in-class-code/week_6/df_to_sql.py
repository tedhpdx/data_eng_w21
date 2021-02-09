import pandas as pd
from sqlalchemy import create_engine


df = pd.read_csv('acs2015_census_tract_data.csv')
engine = create_engine('postgresql://:@34.105.70.119:5432/storact')
df.to_sql('censusdata_test', engine, if_exists='append', index=False, method='multi')
