import datetime
import pandas as pd
from sqlalchemy import create_engine

#figure 'breadcrumb_single_data' == json.loads(record_value)
df = pd.read_json('breadcrumb_single_data.json')

#data validations

if df['VELOCITY'][0] is '':
    df['VELOCITY'] = None
print (df['VELOCITY'])

time = int(df['ACT_TIME'][0])
print (str(datetime.timedelta(seconds=time)))

#finish validations

engine = create_engine('postgresql://@34.105.70.119:5432/practice')
df.to_sql('another_test_trip', engine, if_exists='append', index=False)
#send to db
