import datetime
import pandas as pd

#figure 'breadcrumb_single_data' == json.loads(record_value)
df = pd.read_json('breadcrumb_single_data.json')

#data validations

if df['VELOCITY'][0] is '':
    df['VELOCITY'] = None
print (df['VELOCITY'])

time = int(df['ACT_TIME'][0])
print (str(datetime.timedelta(seconds=time)))

#finish validations

#send to db
