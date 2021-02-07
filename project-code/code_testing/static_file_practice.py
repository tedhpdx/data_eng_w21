import datetime
import pandas as pd
from sqlalchemy import create_engine

#figure 'breadcrumb_single_data' == json.loads(record_value)
df = pd.read_json('breadcrumb_single_data.json')

def get_date():
    time = int(df['ACT_TIME'][0])
    time = (str(datetime.timedelta(seconds=time)))
    date = df['OPD_DATE'][0]
    date += ' ' + time
    date = datetime.datetime.strptime(date, '%d-%b-%y %H:%M:%S')
    return date

def day_of_week(date):
    day_of_week = date.weekday()
    if day_of_week <= 4:
        return 'Weekday'
    if day_of_week == 5:
        return 'Saturday'
    if day_of_week == 6:
        return 'Sunday'

#primary and foreign key
trip_id = df['EVENT_NO_TRIP'][0]
date = get_date()

route_id = 2
vehicle_id = df['VEHICLE_ID'][0]
service_key = day_of_week(date)
direction = 'Out'


engine = create_engine('postgresql://herring:Entage1234@34.105.70.119:5432/practice')
results = engine.execute("SELECT trip_id FROM trip WHERE trip_id=trip_id;")
if not results.returns_rows:
    trip_df = pd.DataFrame(columns=['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'])
    new_row = {'trip_id': trip_id, 'route_id': route_id, 'vehicle_id': vehicle_id, 'service_key': service_key, 'direction': direction}
    trip_df = trip_df.append(new_row, ignore_index=True)
    trip_df.to_sql('trip', engine, if_exists='append', index=False)

if df['GPS_LATITUDE'][0]:
    latitude = df['GPS_LATITUDE'][0]
else:
    latitude = None
if df['GPS_LONGITUDE'][0]:
    longitude = df['GPS_LONGITUDE'][0]
else:
    longitude = None
if df['DIRECTION'][0]:
    direction = df['DIRECTION'][0]
else:
    direction = None
if df['VELOCITY'][0]:
    speed = df['VELOCITY'][0]
else:
    speed = None

breadcrumb_df = pd.DataFrame(columns=['tstamp', 'latitude', 'longitude', 'direction', 'speed', 'trip_id'])
new_row = {'tstamp': date, 'latitude': latitude, 'longitude': longitude, 'direction': direction, 'speed': speed, 'trip_id': trip_id}
breadcrumb_df = breadcrumb_df.append(new_row, ignore_index=True)
breadcrumb_df.to_sql('breadcrumb', engine, if_exists='append', index=False)