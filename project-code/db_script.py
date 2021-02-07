import datetime
import json
import os

import pandas as pd
from sqlalchemy import create_engine


def get_date(df):
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


def send_to_db(json_package):
    data = []
    trip_df = None
    breadcrumb_df = None
    data.append(json.loads(json_package))
    with open('temp.json', 'w') as outfile:
        json.dump(data, outfile)
    df = pd.read_json('temp.json')
    os.remove('temp.json')

    trip_ID = df['EVENT_NO_TRIP'][0]
    date = get_date(df)
    vehicle_id = df['VEHICLE_ID'][0]
    service_key = day_of_week(date)

    #set these later
    route_id = 2
    direction = 'Out'


    engine = create_engine('postgresql://herring:Entage1234@34.105.70.119:5432/practice')
    query = "SELECT trip_id FROM trip WHERE trip_id = " + str(trip_ID) + ";"
    results = engine.execute(query)
    if results.rowcount == 0:
        trip_df = pd.DataFrame(columns=['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'])
        new_row = {'trip_id': trip_ID, 'route_id': route_id, 'vehicle_id': vehicle_id, 'service_key': service_key, 'direction': direction}
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
    new_row = {'tstamp': date, 'latitude': latitude, 'longitude': longitude, 'direction': direction, 'speed': speed, 'trip_id': trip_ID}
    breadcrumb_df = breadcrumb_df.append(new_row, ignore_index=True)
    breadcrumb_df.to_sql('breadcrumb', engine, if_exists='append', index=False)