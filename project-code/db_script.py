import datetime
import json
import os
import random
import time
import pandas as pd
from sqlalchemy import create_engine


def get_date(df, i):
    time = int(df['ACT_TIME'][i])
    time = (str(datetime.timedelta(seconds=time)))
    date = df['OPD_DATE'][i]
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


def get_dataframe(json_package):
    temp_file_name = str(random.getrandbits(128))
    with open(temp_file_name, 'w') as outfile:
        json.dump(json_package, outfile)

    df = pd.read_json(temp_file_name)
    os.remove(temp_file_name)
    return df


def validate_data(df, i):
    trip_ID = df['EVENT_NO_TRIP'][i]
    try:
        date = get_date(df, i)
    except:
        date = None
    vehicle_id = df['VEHICLE_ID'][i]
    try:
        service_key = day_of_week(date)
    except:
        service_key = 'Weekday'

    if df['GPS_LATITUDE'][i]:
        latitude = df['GPS_LATITUDE'][i]
    else:
        latitude = None
    if df['GPS_LONGITUDE'][i]:
        longitude = df['GPS_LONGITUDE'][i]
    else:
        longitude = None
    if df['DIRECTION'][i]:
        direction = df['DIRECTION'][i]
    else:
        direction = None
    if df['VELOCITY'][i]:
        speed = df['VELOCITY'][i]
    else:
        speed = None

    # set these later
    route_id = None
    #direction = 'Out'

    validated_data = {
        'trip_id': trip_ID,
        'date': date,
        'vehicle_id': vehicle_id,
        'service_key': service_key,
        'route_id': route_id,
        'direction': direction,
        'latitude': latitude,
        'longitude': longitude,
        'speed': speed
    }
    return validated_data


def send_to_trip_db(engine, validated_data):
    query = "SELECT trip_id FROM trip WHERE trip_id = " + str(validated_data['trip_id']) + ";"
    results = engine.execute(query)
    if results.rowcount == 0:
        trip_df = pd.DataFrame(columns=['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'])
        new_row = {'trip_id': validated_data['trip_id'],
                   'route_id': validated_data['route_id'],
                   'vehicle_id': validated_data['vehicle_id'],
                   'service_key': validated_data['service_key'],
                   'direction': validated_data['direction']
                   }
        prev_trip_id = validated_data['trip_id']
        trip_df = trip_df.append(new_row, ignore_index=True)
        trip_df.to_sql('trip', engine, if_exists='append', index=False)
        return prev_trip_id


def append_breadcrumb_df(validated_data, breadcrumb_df):
    new_row = {'tstamp': validated_data['date'],
               'latitude': validated_data['latitude'],
               'longitude': validated_data['longitude'],
               'direction': validated_data['direction'],
               'speed': validated_data['speed'],
               'trip_id': validated_data['trip_id']
               }
    breadcrumb_df = breadcrumb_df.append(new_row, ignore_index=True)
    return breadcrumb_df


def send_to_db(json_package):
    username = ''
    password = ''
    database = 'practice'
    df = get_dataframe(json_package)
    breadcrumb_df = pd.DataFrame(columns=['tstamp', 'latitude', 'longitude', 'direction', 'speed', 'trip_id'])
    prev_trip_id = None
    engine = create_engine('postgresql://' + username + ':' + password + '@34.105.70.119:5432/' + database)
    for i in range(len(df)):
        validated_data = validate_data(df, i)
        if validated_data['trip_id'] != prev_trip_id:
            prev_trip_id = send_to_trip_db(engine, validated_data)
        breadcrumb_df = append_breadcrumb_df(validated_data, breadcrumb_df)
        if i == len(df) - 1:
            breadcrumb_df.to_sql('breadcrumb', engine, if_exists='append', index=False, method='multi', chunksize=10000)
