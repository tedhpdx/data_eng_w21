import datetime
import json
import os
import random
import time
import pandas as pd
from sqlalchemy import create_engine


def get_dataframe(json_package):
    temp_file_name = str(random.getrandbits(128))
    with open(temp_file_name, 'w') as outfile:
        json.dump(json_package, outfile)

    df = pd.read_json(temp_file_name)
    os.remove(temp_file_name)
    return df


def validate_data(df, i):
    trip_ID = df['trip_id'][i]
    route_number = df['route_number'][i]

    if df['direction'][i] is 0:
        direction = 'Out'
    elif df['direction'][i] is 1:
        direction = 'Back'
    else:
        direction = 'Out'

    if df['service_key'][i] is 'S':
        service_key = 'Saturday'
    else:
        service_key = 'Weekday'

    validated_data = {
        'trip_id': trip_ID,
        'route_number': route_number,
        'direction': direction,
        'service_key': service_key
    }
    return validated_data


def send_to_trip_db(engine, validated_data):
    trip_id = str(validated_data['trip_id'])
    route_number = str(validated_data['route_number'])
    service_key = str(validated_data['service_key'])
    direction = str(validated_data['direction'])
    query = "UPDATE trip SET route_id = "+route_number+" WHERE trip_id = "+trip_id
    engine.execute(query)
    query = "UPDATE trip SET service_key = '%s', direction = '%s' WHERE trip_id = %s;" % (service_key, direction, trip_id)
    results = engine.execute(query)
    return validated_data['trip_id']



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


def send_to_stop_events_db(json_package):
    username = 'herring'
    password = 'Entage1234'
    database = 'practice'
    df = get_dataframe(json_package)
    prev_trip_id = None
    engine = create_engine('postgresql://' + username + ':' + password + '@34.105.70.119:5432/' + database)
    for i in range(len(df)):
        validated_data = validate_data(df, i)
        if validated_data['trip_id'] != prev_trip_id:
            prev_trip_id = send_to_trip_db(engine, validated_data)

