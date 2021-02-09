import datetime
import json
import os
import random
import time
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
        return  'Sunday'




def send_to_db(json_package):

    #data = []
    trip_df = None
    breadcrumb_df = None
    #data.append(json.loads(json_package))

    temp_file_name = str(random.getrandbits(128))
    with open(temp_file_name, 'w') as outfile:
        json.dump(json_package, outfile)

    df = pd.read_json(temp_file_name)
    os.remove(temp_file_name)

    breadcrumb_df = pd.DataFrame(columns=['tstamp', 'latitude', 'longitude', 'direction', 'speed', 'trip_id'])
    prev_trip_id = None
    engine = create_engine('postgresql://herring:Entage1234@34.105.70.119:5432/practice')
    for i in range(len(df)):
        print(i)
        trip_ID = df['EVENT_NO_TRIP'][i]
        try:
            date = get_date(df)
        except:
            date = None
        vehicle_id = df['VEHICLE_ID'][i]
        '''
        the service key may need to accept values of None if the date field is wrong
        '''
        try:
            service_key = day_of_week(date)
        except:
            service_key = 'Weekday'

        #set these later
        route_id = 2
        direction = 'Out'



        if trip_ID != prev_trip_id:
            query = "SELECT trip_id FROM trip WHERE trip_id = " + str(trip_ID) + ";"
            results = engine.execute(query)
            if results.rowcount == 0:
                trip_df = pd.DataFrame(columns=['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'])
                new_row = {'trip_id': trip_ID, 'route_id': route_id, 'vehicle_id': vehicle_id, 'service_key': service_key, 'direction': direction}
                prev_trip_id = trip_ID
                trip_df = trip_df.append(new_row, ignore_index=True)
                trip_df.to_sql('trip', engine, if_exists='append', index=False)

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


        new_row = {'tstamp': date, 'latitude': latitude, 'longitude': longitude, 'direction': direction, 'speed': speed, 'trip_id': trip_ID}
        breadcrumb_df = breadcrumb_df.append(new_row, ignore_index=True)
        if i == len(df) - 1:
            breadcrumb_df.to_sql('breadcrumb', engine, if_exists='append', index=False, method='multi', chunksize=10000)

