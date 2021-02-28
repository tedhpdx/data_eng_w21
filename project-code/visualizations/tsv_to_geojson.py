import csv, json
from geojson import Feature, FeatureCollection, Point
from sqlalchemy import create_engine

def convert(input):
    features = []
    with open(input , newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        data = csvfile.readlines()
        prev_line = []
        for line in data[1:]:
            line = line.replace('\r','')
            line = line.replace('\n','')
            row = line.split("\t")
            if row[0] == '':
                continue
            if prev_line == line:
                continue
            # Uncomment these lines
            lat = row[0]
            long = row[1]
            if row[2]:
                speed = float(row[2])
            else:
                speed = row[2]
            # skip the rows where speed is missing
            if speed is None or speed == "":
                continue

            try:
                latitude, longitude = map(float, (lat, long))
                features.append(
                    Feature(
                        geometry = Point((longitude,latitude)),
                        properties = {
                            'speed': (int(speed))
                        }
                    )
                )
            except ValueError:
                continue
            prev_line = line

    collection = FeatureCollection(features)
    with open("visualizations\\data.geojson", "w") as f:
        f.write('%s' % collection)

def tsv_to_geojson():
    username = 'herring'
    password = 'Entage1234'
    database = 'practice'
    engine = create_engine('postgresql://' + username + ':' + password + '@34.105.70.119:5432/' + database)
    query = "SELECT latitude, longitude, speed, tstamp FROM breadcrumb, trip WHERE tstamp::date = date '2020-09-26' and route_id = 65 and tstamp::time between '16:00' and '18:00' and trip.direction = 'Out'"
    results = engine.execute(query)
    with open("test1.tsv", 'w') as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerows(results)
    convert('test1.tsv')


tsv_to_geojson()
