import json

import requests
import ccloud_lib
from confluent_kafka import Producer, KafkaError
from bs4 import BeautifulSoup
import pandas as pd
import re

#r = requests.get('http://rbi.ddns.net/getStopEvents')
#soup = BeautifulSoup(r.text, 'html.parser')
'''
h1 = soup.find_all('h1')
print(h1[0].text)
h3s = soup.find_all('h3')
trip_ids = []
for heading in h3s:
    temp_heading = (re.findall(r'\d+', heading.text))
    for h in temp_heading:
        trip_ids.append(int(h))
#print(trip_ids)
'''
#tables = soup.find_all('table')
#for table in tables:
#    temp = pd.read_html(str(table))
#    print (temp)


if __name__ == '__main__':

    #config_file = '/home/herring/.confluent/librdkafka.config'
    config_file = 'C:\\Users\\Ted\\Desktop\\librdkafka.config'

    topic = 'StopEvents'
    conf = ccloud_lib.read_ccloud_config(config_file)


    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'queue.buffering.max.messages': 1000000,
    })
    try:
        r = requests.get('http://rbi.ddns.net/getStopEvents')
    except:
        print("getStopEvents not available")
        error_log = open("error_log.txt", "a")
        error_log.write("stop unavailable" + "\n")
        error_log.close()
        exit(-1)

    soup = BeautifulSoup(r.text, 'html.parser')

    delivered_records = 0
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            #print("Produced record to topic {} partition [{}] @ offset {}"
            #      .format(msg.topic(), msg.partition(), msg.offset()))

    h3s = soup.find_all('h3')
    trip_ids = []
    for heading in h3s:
        temp_heading = (re.findall(r'\d+', heading.text))
        for h in temp_heading:
            trip_ids.append(int(h))
    tables = soup.find_all('table')
    values = []
    #when table changes we need to change trip id

    trip_id_number = 0
    for table in tables:
        trip_id = trip_ids[trip_id_number]
        keys = [th.get_text(strip=True)for th in table.find_all('th')]
        keys.insert(0, 'trip_id')
        values = [td.get_text(strip=True) for td in table.find_all('td')]
        new_table = []
        for i in range(0, len(values), 24):
            values.insert(i, trip_id)
            new_table.append(values[i:i+24])
        df = pd.DataFrame(new_table, columns=keys)
        df = df.drop(df.columns[[1,2,3,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]], axis=1)
        record_key = "bob"
        record_value = df.to_json()
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        trip_id_number += 1
    producer.poll(0)


    producer.flush()