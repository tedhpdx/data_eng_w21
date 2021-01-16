import requests
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib

if __name__ == '__main__':

    #config_file = '/home/herring/.confluent/librdkafka.config'
    config_file = 'librdkafka.config'

    topic = 'breadcrumbs'
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
    r = requests.get('http://rbi.ddns.net/getBreadCrumbData')
    rj = r.json()

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
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    #for breadcrumb in range(10):
    for breadcrumb in range(len(rj)):
            record_key = "alice"
            record_value = json.dumps(rj[breadcrumb])
            producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls.
            producer.poll(0)

    producer.flush()
    '''
    second = first['EVENT_NO_TRIP']
    with open(json_file_path, 'w') as outfile:
            json.dump(rj, outfile)
    '''


'''
Notes:
This will be called every day at 4am
It will:
        pull the data
        make it json
        
        
'''


