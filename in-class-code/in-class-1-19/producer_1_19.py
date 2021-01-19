import requests
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import datetime


def get_timestamp():
    timestamp = str(datetime.datetime.now()+datetime.timedelta(hours=-8))
    return timestamp


if __name__ == '__main__':

    #config_file = '/home/herring/.confluent/librdkafka.config'
    config_file = 'C:\\Users\\Ted\\Desktop\\librdkafka.config'

    topic = 'data_transport'
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
        r = requests.get('http://rbi.ddns.net/getBreadCrumbData')
    except:
        print("http://rbi.ddns.net/getBreadCrumbData not available")
        error_log = open("error_log.txt", "a")
        error_log.write("http://rbi.ddns.net/getBreadCrumbData unavailable at " + get_timestamp() +"\n")
        error_log.close()
        exit(-1)

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
            print (delivered_records)
            #print("Produced record to topic {} partition [{}] @ offset {}"
            #      .format(msg.topic(), msg.partition(), msg.offset()))

    #for breadcrumb in range(10):
    for breadcrumb in range(1000):
            record_key = "alice"
            record_value = json.dumps(rj[breadcrumb])
            producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls.
            producer.poll(0)

    producer.flush()




