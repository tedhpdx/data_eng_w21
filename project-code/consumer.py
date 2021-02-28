#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
from db_script import send_to_db
from stop_events_db_script import send_to_stop_events_db
import json
import ccloud_lib
import datetime

def process_breadcrumbs():
    # Process messages
    total_count = 0
    data = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                timestamp = str(datetime.datetime.now()+datetime.timedelta(hours=-7))
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                #send remaining messages to db
                if data:
                    send_to_db(data)
                    data = []
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                #validate, transform and add to database here

                data.append(json.loads(record_value))
                if len(data) == 1000:
                    send_to_db(data)
                    data = []
                total_count += 1
                #print("Consumed record with key {} and value {}, \
                #      and updated total count to {}"
                #      .format(record_key, record_value, total_count))

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


def process_stop_events():
    topic = 'StopEvents'
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                record_key = msg.key()
                record_value = msg.value()
                record_value = json.loads(record_value)
                send_to_stop_events_db(record_value)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()




if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    #config_file = '/home/herring/.confluent/librdkafka.config'
    config_file = 'C:\\Users\\Ted\\Desktop\\librdkafka.config'
    topic = 'breadcrumbs'
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    #process_breadcrumbs()
    process_stop_events()



