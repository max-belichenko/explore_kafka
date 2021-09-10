#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
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

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

import pickle
import sys

from confluent_kafka import Producer

from schemas import cmdb


def generate_cmdb_test_data():
    NUMBER_OF_IT_SERVICES = 10

    it_services = [
        cmdb.CMDBItServiceScheme(
            id=i,
            name=f'IT Service #{i}',
            code='CODE',
            is_active=True,
        )
        for i in range(NUMBER_OF_IT_SERVICES)
    ]

    products = [
        cmdb.CMDBProductScheme(
            id=i,
            name=f'Product #{i}',
            code='CODE',
            is_active=True,
            owner_ad_uid='',
            owner_email='',
            it_services=[it_services[i], ]
        )
        for i in range(NUMBER_OF_IT_SERVICES)
    ]

    return products


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    producer = Producer(**conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    # products = generate_cmdb_test_data()
    # for product in products:
    for line in sys.stdin:
        # Pickle product object
        product_pickle = pickle.dumps(line)

        try:
            producer.produce(topic, product_pickle, callback=delivery_callback)
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        # producer.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()
