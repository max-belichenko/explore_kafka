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
# Example high-level Kafka 0.9 balanced Consumer
#
import pickle
import sys
import getopt
import json
import logging
from contextlib import closing

from confluent_kafka import Consumer, KafkaException
from pprint import pformat


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    options = '''
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
'''
    sys.stderr.write(options)
    sys.exit(1)


if __name__ == '__main__':
    optlist, argv = getopt.getopt(sys.argv[1:], 'T:')
    if len(argv) < 3:
        print_usage_and_exit(sys.argv[0])

    broker = argv[0]
    group = argv[1]
    topics = argv[2:]

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker,
            'group.id': group,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}


    # Check to see if -T option exists
    for opt in optlist:
        if opt[0] != '-T':
            continue
        try:
            intval = int(opt[1])
        except ValueError:
            sys.stderr.write("Invalid option value for -T: %s\n" % opt[1])
            sys.exit(1)

        if intval <= 0:
            sys.stderr.write("-T option value needs to be larger than zero: %s\n" % opt[1])
            sys.exit(1)

        conf['stats_cb'] = stats_cb
        conf['statistics.interval.ms'] = int(opt[1])

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # # Create Consumer instance
    # # Hint: try debug='fetch' to generate some log messages
    # consumer = Consumer(conf, logger=logger)
    #
    # def print_assignment(consumer, partitions):
    #     print('Assignment:', partitions)
    #
    # # Subscribe to topics
    # consumer.subscribe(topics, on_assign=print_assignment)
    #
    # # Read messages from Kafka, print to stdout
    # try:
    #     while True:
    #         msg = consumer.poll(timeout=1.0)
    #         if msg is None:
    #             continue
    #         if msg.error():
    #             raise KafkaException(msg.error())
    #
    #         # Proper message
    #         # sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
    #         #                  (msg.topic(), msg.partition(), msg.offset(),
    #         #                   str(msg.key())))
    #         # print(msg.value())
    #         data = pickle.loads(msg.value())
    #         print(f'{data=}')
    #
    # except KeyboardInterrupt:
    #     sys.stderr.write('%% Aborted by user\n')
    #
    # finally:
    #     # Close down consumer to commit final offsets.
    #     consumer.close()


    def set_offset_to_last_unread_message(consumer, partitions):
        logger.debug('Subscription assigned')
        logger.debug(f'{type(partitions)} {partitions=}')

        partitions_committed = consumer.committed(partitions)
        logger.debug(f'{partitions_committed=}')

        for i in range(len(partitions)):
            committed_offset = partitions_committed[i].offset
            logger.debug(f'{committed_offset=}')
            first_offset_index, end_offset_index = consumer.get_watermark_offsets(partitions[i])
            logger.debug(f'{first_offset_index=} {end_offset_index=}')
            last_offset = end_offset_index - 1
            logger.debug(f'{last_offset=}')
            if committed_offset < last_offset:
                logger.debug(f'Set offset to the last message')
                partitions[i].offset = last_offset

        consumer.assign(partitions)
        logger.debug('Partitions reassigned')

    with closing(Consumer(conf, logger=logger)) as consumer:
        # ?????????????????????? ???? ??????????
        consumer.subscribe(topics, on_assign=set_offset_to_last_unread_message)

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                # Proper message
                # sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                #                  (msg.topic(), msg.partition(), msg.offset(),
                #                   str(msg.key())))
                # print(msg.value())
                data = pickle.loads(msg.value())
                print(f'{msg.headers()=}')
                print(f'{data=}')

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')



"""
var consumer = new ConsumerBuilder<Null, string>(config)
                .SetPartitionsAssignedHandler((c, tps) =>
                {
                    var partitionOffsets = c.Committed(tps, TimeSpan.FromSeconds(10));
                    var watermarkOffsets = tps.Select(tp => c.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)));
                    var offsets = watermarkOffsets.Zip(partitionOffsets, (watermarkOffset, topicPartitionOffset) =>
                    {
                        if (topicPartitionOffset.Offset.IsSpecial || watermarkOffset.High.IsSpecial)
                        {
                            return topicPartitionOffset.Offset;
                        }

                        var lastTopicOffset = watermarkOffset.High - 1;
                        var lastCommittedOffset = topicPartitionOffset.Offset - 1;

                        if (lastTopicOffset == 0)
                        {
                            return new Offset(0);
                        }
                        if (lastCommittedOffset == lastTopicOffset)
                        {
                            return new Offset(lastCommittedOffset);
                        }
                        return new Offset(lastCommittedOffset + 1);
                    });

                    return tps.Zip(offsets, (partition, offset) => new TopicPartitionOffset(partition, offset));
                })
                .Build()
"""