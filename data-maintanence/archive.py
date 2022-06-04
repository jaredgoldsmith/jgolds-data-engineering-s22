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
import json
import ccloud_lib
import csv
from google.cloud import storage
import zlib


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_6'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                data = json.dumps(data).encode('utf-8')
                compressed_data = zlib.compress(data, 2)
                print(f'Consumed record with key {record_key} and value {data}')
                with open('data.json','a') as f:
                    writer = csv.writer(f)
                    #writer.writerow(f'Consumed record with key {record_key} and value {data}')
                    writer.writerow(compressed_data)
                count += 1
                #Try with specific key
                bucket_name = "archive05252022"
                #create_bucket_class_location(bucket_name)
                sample = 'sample.txt'
                desty = 'some_sample'
               
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        bucket_name = "archive05252022"
        #create_bucket_class_location(bucket_name)
        sample = 'data.json'
        #original_data = open('data.json', 'rb').read()
        #compressed_data = zlib.compress(original_data, zlib.Z_BEST_COMPRESSION)
        desty = 'some_sample3'
        upload_blob(bucket_name, sample, desty)
        print(f'Total number of messages consumer is {count}')
        consumer.close()
~                                                                                                                                                                                                                                                                                                                                                                                            
~                                                                                                                                                                                                                                                                                                                                                                                            
~                                                                                                                                                                                                                                                                                                                                                                                            
~                                                                                                                                                                                                                                                                                                                                                                                            
~                                                                                                                                                                                                                                                                                                                                                                                            
~                                                                                                                                                                                                                                                                                                                                                                                            
~                                                                                                                                                                                                                                                                                                                                                                                            
~                                                                                                                                                                                                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                                                                                                           42,5          All
