import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import json
import os
from datetime import datetime


# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'GOLQZTZY2L3FYG4X',
    'sasl.password': 'cfltzrcmjxXol9ayf6xpl1aFPEVfplW24y1TXWHcb9XbkbexgcoEgwn4pbTSRaNQ',
    'group.id': 'group31',
    'auto.offset.reset': 'latest'  # Start reading from the latest message if no offset is committed
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-1o8pr0d.us-east1.gcp.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('EALUUCGFDPS2A5EH', 'cfltWGWAhHB5ks2ixHuwfV+n+Igq0sRrbP9IhQwniFyKourt7+9jaNkrUeq5g1aQ')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_data'

schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset'],
    #'enable.auto.commit': True,
    #'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# To handle serialization of datetime objects,defining a custom encoder.
def datetime_encoder(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
# Path to the separate JSON file for each consumer
file_path = 'consumer1.json'  

# Python function to load append the json string data into json file.
def write_to_json_file(json_string, file_path):
    with open(file_path, 'a') as file:
        file.write(json_string + '\n')

# Subscribe to the 'retail_data' topic
consumer.subscribe(['product_updates'])

# Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        
        #Change the category column to lowercase,in source it's in uppercase.
        msg.value()['category'] = msg.value()['category'].lower() 
        
        # updating the price to half if product belongs to 'category a'
        if msg.value()['category'] == 'category a':
            
            msg.value()['price'] = msg.value()['price'] * 0.5
            msg.value()['price'] = round(msg.value()['price'],2)
            
        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
        json_string = json.dumps(msg.value(), default=datetime_encoder)

        def write_to_json_file(json_string, file_path):
            with open(file_path, 'a') as file:
                file.write(json_string + '\n')

        # Check if the file exists
        if not os.path.isfile(file_path):
            # Create the file and write the initial data
            with open(file_path, 'w') as file:
                file.write(json_string + '\n')
        else:
            # Append the data to the existing file
            write_to_json_file(json_string, file_path)
            print("json_string data is added to the JSON file.")
        
           
except KeyboardInterrupt:
    pass
finally:
    consumer.close()