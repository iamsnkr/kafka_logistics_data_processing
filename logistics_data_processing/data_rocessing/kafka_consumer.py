import random

from confluent_kafka import DeserializingConsumer  # To Serialize Data Class
from confluent_kafka.schema_registry import SchemaRegistryClient  # make a connection to Schema registry
from confluent_kafka.schema_registry.avro import AvroDeserializer  # To serialize value data to avro
from confluent_kafka.serialization import StringDeserializer  # To serialize key data
from configparser import ConfigParser
import configparser as cf
from datetime import datetime
from pymongo import MongoClient
import json

# Load the config properties
config: ConfigParser = cf.ConfigParser()
config.read('config.properties', 'utf-8')

# Connecting to MongoClient
client = MongoClient(
    "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.10")
db = client.kafka_delivery_trip
# client.drop_database(db)


# Store data to MongoDB
def store_data_in_mongodb(db, dict_data):
    data = db.truck_data
    for key in dict_data.keys():
        if (key in ['BookingID_Date', 'actual_eta', 'trip_start_date', 'trip_end_date']) and (
                dict_data[key] is not None):
            # print(dict_data[key], type(dict_data[key]))
            datetime_obj = datetime.strptime(dict_data[key], "%d-%m-%Y, %H:%M:%S")
            dict_data[key] = datetime_obj
    print(json.dumps(dict_data, indent=4, default=str))

    if validate_data(dict_data):
        return data.insert_one(dict_data).inserted_id


def validate_data(data_dict):
    required_fields = [
        "GpsProvider",
        "BookingID",
        "Market_Regular",
        "BookingID_Date",
        "vehicle_no",
        "Origin_Location",
        "Destination_Location",
        "Org_lat_lon",
        "Des_lat_lon",
        "Data_Ping_time",
        "Planned_ETA",
        "Current_Location",
        "DestinationLocation",
        "actual_eta",
        "Curr_lat",
        "Curr_lon",
        "ontime",
        "delay",
        "OriginLocation_Code",
        "DestinationLocation_Code",
        "trip_start_date",
        "trip_end_date",
        "TRANSPORTATION_DISTANCE_IN_KM",
        "vehicleType",
        "Minimum_kms_to_be_covered_in_a_day",
        "Driver_Name",
        "Driver_MobileNo",
        "customerID",
        "customerNameCode",
        "supplierID",
        "supplierNameCode",
        "Material_Shipped"
    ]
    # Validate required fields
    for key in data_dict.keys():
        if isinstance(data_dict[key], dict):
            # it will validate recursively if for inner objects
            validate_data(data_dict)
        if key not in required_fields and data_dict[key] is None:
            raise ValueError(f"{key} is missing in the data or value of that key is {None}")

    # Validate data type of the fields
    # Validate datatime data type of the fields
    date_fields = ['BookingID_Date', 'actual_eta', 'trip_start_date', 'trip_end_date']
    for key in date_fields:
        if not isinstance(data_dict[key], datetime):
            raise ValueError(f"{key} is not a datatype of {datetime}")
    # Validate int and float data type of the fields
    int_float_fields = ['TRANSPORTATION_DISTANCE_IN_KM', 'Minimum_kms_to_be_covered_in_a_day', 'Curr_lat', 'Curr_lon',
                        'Driver_MobileNo']
    for key in int_float_fields:
        if not isinstance(data_dict[key], (int, float)):
            raise ValueError(f"{key} is not a datatype of {int, float}")
    return data_dict


def assign_callback(consumer, topic_partitions):
    print(f"topic_partitions : {topic_partitions}")
    for tp in topic_partitions:
        print(f"partition [{tp.partition}] from topic [{tp.topic}] at offset [{tp.offset}]")


schema_reg_conf = {
    # URL
    'url': config['SCHEMA_REG_CONF']['url'],
    # APU Scetect Key for the schema registry
    'basic.auth.user.info': config['SCHEMA_REG_CONF']['basic.auth.user.info']
}

# Create a schema registry client
schema_registry_client = SchemaRegistryClient(schema_reg_conf)
value_schema = schema_registry_client.get_latest_version(config['SCHEMA_REG_CONF']['schema.name']).schema.schema_str

# Create avro serializer for the value to convert to avro format
avro_deserializer = AvroDeserializer(schema_registry_client, value_schema)
key_deserializer = StringDeserializer('utf_8')

consumer = DeserializingConsumer({
    # Common Info about cluster
    'bootstrap.servers': config['KAFKA_CONF']['bootstrap.servers'],
    'security.protocol': config['KAFKA_CONF']['security.protocol'],
    'sasl.mechanisms': config['KAFKA_CONF']['sasl.mechanisms'],
    'sasl.username': config['KAFKA_CONF']['sasl.username'],
    'sasl.password': config['KAFKA_CONF']['sasl.password'],

    # De-Serializer Info
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,

    # Consumer Properties
    'group.id': config['CONSUMER_CONF']['group.id'],
    'auto.offset.reset': config['CONSUMER_CONF']['auto.offset.reset'],

    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})


def data_transformation(param):
    for key, value in param.items():
        print(key, type(value), value, sep="::")


def produce_messages_to_kafka():
    consumer.subscribe(topics=[config['TOPIC_CONF']['topic.name']], on_assign=assign_callback)
    consumer_id = random.randint(125058, 125589)
    try:
        while True:
            msg = consumer.poll(1.0)  # How many seconds to wait for message
            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue
            print(
                'Successfully consumed record with offset [{}] in partition [{}] from '
                'topic [{}]'.format(msg.offset(), msg.partition(), msg.topic()))
            print("consumer_id [{}] :: message: {}".format(consumer_id, msg.value()))
            try:
                _id = store_data_in_mongodb(db, msg.value())
                print(f"record inserted successfully into mongodb with id {_id}")
            except Exception as e:
                print(f"Exception occurred while inserting data {e}")
            consumer.commit(msg)

    except KeyboardInterrupt:
        pass
    finally:
        db.drop()
        consumer.close()
        client.close()
    print("All Messages consumed Successfully")


if __name__ == "__main__":
    produce_messages_to_kafka()
