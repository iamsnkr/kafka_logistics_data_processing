import time
import traceback
import pandas as pd

from confluent_kafka import SerializingProducer  # To Serialize Data Class
from confluent_kafka.schema_registry import SchemaRegistryClient  # make a connection to Schema registry
from confluent_kafka.schema_registry.avro import AvroSerializer  # To serialize value data to avro
from confluent_kafka.serialization import StringSerializer  # To serialize key data
from configparser import ConfigParser
import configparser as cf
from datetime import datetime


def get_csv_data():
    # Read CSV file into DataFrame
    df = pd.read_csv("delivery_trip_truck_data.csv")
    # # Print information about the DataFrame
    # print('*******************************')
    # print(df.info())
    # print('*******************************')
    # Convert columns to appropriate data types and handle missing values
    df['BookingID_Date'] = pd.to_datetime(df['BookingID_Date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['actual_eta'] = pd.to_datetime(df['actual_eta'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['Curr_lat'] = df['Curr_lat'].astype(float)
    df['Curr_lon'] = df['Curr_lon'].astype(float)
    df['trip_start_date'] = pd.to_datetime(df['trip_start_date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['trip_end_date'] = pd.to_datetime(df['trip_end_date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['TRANSPORTATION_DISTANCE_IN_KM'] = df['TRANSPORTATION_DISTANCE_IN_KM'].astype(float)
    df['TRANSPORTATION_DISTANCE_IN_KM'] = df['TRANSPORTATION_DISTANCE_IN_KM'].fillna(0)
    # Handle NaN values and convert to integer
    df['Minimum_kms_to_be_covered_in_a_day'] = df['Minimum_kms_to_be_covered_in_a_day'].fillna(0)
    df['Minimum_kms_to_be_covered_in_a_day'] = df['Minimum_kms_to_be_covered_in_a_day'].astype(int)
    # Convert columns to string and integer types
    df['ontime'] = df['ontime'].astype(str)
    df['vehicleType'] = df['vehicleType'].astype(str)
    df['Driver_Name'] = df['Driver_Name'].astype(str)
    df['Driver_MobileNo'] = df['Driver_MobileNo'].fillna(0)
    df['Driver_MobileNo'] = df['Driver_MobileNo'].astype('int64')
    df = df.fillna('null')
    return df


def generate_dict_data(dict_data):
    for key in dict_data.keys():
        if dict_data[key] in ['nan', 0, 'null', 'NULL']:
            dict_data[key] = None
        elif key in ['BookingID_Date', 'actual_eta', 'trip_start_date', 'trip_end_date']:
            datetime_obj = datetime.strptime(dict_data[key], '%Y-%m-%d %H:%M:%S')
            dict_data[key] = datetime_obj.strftime("%d-%m-%Y, %H:%M:%S")
    print(dict_data)
    return dict_data


config: ConfigParser = cf.ConfigParser()
# Load the config properties
config.read('config.properties', 'utf-8')
print(config)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
    print("User record {} successfully produced to {} [{}] at offset {}".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


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
avro_serializer = AvroSerializer(schema_registry_client, value_schema)
key_serializer = StringSerializer('utf_8')

producer = SerializingProducer({
    # Common Info about cluster
    'bootstrap.servers': config['KAFKA_CONF']['bootstrap.servers'],
    'security.protocol': config['KAFKA_CONF']['security.protocol'],
    'sasl.mechanisms': config['KAFKA_CONF']['sasl.mechanisms'],
    'sasl.username': config['KAFKA_CONF']['sasl.username'],
    'sasl.password': config['KAFKA_CONF']['sasl.password'],
    # Serializer Info
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})


def produce_messages_to_kafka():
    df = get_csv_data()
    for index, row in df.iterrows():
        value = generate_dict_data(row.to_dict())
        producer.produce(topic=config['TOPIC_CONF']['topic.name'], key=str(index), value=value,
                         on_delivery=delivery_report)
        producer.flush()
        # time.sleep(1)
    print("Messages Produced Successfully")


def sample_data():
    data = {
        'date_strings': ['8/18/20', '08-12-2020', 'null', '01/Aug/2024', '08-11-2020  15:27:00', '8/21/20 16:14'],
        'numbers': ['01', '020', '5', '4', '080', '84']
    }
    df = pd.DataFrame(data)
    try:
        print(df.info())
        df['date_strings'] = pd.to_datetime(df['date_strings'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['numbers'] = df['numbers'].astype(int)
    except Exception as e:
        print(traceback.format_exc())
    print(df.info())
    for i, row in df.iterrows():
        print(type(row.to_dict()['numbers']), row.to_dict()['numbers'])


if __name__ == "__main__":
    produce_messages_to_kafka()
