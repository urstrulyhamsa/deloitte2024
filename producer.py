import pandas as pd
from kafka import KafkaProducer
import json

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Load stock data
data = pd.read_csv("C:\Users\Admin\Downloads\stock_data_new.csv")

# Send data to Kafka topic
for index, row in data.iterrows():
    producer.send('stock_topic', value=row.to_dict())
producer.flush()
