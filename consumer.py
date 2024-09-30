import mysql.connector
from kafka import KafkaConsumer
import json

# Connect to MySQL
db = mysql.connector.connect(user='root', password='Ssmb@152002', host='localhost', database='finCorp')
cursor = db.cursor()

# Initialize the Kafka consumer
consumer = KafkaConsumer('stock_topic',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    stock_data = message.value
    # Insert into MySQL
    cursor.execute("INSERT INTO stock_data (index_name, timestamp, stock_price, trading_volume) VALUES (%s, %s, %s, %s)",
                   (stock_data['index_name'], stock_data['timestamp'], stock_data['stock_price'], stock_data['trading_volume']))
    db.commit()
