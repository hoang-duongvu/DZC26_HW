import pandas as pd
import json
from kafka import KafkaProducer
from time import time


server = "localhost:9092"
topic_name = "green-trips"

columns = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
]

# Create the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

df = pd.read_csv('green_tripdata_2019-10.csv')

df = df[columns]

records = df.to_dict(orient="records")

for message in records:
    producer.send(topic_name, value=message)

producer.flush()