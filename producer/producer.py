from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv('/data/movies_metadata.csv')

for _, row in df.iterrows():
    producer.send("movies", row.to_dict())
    time.sleep(0.01)  # имитация стрима

producer.flush()
print("Data sent to Kafka")