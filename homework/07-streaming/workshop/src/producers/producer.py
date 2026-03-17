import pandas as pd
import json
from kafka import KafkaProducer
from time import time

df = pd.read_parquet("green_tripdata_2025-10.parquet")

cols = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount"
]
df = df[cols]

df["passenger_count"] = df["passenger_count"].fillna(0).astype(int)
df["trip_distance"] = df["trip_distance"].fillna(0.0).astype(float)
df["tip_amount"] = df["tip_amount"].fillna(0.0).astype(float)
df["total_amount"] = df["total_amount"].fillna(0.0).astype(float)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

t0 = time()

for record in df.to_dict(orient="records"):
    record["lpep_pickup_datetime"] = str(record["lpep_pickup_datetime"])
    record["lpep_dropoff_datetime"] = str(record["lpep_dropoff_datetime"])

    producer.send("green-trips", record)

producer.flush()
t1 = time()
print(f"✅ Produced {len(df)} records to Kafka in {(t1 - t0):.2f} seconds")