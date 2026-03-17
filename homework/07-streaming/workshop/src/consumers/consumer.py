from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="green-trips-consumer-3",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=5000
)

count = 0
for message in consumer:
    record = message.value
    if float(record.get("trip_distance", 0)) > 5.0:
        count += 1

consumer.close()
print(f"Number of trips with distance > 5 km: {count}")