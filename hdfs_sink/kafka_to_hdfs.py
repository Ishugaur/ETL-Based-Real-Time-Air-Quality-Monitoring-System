# kafka_to_hdfs.py
from kafka import KafkaConsumer
import json
import datetime
import os

HDFS_DIR = "hdfs://localhost:9000/user/sunbeam/air_quality"

consumer = KafkaConsumer(
    'air_quality',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    record = message.value
    filename = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".json"
    local_path = f"/tmp/{filename}"

    with open(local_path, "w") as f:
        json.dump(record, f)

    os.system(f"hdfs dfs -mkdir -p {HDFS_DIR}")
    os.system(f"hdfs dfs -put {local_path} {HDFS_DIR}")
    os.remove(local_path)
    print(f"Saved record to HDFS: {filename}")
