import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["rc1a-edrhu2fdhseflv91.mdb.yandexcloud.net:9091"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="smart_home_events",
    sasl_plain_password="smart_home_events",
    ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
)

topic = "smart_home_events"

try:
    while True:
        payload = {
            "event_time": datetime.now(timezone.utc).isoformat(),
            "device_id": f"dev-{random.randint(0, 4)}",
            "metric": random.choice(["temp", "humidity"]),
            "value": round(random.random() * 100, 2)
        }
        producer.send(topic, key=payload["device_id"], value=payload)
        producer.flush()
        print("sent:", payload)
        time.sleep(0.5)
except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    producer.close()