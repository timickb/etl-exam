import logging, json, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

logging.basicConfig(level=logging.DEBUG)

producer = KafkaProducer(
    bootstrap_servers=["rc1b-i01ku530sut4843b.mdb.yandexcloud.net:9091"],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="smart_home_events",
    sasl_plain_password="smart_home_events",
    ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
)

producer.bootstrap_connected()