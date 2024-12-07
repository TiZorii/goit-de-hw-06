from kafka import KafkaProducer
from configs import kafka_config, topics_configurations
from functions import produce_alerts
import json
import uuid


topic_name = topics_configurations["topic_sensors"]["name"]
sensor_id = str(uuid.uuid4())
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

produce_alerts(producer, sensor_id, topic_name)
