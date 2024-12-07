kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

user = {
    "username": "spogoretskyi",
}

topics = {
    "topic_sensors": "building_sensors",
    "topic_temperature": "temperature_alerts",
    "topic_humidity": "humidity_alerts",
}


def generate_topics_config(
    topics, username, default_partitions=2, default_replication=1
):
    return {
        key: {
            "name": f"{value}_{username}",
            "num_partitions": default_partitions,
            "replication_factor": default_replication,
        }
        for key, value in topics.items()
    }


topics_configurations = generate_topics_config(topics, user["username"])
