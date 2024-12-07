from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, topics_configurations, user
from functions import create_topics, show_topics


admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

show_topics(admin_client, user)
