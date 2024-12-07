from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import window, avg, col, lit
from configs import kafka_config, topics_configurations, user
from functions import clean_checkpoint_directory, process_and_send_alerts
import pandas as pd
import json
import time
import os


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

checkpoint_directories = {"/tmp/checkpoints-2", "/tmp/checkpoints-3"}
clean_checkpoint_directory(checkpoint_directories)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

alerts_conditions = pd.read_csv("alerts_conditions.csv")

spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "building_sensors")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .load()
)

json_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
    ]
)

sensor_data = (
    df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn(
        "value_json",
        from_json(col("value_deserialized"), json_schema),
    )
    .withColumn(
        "timestamp",
        when(
            col("value_json.timestamp").isNotNull(),
            (col("value_json.timestamp").cast("double").cast("timestamp")),
        ).otherwise(None),
    )
    .withColumn("temperature", col("value_json.temperature"))
    .withColumn("humidity", col("value_json.humidity"))
    .withColumn("sensor_id", col("value_json.sensor_id"))
    .drop("value_json", "value_deserialized")
)

# Data aggregation
aggregated_stream = (
    sensor_data.withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_humidity"),
    )
)

try:
    query = process_and_send_alerts(producer, aggregated_stream, alerts_conditions)
    query.awaitTermination()
except Exception as e:
    print(f"Error: {e}")
