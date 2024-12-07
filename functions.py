from kafka.admin import KafkaAdminClient, NewTopic
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
import time
import random
import json
import os
import shutil


def show_topics(admin_client, user):
    try:
        while True:
            print("my_topics")
            print("all_topics")
            print("close (exit)")

            user_input = input("Enter your choice: ").strip().lower()

            if user_input == "my_topics":
                print(
                    [
                        print(topic)
                        for topic in admin_client.list_topics()
                        if user["username"] in topic
                    ]
                )
                continue

            if user_input == "all_topics":
                print("Existing topics:\n", admin_client.list_topics())
                continue

            if user_input == "close" or user_input == "exit":
                print("Closing connection and exiting.")
                admin_client.close()
                break

            else:
                print("Invalid choice. Please try again.")
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        admin_client.close()


def produce_alerts(producer, sensor_id, topic_name):
    print(f"Starting sensor simulation with ID: {sensor_id}")
    print(f"Sending data to topic: {topic_name}")

    try:
        while True:
            data = {
                "sensor_id": sensor_id,
                "timestamp": time.time(),
                "temperature": random.uniform(25, 45),
                "humidity": random.uniform(15, 85),
            }

            try:
                producer.send(
                    topic_name,
                    key=sensor_id,
                    value=data,
                )
                producer.flush()
                print(f"Sent: {data}")
            except Exception as e:
                print(f"Error sending message: {e}")

            time.sleep(20)
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()
        print("Producer closed.")


def clean_checkpoint_directory(checkpoint_dirs):
    for checkpoint_dir in checkpoint_dirs:
        if os.path.exists(checkpoint_dir):
            print(f"Cleaning checkpoint directory: {checkpoint_dir}")
            shutil.rmtree(checkpoint_dir)
        else:
            print(f"Checkpoint directory does not exist: {checkpoint_dir}")


def process_and_send_alerts(producer, aggregated_stream, conditions):
    def process_batch(batch_df, batch_id):
        condition_list = conditions.to_dict(orient="records")

        batch_df = batch_df.select(
            "avg_temperature", "avg_humidity", "window_start", "window_end"
        )

        for row in batch_df.collect():
            t_avg = row["avg_temperature"]
            h_avg = row["avg_humidity"]
            window_start = row["window_start"]
            window_end = row["window_end"]

            if isinstance(window_start, datetime):
                window_start = window_start.isoformat()
            if isinstance(window_end, datetime):
                window_end = window_end.isoformat()

            for condition in condition_list:
                humidity_min = condition["humidity_min"]
                humidity_max = condition["humidity_max"]
                temperature_min = condition["temperature_min"]
                temperature_max = condition["temperature_max"]
                code = condition["code"]
                message = condition["message"]

                is_humidity_alert = (humidity_min != -999 and h_avg < humidity_min) or (
                    humidity_max != -999 and h_avg > humidity_max
                )

                is_temperature_alert = (
                    temperature_min != -999 and t_avg < temperature_min
                ) or (temperature_max != -999 and t_avg > temperature_max)

                if is_humidity_alert or is_temperature_alert:
                    alert_message = {
                        "window": {"start": window_start, "end": window_end},
                        "t_avg": t_avg,
                        "h_avg": h_avg,
                        "code": code,
                        "message": message,
                        "timestamp": window_end,
                    }

                    print("ALERT GENERATED:", json.dumps(alert_message, indent=2))

                    producer.send("alerts_output_topic", value=alert_message)

    query = (
        aggregated_stream.writeStream.foreachBatch(process_batch)
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", "/tmp/checkpoints-3")
        .start()
    )

    return query
