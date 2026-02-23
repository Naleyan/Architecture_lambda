import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

def consumer_from_kafka(topic):

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='kafka:29092',
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    consumer.subscribe([topic])

    for message in consumer:
        data = json.loads(message.value.decode())

        username = data.get("user")
        text = data.get("message")

        print(f"{username} : {text}")

    consumer.close()


def main():
    print("consumer : ")

    topic = "twitch-chat"

    try:
        admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    except Exception:
        pass

    consumer_from_kafka(topic)


if __name__ == "__main__":
    main()