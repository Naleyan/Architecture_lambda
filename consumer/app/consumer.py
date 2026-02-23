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

    """Print
    for message in consumer:
        data = json.loads(message.value.decode())

        username = data.get("user")
        text = data.get("message")

        print(f"{username} : {text}")"""

    """Chat par minute
    """ 

    count = 0
    start = time.time()

    for message in consumer:
        count += 1
        elapsed = time.time() - start

        if elapsed >= 60:
            print(f"{count} messages/minute")
            count = 0
            start = time.time()

    """Top 10 user plus actifs
    from collections import Counter

    counter = Counter()

    for message in consumer:
        data = message.value
        counter[data["user"]] += 1

        top_users = counter.most_common(5)
        print("Top 5:", top_users)

    """


    consumer.close()


def main():
    print("consumer : ")
    STREAMER = "Kamet0"
    topic = "twitch-chat-" + STREAMER

    try:
        admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    except Exception:
        pass

    consumer_from_kafka(topic)


if __name__ == "__main__":
    main()