import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

OUTPUT_FILE = "/data/twitch.json"

def consumer_from_kafka(topic):
    open(OUTPUT_FILE, "w").close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='kafka:29092',
        auto_offset_reset="latest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        group_id="twitch-consumer-group"
    )

    consumer.subscribe([topic])

    print("Consumer connecté, écriture dans JSON")

    """Print
    for message in consumer:
        data = json.loads(message.value.decode())

        username = data.get("user")
        text = data.get("message")

        print(f"{username} : {text}")"""

    """Chat par minute
    """ 

    """count = 0
    start = time.time()

    for message in consumer:
        count += 1
        elapsed = time.time() - start

        if elapsed >= 60:
            print(f"{count} messages/minute")
            count = 0
            start = time.time()
"""
    
    # compteur temps réel
    count = 0
    start = time.time()

    for message in consumer:
        try:
            data = json.loads(message.value.decode())

            username = data.get("user")
            text = data.get("message")

            # enrichissement important pour spark
            record = {
                "user": username,
                "message": text,
                "timestamp": datetime.now().isoformat()
            }

            # ecriture dans le fichier JSON (1 ligne = 1 message)
            with open(OUTPUT_FILE, "a") as f:
                json.dump(record, f)
                f.write("\n")

            # affichage simple
            print(f"{username} : {text}")

            # stats temps réel (message/minute)
            count += 1
            elapsed = time.time() - start

            if elapsed >= 60:
                print(f"{count} messages/minute")
                count = 0
                start = time.time()
        except Exception as e:
            print("Erreur lors du traitement du message:", e)
        
    
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
    STREAMER = "Locklear" #"Kamet0"
    topic = "twitch-chat-" + STREAMER

    try:
        admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    except Exception:
        pass

    consumer_from_kafka(topic)


if __name__ == "__main__":
    main()