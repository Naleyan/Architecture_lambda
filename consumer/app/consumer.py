import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

OUTPUT_FILE = "/data/twitch.json"

def consumer_from_kafka(topic):
    open(OUTPUT_FILE, "w").close()

    print(f"Connexion du Consumer au topic {topic}...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='kafka:29092',
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="twitch-consumer-group4",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 8, 1)
    )

    print("Consumer connecté, en attente de messages...")

    count = 0
    start = time.time()

   
    while True:
        # On interroge Kafka toutes les 2 secondes 
        messages_par_partition = consumer.poll(timeout_ms=2000)
        
        partitions = consumer.assignment()
        if not partitions:
            print("En attente d'assignation de la partition par Kafka")
            continue
            
        if not messages_par_partition:
            print(f"Assigné à {partitions}, mais pas de messages")
            continue

        # Traitement des messages reçus
        for topic_partition, messages in messages_par_partition.items():
            for message in messages:
                try:
                    data = message.value
                    username = data.get("user")
                    text = data.get("message")

                    record = {
                        "user": username,
                        "message": text,
                        "timestamp": datetime.now().isoformat()
                    }

                    with open(OUTPUT_FILE, "a") as f:
                        json.dump(record, f)
                        f.write("\n")

                    print(f"[{message.offset}] {username} : {text}")

                    # Stats temps réel
                    count += 1
                    elapsed = time.time() - start

                    if elapsed >= 60:
                        print(f"--- {count} messages/minute ---")
                        count = 0
                        start = time.time()

                except Exception as e:
                    print("Erreur lors du traitement du message:", e)

def main():
    print("Démarrage du consumer...")
    STREAMER = "jynxzi"
    topic = "twitch-chat-" + STREAMER

    kafka_ready = False
    while not kafka_ready:
        try:
            admin = KafkaAdminClient(bootstrap_servers='kafka:29092', api_version=(2, 8, 1))
            kafka_ready = True
            print("Kafka est prêt !")
        except Exception as e:
            print("En attente de Kafka...")
            time.sleep(5)

    consumer_from_kafka(topic)

if __name__ == "__main__":
    main()
