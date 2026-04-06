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
        group_id="twitch-consumer-group4", # On change encore pour forcer la lecture depuis le début
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 8, 1)
    )

    print("Consumer connecté, en attente de messages...")

    count = 0
    start = time.time()

    # REMPLACEMENT DE LA BOUCLE FOR PAR UNE BOUCLE POLL()
    while True:
        # On interroge Kafka toutes les 2 secondes (2000 millisecondes)
        messages_par_partition = consumer.poll(timeout_ms=2000)
        
        # Ligne de debug pour voir si le consumer est bien assigné au topic
        partitions = consumer.assignment()
        if not partitions:
            print("En attente d'assignation de la partition par Kafka...")
            continue
            
        if not messages_par_partition:
            # S'il n'y a pas de message, on le signale au lieu de bloquer silencieusement
            print(f"Assigné à {partitions}, mais aucun nouveau message...")
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

"""import time
import json
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer

OUTPUT_FILE = "/data/twitch.json"

def consumer_from_kafka(topic):
    open(OUTPUT_FILE, "w").close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='kafka:29092',
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="twitch-consumer-group2",
        value_deserializer=lambda x:json.loads(x.decode('utf-8'))
    )

    consumer.subscribe([topic])

    print("Consumer connecté, écriture dans JSON")
    """
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
"""   
    # compteur temps réel
    count = 0
    start = time.time()

    for message in consumer:
        print("MESSAGE BRUT: ", message)
        try:
            data = message.value

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
        
    """
"""Top 10 user plus actifs
    from collections import Counter

    counter = Counter()

    for message in consumer:
        data = message.value
        counter[data["user"]] += 1

        top_users = counter.most_common(5)
        print("Top 5:", top_users)

    """

"""
    consumer.close()


def main():
    print("consumer : ")
    STREAMER = "Locklear"
    topic = "twitch-chat-" + STREAMER

    # Boucle d'attente pour Kafka
    kafka_ready = False
    while not kafka_ready:
        try:
            admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
            kafka_ready = True
            print("Kafka est prêt !")
        except Exception:
            print("En attente de Kafka...")
            time.sleep(5)

    consumer_from_kafka(topic)


if __name__ == "__main__":
    main()"""