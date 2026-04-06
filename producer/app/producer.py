import socket
import ssl
import json
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

def main():
    # =========================
    # CONFIGURATION
    # =========================
    KAFKA_BROKER = "kafka:29092"
    SERVER = "irc.chat.twitch.tv"
    PORT = 6697  # TLS
    NICK = "marthah_h"
    TOKEN = "oauth:qc9wckdsq54mozcd1tfagoxl3bkg03"
    STREAMER = "jynxzi"
    CHANNEL = "#" + STREAMER
    topic = 'twitch-chat-' + STREAMER
    num_partition = 1

    # =========================
    # ATTENTE DE KAFKA & TOPIC
    # =========================
    kafka_ready = False
    admin = None
    while not kafka_ready:
        try:
            # Forcer api_version corrige un bug connu de kafka-python avec les Kafka récents
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, api_version=(2, 8, 1))
            kafka_ready = True
            print("Kafka est prêt pour le Producer !")
        except Exception as e:
            print("En attente de Kafka...", e)
            time.sleep(5)

    server_topics = admin.list_topics()
    if topic not in server_topics:
        try:
            print("Création du topic :", topic)
            topic1 = NewTopic(name=topic, num_partitions=num_partition, replication_factor=1)
            admin.create_topics([topic1])
        except Exception as e:
            print("Erreur création topic :", e)
    else:
        print(topic, "est déjà créé")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER, 
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(2, 8, 1) # Crucial ici aussi !
    )

    # =========================
    # CONNEXION IRC (TLS)
    # =========================
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    context = ssl.create_default_context()
    sock = context.wrap_socket(sock, server_hostname="irc.chat.twitch.tv")
    sock.connect((SERVER, PORT))

    sock.send(f"PASS {TOKEN}\r\n".encode("utf-8"))
    sock.send(f"NICK {NICK}\r\n".encode("utf-8"))
    sock.send(f"JOIN {CHANNEL}\r\n".encode("utf-8"))
    print("Connecté au chat Twitch")

    # =========================
    # LECTURE DU FLUX
    # =========================
    while True:
        resp = sock.recv(2048).decode("utf-8")

        if resp.startswith("PING"):
            sock.send("PONG :tmi.twitch.tv\r\n".encode("utf-8"))
            continue

        for line in resp.split("\r\n"):
            if "PRIVMSG" in line:
                try:
                    prefix, message = line.split("PRIVMSG", 1)
                    user = prefix.split("!")[0][1:]
                    content = message.split(":", 1)[1]

                    payload = {
                        "user": user,
                        "message": content.strip()
                    }
                    
                    # Envoi à Kafka AVEC vérification
                    future = producer.send(topic, value=payload)
                    record_metadata = future.get(timeout=10) # Si ça échoue, ça plantera ici !
                    
                    print(f"[{record_metadata.partition}:{record_metadata.offset}] {user}: {content.strip()}")
                    
                except Exception as e:
                    print("ERREUR D'ENVOI KAFKA:", e)

if __name__ == "__main__":
    main()
"""import socket
import ssl
import json
import time
from kafka import KafkaProducer, KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic


def main():
    # =========================
    # CONFIGURATION
    # =========================
    KAFKA_BROKER = "kafka:29092"

    SERVER = "irc.chat.twitch.tv"
    PORT = 6697  # TLS
    NICK = "marthah_h"
    TOKEN = "oauth:qc9wckdsq54mozcd1tfagoxl3bkg03"
    STREAMER = "Locklear" # Kamet0
    CHANNEL = "#" + STREAMER  # ex: #pokimane

    topic = 'twitch-chat-' + STREAMER
    num_partition = 1
    
    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')
    
    admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    server_topics = admin.list_topics()

    print(server_topics)
    # création du topic si celui-ci n'est pas déjà créé
    if topic not in server_topics:
        try:
            print("create new topic :", topic)

            topic1 = NewTopic(name=topic,
                             num_partitions=num_partition,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            print("error")
            pass
    else:
        print(topic,"est déjà créé")

    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, 
                             value_serializer=lambda v: json.dumps(v).encode("utf-8")
                             )


    # =========================
    # CONNEXION IRC (TLS)
    # =========================
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    context = ssl.create_default_context()
    sock = context.wrap_socket(sock, server_hostname="irc.chat.twitch.tv")
    sock.connect((SERVER, PORT))

    # Authentification
    sock.send(f"PASS {TOKEN}\r\n".encode("utf-8"))
    sock.send(f"NICK {NICK}\r\n".encode("utf-8"))
    sock.send(f"JOIN {CHANNEL}\r\n".encode("utf-8"))

    print("Connecté au chat Twitch")

    # =========================
    # LECTURE DU FLUX
    # =========================
    while True:
        resp = sock.recv(2048).decode("utf-8")

        # Réponse au PING (obligatoire)
        if resp.startswith("PING"):
            sock.send("PONG :tmi.twitch.tv\r\n".encode("utf-8"))
            continue

        for line in resp.split("\r\n"):
            if "PRIVMSG" in line:
                try:
                    # Format :
                    # :user!user@user.tmi.twitch.tv PRIVMSG #channel :message
                    prefix, message = line.split("PRIVMSG", 1)
                    user = prefix.split("!")[0][1:]
                    content = message.split(":", 1)[1]

                    print(f"{user}: {content}")
                    payload = {
                        "user": user,
                        "message": content.strip()
                        }
                    #producer.send(topic, value=payload)
                    
                    future = producer.send(topic, value=payload)
                    future.get(timeout=10)

                except Exception as e:
                    print("Error processing message:", e)

if __name__ == "__main__":
    main()"""