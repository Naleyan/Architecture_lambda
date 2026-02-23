import socket
import ssl
import json
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
    STREAMER = "Kamet0"
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
                    producer.send(topic, value=payload)
                    
                except Exception as e:
                    print("Error processing message:", e)

if __name__ == "__main__":
    main()