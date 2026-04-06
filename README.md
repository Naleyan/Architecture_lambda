# Analyse de la Toxicité sur Twitch - Architecture Lambda

Ce projet déploie une infrastructure Big Data complète pour capter, analyser et stocker en temps réel (et en différé) les messages des chats Twitch afin d'en évaluer l'activité et le niveau de toxicité.

Stack Technique : Docker, Apache Kafka, Apache Spark (Batch & Streaming), Cassandra, Grafana.

# Structure du Projet

.
├── README.md
├── data/
│   ├── twitch.json
│   └── twitch_parquet/
├── cassandra/
│   └── docker-compose.yml
├── grafana/
│   └── docker-compose.yml
├── kafka/
│   └── docker-compose.yml
├── consumer/
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       └── consumer.py
├── producer/
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── requirements_kafka.txt
│   └── app/
│       └── producer.py
└── spark/
│   ├── app\checkpoints/
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── start-spark.sh
│   └── apps/
│        ├── checkpoints/
│        ├── spark_batch.py
│        ├── kafka_spark_streaming.py
│        ├── twitch_batch_layer.py
│        └── twitch_speed_layer.py

# Configuration : Changer de Streamer

Avant de lancer l'infrastructure, vous pouvez choisir la chaîne Twitch à analyser.

- Allez dans les fichiers du consumer et du producer.

   Remplacez le nom du streamer dans le code (utilisez exactement le pseudo tel qu'il apparaît dans l'URL Twitch, en minuscules).

- Allez dans le fichier du speed layer, lors du setup de df_kafka pour se subscribe au bon topic.

# Lancement de l'Infrastructure (Docker)

Lancez les différents modules dans cet ordre précis pour respecter les dépendances :
Bash

## 1. Lancer Kafka et Zookeeper
cd kafka 
docker-compose up -d

## 2. Lancer le cluster Spark
cd ../spark
docker-compose up -d
### Note : Si erreur sur les workers, utilisez : docker-compose down --remove-orphans && docker-compose up -d --build

## 3. Lancer l'ingestion de données
cd ../consumer
docker-compose up -d
cd ../producer
docker-compose up -d

- Pensez à créer un script start.sh à la racine du projet pour automatiser cette séquence de lancement.

# Vérification du démarrage

Pour vérifier que tous les conteneurs tournent correctement :

docker ps

Vous devriez voir les conteneurs suivants actifs : kafka_twitch, zookeeper_twitch, kafdrop_twitch, spark-master, spark-worker-a, spark-worker-b, consumer_twitch, producer_twitch, et cassandra_twitch.
## Interfaces Graphiques

   - Kafdrop (Gestion Kafka) : http://localhost:19000/

   - Spark UI (Suivi des Jobs) : http://localhost:9090/

   - Grafana (Visualisation) : http://localhost:3000/ (Login: admin / admin)

# Validation de l'Ingestion (Kafka)

Vérifiez que la donnée circule bien en temps réel :

   - Côté Producer (Captation Twitch) :

    docker logs -f producer_twitch

   - Côté Consumer (Sauvegarde locale JSON) :

    docker logs -f consumer_twitch

   - Dans Kafdrop : Naviguez vers le topic twitch-chat-<streamer> > View Messages pour vérifier que les données brutes arrivent bien.

   - En local : Vérifiez que le fichier data/twitch.json se remplit bien.

# Traitements Big Data (Spark & Cassandra)

Connectez-vous au conteneur Spark Master pour exécuter les jobs :

docker exec -it spark-master bash

## Batch Layer (Traitement de l'historique)

Ce job lit le JSON, le convertit en Parquet, calcule les statistiques globales et sauvegarde dans Cassandra.


/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages com.datastax.spark:spark-cassandra-connector_2.13:3.5.1 \
  /opt/spark-apps/twitch_batch_layer.py

## Speed Layer (Traitement en Temps Réel)

Ce job lit Kafka en direct, applique les règles de détection de toxicité, et fait des "upserts" dans Cassandra.

   - Avant chaque relance d'un job Streaming, nettoyez les checkpoints pour éviter les erreurs de désynchronisation d'état :
   
rm -rf /opt/spark-apps/checkpoints/


/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1 \
  /opt/spark-apps/twitch_speed_layer.py

## Exploration des données (Cassandra)

Pour vérifier les tables générées et interroger la base de données :
Bash

### Entrer dans le conteneur Cassandra
docker exec -it cassandra_twitch cqlsh

Commandes CQL utiles :
SQL

USE twitch;
DESCRIBE TABLES;

-- Voir les résultats Batch
SELECT * FROM batch_global_stats;

-- Voir les résultats Streaming (Toxicité)
SELECT * FROM streaming_toxic_users;