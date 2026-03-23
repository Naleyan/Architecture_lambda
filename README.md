# Toxicité des messages dans le tchat des streamers sur twitch


# Toxicité des messages dans le tchat des streamers sur twitch

## Contexte

Nous avons choisi de nous attaquer à une problématique majeure du web actuel : la toxicité dans les tchats Twitch. Avec des millions d'utilisateurs échangeant en direct, les flux de données provenant de l'extérieur sont massifs et souvent imprévisibles. En tant qu'étudiants en data science, notre objectif est de concevoir un système capable non seulement de détecter les insultes, mais aussi d'analyser les comportements malveillants sur le long terme pour aider les créateurs à protéger leur communauté.

C’est la première fois que nous mettons en œuvre une Architecture Lambda, et ce modèle nous a semblé idéal pour répondre aux contraintes de Twitch. Le défi technique réside dans la dualité du traitement : il faut être capable de réagir en quelques millisecondes pour modérer un message (vitesse), tout en étant capable de traiter des téraoctets de données historiques pour repérer des harceleurs récurrents (précision). L’architecture Lambda nous permet de ne pas sacrifier l’un pour l’autre en séparant le travail en deux circuits complémentaires.

Le cœur de notre dispositif commence par une phase d'ingestion robuste gérée par Apache Kafka. Kafka agit comme une plateforme de distribution qui reçoit les logs de tchats Twitch et les redirige simultanément vers nos deux couches de calcul. Pour la partie réactive, nous utilisons la "Real time layer" basée sur Spark Streaming. Cette couche traite les messages par micro-lots pour identifier immédiatement les pics de toxicité ou les mots interdits, permettant une mise à jour rapide des indicateurs de modération.

En parallèle, nous alimentons la "Batch layer" où les données brutes sont stockées de manière permanente sur Hadoop HDFS ou Amazon S3. Ici, nous utilisons Apache Spark  pour effectuer des calculs beaucoup plus lourds et précis sur l'historique complet des messages. Cette étape est cruciale car elle nous permet de corriger les approximations du temps réel et d'établir des profils de toxicité basés sur des semaines de logs, offrant ainsi une profondeur d'analyse impossible à obtenir en direct.

Enfin, la convergence de ces deux analyses se fait au niveau de la "Serving layer" , où nous avons choisi Cassandra  pour stocker et fusionner les résultats. Cette base de données distribue ensuite les informations vers notre interface de visualisation (Visu). Grâce à cette structure, l'utilisateur final dispose d'un tableau de bord complet qui combine la vigilance du direct et la fiabilité des données historiques, garantissant une modération à la fois rapide et intelligente.

# o L’architecture développée
# o Les traitements appliqués sur chacun des modules
# o Les liens entre les différents modules
# o Le résultat obtenu
# o Une analyse de celui-ci

# Setup

## How to use

### Using Docker Compose 
You will need Docker installed to follow the next steps. To create and run the image use the following command:

```bash
bash TP2_start-kafka-producer-consumer.sh 
```

The configuration will create 3 clusters with 5 containers:

- Consumer:
   - Consumer container
     - from python:3.11-alpine (version amd64 and arm64/v8)
- Producer
	- Publisher container
	  - from python:3.11-alpine (version amd64 and arm64/v8) 
- Kafka
   - kafka container
      - from confluentinc/cp-kafka:7.9.0 (version amd64 and arm64/v8)
   - kafdrop container
      - from obsidiandynamics/kafdrop:4.1.0 (version amd64 and arm64/v8)
   - zookeeper container
      - from confluentin/cp-zookeeper:7.9.0 (version amd64 and arm64/v8)

container|Exposed ports
---|---
consumer|
producer|8000
kafdrop|19000:9000
kafka|9091 9092
zookeeper|2181

The Publisher container sends data to Kafka.

The Consumer container is a script that aims to wait and receive messages from Kafka.

And the kafdrop container will provide acess to  web UI for viewing Kafka topics and browsing consumer groups that can be accessed at `http://localhost:19000`.

Il faut ouvrir un terminal sur les containers producer et consumer puis lancer le programmer le programme python.

```bash
docker exec -ti producer_twitch sh
```
```bash
cd app
python producer.py
```


```bash
docker exec -ti consumer_twitch sh
```

```bash
cd app
python consumer.py
```

## Project Structure
Below is a project structure created:

```
cmd .
├── README.md
├── kafka
│   ├── docker-compose.yml
├── consumer
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── app
│   │   ├── __init__.py
│   │   └── TP2_consumer.py
│   └── requirements.txt
└── producer
    ├── Dockerfile
    ├── docker-compose.yml
    ├── app
    │   ├── __init__.py
    │   ├── TP2_producer.py
    └── requirements.txt
```


# Changer de streamer
Copier coller le pseudo du STREAMER dans consumer, producer

# Lancement: 
cd kafka 
docker-compose up -d
cd ../spark
docker-compose up -d
(si erreur pour worker-b ou a:
docker-compose down --remove-orphans
docker-compose up -d --build)

cd ../consumer
docker-compose up -d
cd ../producer
docker-compose up -d

- (faire un fichier de lancement sh pour tout faire automatiquement)

# Vérification bon lancement:
docker ps

## Affiche
kafka_twitch
zookeeper_twitch
kafdrop_twitch
spark-master
spark-worker-a
spark-worker-b
consumer_twitch
producer_twitch

## Vérifier les interfaces graphique
Kafka:
http://localhost:19000/

Spark:
http://localhost:9090/

# Kafka (connexion Twitch)
## Docker
Dans un terminal:
cd producer
docker logs -f producer_twitch
-> Tout doit s'afficher en temps réel

Dans un autre terminal:
cd consumer
docker logs -f consumer_twitch
-> Tout doit s'afficher en temps réel

## Kafdrop
Aller dans le twitch-chat-STREAMER
All Messages
-> normalement ils sont tous là (Ajout en temps réel (actualiser))

## En local
data/twitch.json
-> normalement ils sont tous là (Ajout en temps réel (actualiser))

KAFKA OK

# Spark

## Spark Batch
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_batch.py

Spark doit afficher les données brutes, top users, messages/minute, mot fréquents, state = FINISHED

## Spark Streaming


## Cassandra


## Grafana