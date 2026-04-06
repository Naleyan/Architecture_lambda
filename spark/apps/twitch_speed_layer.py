from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import col, from_json, count, window, current_timestamp, desc
from cassandra.cluster import Cluster

def setup_cassandra():
    """Initialisation des 4 tables de streaming dans Cassandra"""
    print("Initialisation de Cassandra...")
    clstr = Cluster(['cassandra']) 
    session = clstr.connect()
    
    # 1. Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS twitch 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """) 
    
    # 2. Table : Activité globale temporelle
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.streaming_global_activity (
            window_start timestamp PRIMARY KEY,
            total_messages int
        );
    """) 

    # 3. Table : Top utilisateurs temps réel
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.streaming_user_activity (
            user text PRIMARY KEY,
            nb_messages int
        );
    """) 

    # 4. Table : Activité toxique temporelle
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.streaming_toxic_activity (
            window_start timestamp PRIMARY KEY,
            total_toxic_messages int
        );
    """) 

    # 5. Table : Utilisateurs toxiques
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.streaming_toxic_users (
            user text PRIMARY KEY,
            nb_toxic_messages int
        );
    """) 
    print("Schéma Cassandra prêt pour les 4 métriques.")

def main():
    # =============================
    # 1. Préparer Cassandra
    # =============================
    setup_cassandra()

    # =============================
    # 2. Configuration Spark
    # =============================
    scala_version = '2.13'
    spark_version = '4.0.1' 
    cassandra_version = '3.5.1' 
    
    packages = [
        f'com.datastax.spark:spark-cassandra-connector_{scala_version}:{cassandra_version}',
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
    ] 

    spark = SparkSession.builder \
        .appName("Twitch-Speed-Layer-Final") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # =============================
    # 3. Lecture Kafka
    # =============================
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "twitch-chat-jynxzi") \
        .option("startingOffsets", "earliest") \
        .load()

    # =============================
    # 4. Traitement & Nettoyage
    # =============================
    schema = StructType() \
        .add("user", StringType()) \
        .add("message", StringType()) \
        .add("timestamp", StringType())

    df = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", current_timestamp()) # Correction temporelle

    # Toxicité via regex
    toxic_words = ["idiot", "noob", "trash", "stupid", "hate"]
    regex_pattern = "(?i)\\b(" + "|".join(toxic_words) + ")\\b"
    df_clean = df.withColumn("is_toxic", col("message").rlike(regex_pattern))

    df_watermarked = df_clean.withWatermark("timestamp", "10 seconds")

    # =============================
    # 5. LES 4 AGREGATIONS (Préparées pour Cassandra)
    # =============================
    
    # M1. Activité globale (extraction de window.start et cast en int)
    global_activity = df_watermarked.groupBy(window(col("timestamp"), "10 seconds")) \
        .agg(count("*").cast("int").alias("total_messages")) \
        .select(col("window.start").alias("window_start"), col("total_messages"))

    # M2. Suivi des utilisateurs
    user_activity = df_watermarked.groupBy("user") \
        .agg(count("*").cast("int").alias("nb_messages"))

    # M3. Activité toxique globale
    toxic_activity = df_watermarked.filter(col("is_toxic") == True) \
        .groupBy(window(col("timestamp"), "10 seconds")) \
        .agg(count("*").cast("int").alias("total_toxic_messages")) \
        .select(col("window.start").alias("window_start"), col("total_toxic_messages"))

    # M4. Utilisateurs toxiques
    toxic_users = df_watermarked.filter(col("is_toxic") == True) \
        .groupBy("user") \
        .agg(count("*").cast("int").alias("nb_toxic_messages"))

    # =============================
    # 6. ECRITURE VERS CASSANDRA
    # =============================
    
    # Fonction dynamique pour écrire dans n'importe quelle table
    def write_to_cassandra(table_name):
        def _writer(writeDF, epochId):
            writeDF.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode('append') \
                .options(table=table_name, keyspace="twitch") \
                .save()
        return _writer

    print("Démarrage des 4 flux Speed Layer vers Cassandra...")

    # On utilise le mode "update" pour tout le monde (Cassandra fera des upserts par clé primaire)
    query1 = global_activity.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra("streaming_global_activity")) \
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/speed_global") \
        .start()

    query2 = user_activity.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra("streaming_user_activity")) \
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/speed_users") \
        .start()

    query3 = toxic_activity.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra("streaming_toxic_activity")) \
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/speed_toxic_time") \
        .start()

    query4 = toxic_users.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra("streaming_toxic_users")) \
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/speed_toxic_users") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()