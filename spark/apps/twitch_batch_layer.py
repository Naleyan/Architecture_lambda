import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, explode, split, lower, regexp_replace, avg, to_timestamp
from cassandra.cluster import Cluster

def setup_cassandra():
    """Initialisation automatique du schéma Cassandra"""
    print("Initialisation du schéma Cassandra...")
    clstr = Cluster(['cassandra']) 
    session = clstr.connect()
    
    # 1. Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS twitch 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """) 
    
    # 2. Table : Top utilisateurs
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.batch_top_users (
            user text PRIMARY KEY,
            nb_messages int
        );
    """) 
    
    # 3. Table : Top mots
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.batch_top_words (
            word text PRIMARY KEY,
            count int
        );
    """)
    
    # 4. Table : Statistiques globales (changé en 'double' pour gérer la moyenne)
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.batch_global_stats (
            metric_name text PRIMARY KEY,
            value double
        );
    """)
    print("Schéma Cassandra prêt.")

def main():
    # =============================
    # 1. Préparation
    # =============================
    setup_cassandra()

    spark = SparkSession.builder \
        .appName("Twitch Batch Final Analysis") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # =============================
    # 2. Ingestion & Nettoyage (Parquet)
    # =============================
    parquet_path = "/opt/spark-data/twitch_parquet"

    print("Mise à jour du Data Lake : Conversion du JSON brut en Parquet...")
    # On lit le JSON avec les 46 000 messages (et plus)
    df_raw = spark.read.json("/opt/spark-data/twitch.json")
    
    # On écrase l'ancien Parquet avec les nouvelles données
    df_raw.write.mode("overwrite").parquet(parquet_path)

    print("Lecture depuis le Data Lake (Parquet)...")
    df = spark.read.parquet(parquet_path)

    # Nettoyage de base
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    df = df.filter(col("user").isNotNull())

    print("\n=== Aperçu des données ===")
    df.show(5)

    # =============================
    # 3. ANALYSES & ÉCRITURE CASSANDRA
    # =============================

    # -----------------------------
    # A. Top Utilisateurs
    # -----------------------------
    print("Calcul et sauvegarde : Top Utilisateurs...")
    messages_per_user = df.groupBy("user") \
        .agg(count("*").cast("int").alias("nb_messages"))
    
    messages_per_user.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="batch_top_users", keyspace="twitch") \
        .mode("append") \
        .save()

    # Affichage Console
    messages_per_user.orderBy(desc("nb_messages")).show(10)

    # -----------------------------
    # B. Mots les plus fréquents
    # -----------------------------
    print("Calcul et sauvegarde : Top Mots...")
    words = df.select(
        explode(
            split(regexp_replace(lower(col("message")), "[^a-zA-Z0-9 ]", ""), " ")
        ).alias("word")
    ).filter(col("word") != "")

    top_words = words.groupBy("word") \
        .agg(count("*").cast("int").alias("count")) \
        .orderBy(desc("count")) \
        .limit(100) # On garde le top 100 pour ne pas surcharger Cassandra

    top_words.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="batch_top_words", keyspace="twitch") \
        .mode("append") \
        .save()
    
    # Affichage Console
    top_words.show(10)

    # -----------------------------
    # C. Statistiques Globales
    # -----------------------------
    print("Calcul et sauvegarde : Statistiques Globales...")
    
    # Total messages
    total_messages = df.count()
    
    # Moyenne par utilisateur
    avg_row = messages_per_user.agg(avg("nb_messages").alias("moyenne")).collect()[0]["moyenne"]
    avg_messages = float(avg_row) if avg_row else 0.0

    print(f"Total messages : {total_messages}")
    print(f"Moyenne messages/user : {avg_messages:.2f}")

    # Création d'un mini DataFrame pour Cassandra
    global_stats_data = [
        ("total_messages", float(total_messages)),
        ("average_messages_per_user", avg_messages)
    ]
    df_global_stats = spark.createDataFrame(global_stats_data, ["metric_name", "value"])

    df_global_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="batch_global_stats", keyspace="twitch") \
        .mode("append") \
        .save()

    # =============================
    # 4. Fin
    # =============================
    print("\n✅ Job Batch terminé avec succès et données sauvegardées dans Cassandra !")
    spark.stop()

if __name__ == "__main__":
    main()