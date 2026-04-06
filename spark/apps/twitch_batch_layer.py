import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, explode, split, lower, regexp_replace, avg, to_timestamp
from cassandra.cluster import Cluster

def setup_cassandra():
    print("Initialisation Cassandra...")
    clstr = Cluster(['cassandra']) 
    session = clstr.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS twitch 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """) 
    
    # Top utilisateurs
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.batch_top_users (
            user text PRIMARY KEY,
            nb_messages int
        );
    """) 
    
    # Top mots
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.batch_top_words (
            word text PRIMARY KEY,
            count int
        );
    """)
    
    # Statistiques globales
    session.execute("""
        CREATE TABLE IF NOT EXISTS twitch.batch_global_stats (
            metric_name text PRIMARY KEY,
            value double
        );
    """)
    print("Cassandra prêt.")

def main():

    setup_cassandra()

    spark = SparkSession.builder \
        .appName("Twitch Batch Final Analysis") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    parquet_path = "/opt/spark-data/twitch_parquet"

    print("Conversion JSON -> Parquet")

    df_raw = spark.read.json("/opt/spark-data/twitch.json")
    

    df_raw.write.mode("overwrite").parquet(parquet_path)

    df = spark.read.parquet(parquet_path)

    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    df = df.filter(col("user").isNotNull())

    print("\n=== Aperçu des données ===")
    df.show(5)


    # Top Utilisateurs
    print("Top Utilisateurs")
    messages_per_user = df.groupBy("user") \
        .agg(count("*").cast("int").alias("nb_messages"))
    
    messages_per_user.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="batch_top_users", keyspace="twitch") \
        .mode("append") \
        .save()


    messages_per_user.orderBy(desc("nb_messages")).show(10)


    # Mots les plus fréquents
    print("Top Mots")
    words = df.select(
        explode(
            split(regexp_replace(lower(col("message")), "[^a-zA-Z0-9 ]", ""), " ")
        ).alias("word")
    ).filter(col("word") != "")

    top_words = words.groupBy("word") \
        .agg(count("*").cast("int").alias("count")) \
        .orderBy(desc("count")) \
        .limit(100) 

    top_words.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="batch_top_words", keyspace="twitch") \
        .mode("append") \
        .save()
    

    top_words.show(10)

    print("Statistiques Globales")
    
    # Total messages
    total_messages = df.count()
    
    # Moyenne par utilisateur
    avg_row = messages_per_user.agg(avg("nb_messages").alias("moyenne")).collect()[0]["moyenne"]
    avg_messages = float(avg_row) if avg_row else 0.0

    print(f"Total messages : {total_messages}")
    print(f"Moyenne messages/user : {avg_messages:.2f}")

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

    print("\nBatch terminé dans Cassandra")
    spark.stop()

if __name__ == "__main__":
    main()