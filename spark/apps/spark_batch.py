from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder \
    .appName("Twitch Batch Final Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()


parquet_path = "/opt/spark-data/twitch_parquet"

if not os.path.exists(parquet_path):
    print("Création Parquet...")
    df_raw = spark.read.json("/opt/spark-data/twitch.json")
    df_raw.write.mode("overwrite").parquet(parquet_path)

df = spark.read.parquet(parquet_path)

print("=== Données ===")
df.show(5)


# Nombre total de messages

total_messages = df.count()
print(f"\nTotal messages : {total_messages}")

# Messages par utilisateur

messages_per_user = df.groupBy("user") \
    .agg(count("*").alias("nb_messages"))

print("\n=== Messages par utilisateur ===")
messages_per_user.show()

# Moyenne messages / utilisateur

avg_messages = messages_per_user.agg(
    avg("nb_messages").alias("moyenne_messages")
)

print("\n=== Moyenne messages par utilisateur ===")
avg_messages.show()

# Top utilisateurs

top_users = messages_per_user.orderBy(desc("nb_messages"))

print("\n=== Top utilisateurs ===")
top_users.show(10)


# Mots les plus fréquents

words = df.select(
    explode(
        split(
            regexp_replace(lower(col("message")), "[^a-zA-Z0-9 ]", ""),
            " "
        )
    ).alias("word")
).filter(col("word") != "")

top_words = words.groupBy("word") \
    .count() \
    .orderBy(desc("count"))

print("\n=== Mots les plus fréquents ===")
top_words.show(20)


# Table messages
df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="messages", keyspace="twitch") \
    .mode("append") \
    .save()

# Table stats utilisateurs
messages_per_user \
    .withColumn("timestamp", current_timestamp()) \
    .withColumnRenamed("nb_messages", "value") \
    .withColumn("metric", lit("messages_per_user")) \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="stats", keyspace="twitch") \
    .mode("append") \
    .save()

# Table top mots
top_words \
    .withColumn("timestamp", current_timestamp()) \
    .withColumnRenamed("count", "value") \
    .withColumn("metric", lit("top_words")) \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="stats", keyspace="twitch") \
    .mode("append") \
    .save()

spark.stop()