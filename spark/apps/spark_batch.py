from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# 1. Création session Spark
spark = SparkSession.builder \
    .appName("Twitch Batch Analysis") \
    .getOrCreate()

# 2. Lecture du fichier JSON
df = spark.read.json("/opt/spark-data/twitch.json")

print("=== Données brutes ===")
df.show(5)

# 3. Nettoyage / typage
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# supprimer lignes vides
df = df.filter(col("user").isNotNull())

# 4. ANALYSES

# -----------------------------
# 🔹 Top utilisateurs
# -----------------------------
print("\n=== Top utilisateurs ===")

top_users = df.groupBy("user") \
    .agg(count("*").alias("nb_messages")) \
    .orderBy(desc("nb_messages"))

top_users.show(10)


# -----------------------------
# 🔹 Nombre total de messages
# -----------------------------
print("\n=== Nombre total de messages ===")
total = df.count()
print(f"Total messages : {total}")


# -----------------------------
# 🔹 Messages par utilisateur (simple)
# -----------------------------
print("\n=== Messages par utilisateur ===")
df.groupBy("user").count().show()


# -----------------------------
# 🔹 Messages par minute
# -----------------------------

print("\n=== Messages par minute ===")

messages_per_min = df.groupBy(
    window(col("timestamp"), "1 minute")
).count()

messages_per_min.show(truncate=False)


# -----------------------------
# 🔹 Mots les plus fréquents
# -----------------------------

print("\n=== Mots les plus fréquents ===")

words = df.select(
    explode(
        split(lower(col("message")), " ")
    ).alias("word")
)

top_words = words.groupBy("word") \
    .count() \
    .orderBy(desc("count"))

top_words.show(10)

# -----------------------------
# 🔹 Cassandra
# -----------------------------

spark = SparkSession.builder \
    .appName("Twitch-Spark-Cassandra") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

df = spark.read.json("/data/twitch.json")

df_users = df.groupBy("user").count()

df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="messages", keyspace="twitch") \
    .mode("append") \
    .save()

df_users \
    .withColumn("timestamp", current_timestamp()) \
    .withColumnRenamed("count", "value") \
    .withColumn("metric", lit("messages_per_user")) \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="stats", keyspace="twitch") \
    .mode("append") \
    .save()

# 5. Fin
spark.stop()