from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StringType

# 1. Session Spark
spark = SparkSession.builder \
    .appName("Twitch Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .getOrCreate()

# 2. Lecture depuis Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "twitch-chat-kamet0") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Conversion JSON
schema = StructType() \
    .add("user", StringType()) \
    .add("message", StringType()) \
    .add("timestamp", StringType())

df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. Traitement streaming
top_users = df.groupBy("user").agg(count("*").alias("nb_messages"))

# 5. Output console
query = top_users.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()