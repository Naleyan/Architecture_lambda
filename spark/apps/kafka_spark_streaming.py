from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, from_json, count, window, lower, to_timestamp, desc
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import col, from_json, count, window, lower, current_timestamp, desc


spark = SparkSession.builder \
    .appName("Twitch Streaming - 4 Metrics") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "twitch-chat-jynxzi") \
    .option("startingOffsets", "earliest") \
    .load()


schema = StructType() \
    .add("user", StringType()) \
    .add("message", StringType()) \
    .add("timestamp", StringType())

df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())


# Toxicité

toxic_words = ["idiot", "noob", "trash", "stupid", "hate"]
regex_pattern = "(?i)\\b(" + "|".join(toxic_words) + ")\\b"
df_clean = df.withColumn("is_toxic", col("message").rlike(regex_pattern))

df_watermarked = df_clean.withWatermark("timestamp", "10 seconds")



# Activité globale 
global_activity = df_watermarked.groupBy(
    window(col("timestamp"), "10 seconds")
).agg(count("*").alias("total_messages"))

# Suivi des utilisateurs 
user_activity = df_watermarked.groupBy("user") \
    .agg(count("*").alias("nb_messages")) \
    .orderBy(desc("nb_messages"))

# Activité toxique globale 
toxic_activity = df_watermarked.filter(col("is_toxic") == True) \
    .groupBy(window(col("timestamp"), "10 seconds")) \
    .agg(count("*").alias("total_toxic_messages"))

# Utilisateurs toxiques
toxic_users = df_watermarked.filter(col("is_toxic") == True) \
    .groupBy("user") \
    .agg(count("*").alias("nb_toxic_messages")) \
    .orderBy(desc("nb_toxic_messages"))



query1 = global_activity.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/metric1_global") \
    .trigger(processingTime="5 seconds") \
    .start()

query2 = user_activity.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/metric2_users") \
    .trigger(processingTime="5 seconds") \
    .start()

query3 = toxic_activity.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/metric3_toxic_time") \
    .trigger(processingTime="5 seconds") \
    .start()

query4 = toxic_users.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/metric4_toxic_users") \
    .trigger(processingTime="5 seconds") \
    .start()

spark.streams.awaitAnyTermination()