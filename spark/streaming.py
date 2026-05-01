from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaMoviesStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Схема данных
schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("budget", StringType()),
    StructField("revenue", StringType()),
    StructField("vote_count", StringType()),
    StructField("popularity", StringType())
])

# Чтение из Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "movies") \
    .load()

df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 1. Преобразование
df_transformed = df.select(
    col("id").cast("long").alias("tmdbId"),
    col("title"),
    col("budget").cast("double"),
    col("revenue").cast("double"),
    col("vote_count").cast("int"),
    col("popularity").cast("double")
)

# 2. Фильтрация
df_filtered = df_transformed.filter(
    (col("budget") > 0) &
    (col("revenue") > 0) &
    (col("vote_count") > 10)
)

# 3. Агрегация
df_aggregated = df_filtered \
    .groupBy(window(current_timestamp(), "1 minute")) \
    .agg(
        avg("revenue").alias("avg_revenue"),
        count("*").alias("movie_count")
    )

# Вывод в Kafka
output = df_aggregated.selectExpr("to_json(struct(*)) AS value")

query = output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "movies_aggregated") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("update") \
    .start()

query.awaitTermination()