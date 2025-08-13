from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# Corrected schema to match producer's payload
schema = StructType() \
    .add("location", StringType()) \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("timestamp", StringType())

# Create Spark Session
spark = SparkSession.builder \
    .appName("WeatherDataPreprocessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka (fix topic name)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and transform
parsed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

processed_df = parsed_df.withColumn("temperature_f", col("temperature") * 9 / 5 + 32)

# Output to console
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("✅ Spark streaming started. Waiting for Kafka data on topic 'test-topic'...")

query.awaitTermination()
