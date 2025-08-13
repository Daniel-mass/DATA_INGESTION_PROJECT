from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType
import time

# Define schema matching producer
schema = StructType() \
    .add("location", StringType()) \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("timestamp", StringType())

# Create Spark Session
spark = SparkSession.builder \
    .appName("📡 Real-Time Weather Stream Processor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka and extract fields
parsed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Add Fahrenheit conversion
processed_df = parsed_df.withColumn("temperature_f", col("temperature") * 9 / 5 + 32)

# Enhance output to console
def pretty_console_output(df, epoch_id):
    print(f"\n🌀 ====== Weather Stream Batch #{epoch_id} ====== 🌀")
    df.show(truncate=False)
    print(f"📦 Rows in this batch: {df.count()}")
    print(f"🕐 Batch processed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("===========================================\n")

# Write to console using foreachBatch for better formatting
query = processed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(pretty_console_output) \
    .start()

print("✅ Spark streaming started. Listening for weather data from Kafka topic: 'test-topic'...\n")

query.awaitTermination()
