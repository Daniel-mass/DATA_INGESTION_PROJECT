from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaWeatherConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Define the schema of your Kafka JSON data
schema = StructType() \
    .add("location", StringType()) \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("timestamp", StringType())

# Step 3: Read stream from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Step 4: Convert Kafka value (bytes) to JSON fields
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
    .withColumn("data", from_json(col("json_value"), schema)) \
    .select("data.*")

# Step 5: Write streaming output to console
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
