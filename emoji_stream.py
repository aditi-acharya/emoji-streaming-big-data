from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, window, floor, count, when
from pyspark.sql.types import StringType, StructType, StructField

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaEmojiConsumer") \
    .getOrCreate()

# Set log level to ERROR to suppress unnecessary logs
spark.sparkContext.setLogLevel("ERROR")

# Define Kafka options
kafka_brokers = "localhost:9092"
input_topic = "emoji_topic3"
output_topic = "processed_emoji_topic"

kafka_options = {
    "kafka.bootstrap.servers": kafka_brokers,
    "subscribe": input_topic,
    "startingOffsets": "earliest"
}

# Define the minimum and maximum emojis per interval
MIN_EMOJIS_PER_INTERVAL = 100
MAX_EMOJIS_PER_INTERVAL = 600

# Function to dynamically adjust interval size based on emoji count
def get_dynamic_interval(emoji_count):
    return when(emoji_count < MIN_EMOJIS_PER_INTERVAL, MIN_EMOJIS_PER_INTERVAL) \
           .when(emoji_count > MAX_EMOJIS_PER_INTERVAL, MAX_EMOJIS_PER_INTERVAL) \
           .otherwise(emoji_count)

# Read stream from Kafka topic
emoji_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Convert Kafka message value (binary) to String
emoji_data = emoji_df.selectExpr("CAST(value AS STRING) as message")

# Define schema for incoming JSON
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", StringType(), True)  # If timestamp is numeric, use TimestampType
])

# Parse JSON data
parsed_data = emoji_data.select(
    expr("json_tuple(message, 'user_id', 'emoji_type', 'timestamp')").alias("user_id", "emoji_type", "timestamp")
)

# Add a processing time column for window-based aggregation
processed_data = parsed_data.withColumn("processing_time", expr("current_timestamp()"))

# Perform aggregation within a time window (2 seconds):
# - Group by emoji_type and 2-second processing time window
# - Dynamically calculate the scaled count based on the interval range
aggregated_data = processed_data \
    .groupBy(
        window(col("processing_time"), "2 seconds"),
        col("emoji_type")
    ) \
    .agg(
        count("*").alias("emoji_count")
    ) \
    .withColumn(
        "scaled_count",
        (floor(col("emoji_count") / get_dynamic_interval(col("emoji_count"))) + 1)
    ) \
    .selectExpr(
        "to_json(struct(*)) AS value"  # Convert structured data to JSON for Kafka
    )

# Write the aggregated data to the output Kafka topic
kafka_query = aggregated_data.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("topic", output_topic) \
    .option("checkpointLocation", "/tmp/spark/checkpoints/emoji_stream_kafka") \
    .outputMode("update") \
    .start()

# Minimal console confirmation for each trigger
def confirm_data_sent():
    print("Data is being processed and sent to Kafka.")

# Add a periodic confirmation message
from threading import Timer

def periodic_message():
    confirm_data_sent()
    Timer(10, periodic_message).start()

# Start the confirmation messages
periodic_message()

# Await termination
kafka_query.awaitTermination()
#emoji stream
