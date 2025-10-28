from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, IntegerType
from textblob import TextBlob

# ------------------------
# Spark session
# ------------------------
spark = SparkSession.builder \
    .appName("GalaxyS25SentimentStreaming") \
    .getOrCreate()

# ------------------------
# Kafka config
# ------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "sentiment-stream"

# ------------------------
# Schema for incoming JSON
# ------------------------
schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("text", StringType()) \
    .add("score", IntegerType()) \
    .add("at", StringType()) \
    .add("source", StringType())

# ------------------------
# Read from Kafka
# ------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary 'value' to string
df = df.selectExpr("CAST(value AS STRING) as json")
df = df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# ------------------------
# Sentiment analysis UDF
# ------------------------
def get_sentiment(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "positive"
    elif polarity < -0.1:
        return "negative"
    else:
        return "neutral"

sentiment_udf = udf(get_sentiment, StringType())
df = df.withColumn("sentiment", sentiment_udf(col("text")))

# ------------------------
# Write output to CSV for Streamlit
# ------------------------
query = df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "data/sentiment_stream_output") \
    .option("checkpointLocation", "data/checkpoint") \
    .start()

query.awaitTermination()