from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from textblob import TextBlob

# -----------------------------
# Initialize Spark
# -----------------------------
spark = SparkSession.builder \
    .appName("GalaxyS25SentimentStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Read Kafka Stream
# -----------------------------
kafka_bootstrap = "localhost:9092"
kafka_topic = "sentiment-stream"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Kafka value is in bytes, convert to string
df_string = df.selectExpr("CAST(value AS STRING) as json_str")

# -----------------------------
# Parse JSON
# -----------------------------
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("score", IntegerType()),
    StructField("at", StringType()),
    StructField("source", StringType())
])

df_parsed = df_string.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# -----------------------------
# Sentiment Analysis
# -----------------------------
def get_sentiment(text):
    if text:
        polarity = TextBlob(text).sentiment.polarity
        if polarity > 0.1:
            return "positive"
        elif polarity < -0.1:
            return "negative"
        else:
            return "neutral"
    return "neutral"

sentiment_udf = udf(get_sentiment, StringType())
df_sentiment = df_parsed.withColumn("sentiment", sentiment_udf(col("text")))

# -----------------------------
# Output to CSV (or console for testing)
# -----------------------------
query = df_sentiment.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
