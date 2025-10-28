"""Extract Google Play reviews and send to Kafka."""
import os
import json
from google_play_scraper import reviews, Sort
from kafka import KafkaProducer

# 🔹 Config
APP_ID = os.environ.get("GOOGLE_APP_ID", "com.samsung.android.voc")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")


def make_producer(bootstrap_servers: str):
    """
    Create a Kafka producer that serializes Python objects to JSON.
    Handles datetime objects automatically.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),  # ✅ default=str fixes datetime
        retries=5
    )


def main():
    # 🔹 Fetch reviews
    result, _ = reviews(
        APP_ID,
        lang="en",
        country="in",
        sort=Sort.NEWEST,
        count=100
    )

    producer = make_producer(KAFKA_BOOTSTRAP)
    sent = 0

    for rev in result:
        # Convert datetime to string safely
        timestamp = rev.get("at")
        if timestamp:
            timestamp = str(timestamp)

        data = {
            "id": rev.get("reviewId") or rev.get("userName"),
            "title": "",
            "text": rev.get("content", ""),
            "score": rev.get("score", None),
            "at": timestamp or "",
            "source": "google_play"
        }

        try:
            producer.send(KAFKA_TOPIC, value=data)
            sent += 1
        except Exception as e:
            print(f"Failed to send review to Kafka: {e}")

    producer.flush()
    print(f"✅ Sent {sent} Google Play reviews directly to Kafka topic '{KAFKA_TOPIC}'")


if __name__ == "__main__":
    main()
