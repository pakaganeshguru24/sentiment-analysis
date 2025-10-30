"""Extract Google Play reviews and Reddit posts, send to Kafka."""

import json
import os
from datetime import datetime

from google_play_scraper import reviews, Sort

from reddit_extract import QUERY, collect_posts

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

# ------------------------
# Kafka Config
# ------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")


def make_producer(bootstrap_servers: str):
    if KafkaProducer is None:
        raise RuntimeError("Install kafka-python to enable Kafka integration")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )


# ------------------------
# Google Play Extraction
# ------------------------
def extract_google(producer, app_id="com.samsung.android.voc"):
    result, _ = reviews(
        app_id,
        lang="en",
        country="in",
        sort=Sort.NEWEST,
        count=100
    )
    sent = 0
    for rev in result:
        timestamp = rev.get("at")
        if timestamp:
            timestamp = timestamp.isoformat()
        data = {
            "id": rev.get("reviewId") or rev.get("userName"),
            "title": "",
            "text": rev.get("content", ""),
            "score": rev.get("score"),
            "at": timestamp or "",
            "source": "google_play"
        }
        try:
            producer.send(KAFKA_TOPIC, value=data)
            sent += 1
        except Exception as e:
            print(f"Failed to send Google review to Kafka: {e}")
    print(f"✅ Sent {sent} Google Play reviews to Kafka")
    return sent


# ------------------------
# Reddit Extraction
# ------------------------
def extract_reddit(producer):
    sent = 0
    try:
        for post in collect_posts("all", QUERY):
            timestamp = post.get("date")
            if timestamp:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                timestamp = timestamp.isoformat()
            data = {
                "id": post.get("id"),
                "title": post.get("title", ""),
                "text": post.get("text", ""),
                "score": post.get("score"),
                "at": timestamp or "",
                "source": "reddit"
            }
            producer.send(KAFKA_TOPIC, value=data)
            sent += 1
    except Exception as exc:
        print(f"⚠️ Failed to fetch Reddit posts: {exc}")
        return 0

    if sent == 0:
        print("⚠️ No Reddit posts were sent to Kafka")
    else:
        print(f"✅ Sent {sent} Reddit posts to Kafka")
    return sent


# ------------------------
# Main Function
# ------------------------
def main():
    producer = make_producer(KAFKA_BOOTSTRAP)
    extract_google(producer)
    extract_reddit(producer)
    producer.flush()


if __name__ == "__main__":
    main()
