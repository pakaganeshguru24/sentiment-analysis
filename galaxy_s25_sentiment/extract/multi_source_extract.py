"""Extract Google Play reviews and Reddit posts, send to Kafka."""

import os
import json
from datetime import datetime
from google_play_scraper import reviews, Sort
import praw
from prawcore.exceptions import ResponseException

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
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", "sentiment_dashboard:v1.0 (ganesh guru)")

    if not client_id or not client_secret:
        print("⚠️ Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET environment variables.")
        return 0

    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        subreddit = reddit.subreddit("all")
        query = "Galaxy S25 Ultra"
        sent = 0
        for post in subreddit.search(query, limit=50):
            timestamp = datetime.utcfromtimestamp(post.created_utc).isoformat()
            data = {
                "id": post.id,
                "title": post.title,
                "text": post.selftext or "",
                "score": post.score,
                "at": timestamp,
                "source": "reddit"
            }
            producer.send(KAFKA_TOPIC, value=data)
            sent += 1

    except ResponseException as exc:
        print(f"⚠️ Reddit authentication failed: {exc}. Verify client credentials and permissions.")
        return 0
    except Exception as exc:
        print(f"⚠️ Failed to fetch Reddit posts: {exc}")
        return 0

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
