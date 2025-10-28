"""Extract Google Play reviews and Reddit posts, send to Kafka."""

import os
import json
from datetime import datetime
from kafka import KafkaProducer
from google_play_scraper import reviews, Sort
import praw

# ------------------------
# Kafka config
# ------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")

def make_producer(bootstrap_servers: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

# ------------------------
# Google Play extraction
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
# Reddit extraction
# ------------------------
def extract_reddit(producer):
    CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "DWBQ_jLR87O2tPu_X5hN8w")
    CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "hSs06uR4gi0OmWtK4tMXTrvA0Vh1dw")
    USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "galaxy_s25_sentiment")

    reddit = praw.Reddit(client_id=CLIENT_ID,
                         client_secret=CLIENT_SECRET,
                         user_agent=USER_AGENT)

    subreddit = reddit.subreddit("all")
    query = "Galaxy S25 Ultra"
    posts = subreddit.search(query, limit=50)
    sent = 0
    for post in posts:
        timestamp = datetime.utcfromtimestamp(post.created_utc).isoformat()
        data = {
            "id": post.id,
            "title": post.title,
            "text": post.selftext or "",
            "score": post.score,
            "at": timestamp,
            "source": "reddit"
        }
        try:
            producer.send(KAFKA_TOPIC, value=data)
            sent += 1
        except Exception as e:
            print(f"Failed to send Reddit post to Kafka: {e}")
    print(f"✅ Sent {sent} Reddit posts to Kafka")
    return sent

# ------------------------
# Main function
# ------------------------
def main():
    producer = make_producer(KAFKA_BOOTSTRAP)
    extract_google(producer)
    extract_reddit(producer)
    producer.flush()

if __name__ == "__main__":
    main()