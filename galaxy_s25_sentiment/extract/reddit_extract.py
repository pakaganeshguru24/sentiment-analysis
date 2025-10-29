"""Extract posts from Reddit and send them to Kafka (real-time)."""
import os
import datetime
import json
import praw
from kafka import KafkaProducer

# 🔹 Reddit API credentials
CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "XWB0jgmqRO7NFJCeb8_IUg")
CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "3KAx-qhzBo3EZYeZ3JRy5Q03eo38nA")
USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "sentiment_dashboard:v1.0 (by u/ganesh guru)")


# 🔹 Kafka settings (env vars supported)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")


def make_producer(bootstrap_servers: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )


def main():
    # Connect to Reddit
    reddit = praw.Reddit(client_id=CLIENT_ID,
                         client_secret=CLIENT_SECRET,
                         user_agent=USER_AGENT)

    subreddit = reddit.subreddit("all")
    query = "Galaxy S25 Ultra"

    producer = make_producer(KAFKA_BOOTSTRAP)
    sent = 0

    for post in subreddit.search(query, limit=50):
        text = f"{post.title} {post.selftext if post.selftext else ''}".strip()
        data = {
            "id": post.id,
            "title": post.title,
            "text": text,
            "score": getattr(post, "score", None),
            "date": datetime.datetime.fromtimestamp(post.created_utc).isoformat(),
            "url": getattr(post, "url", None),
            "source": "reddit"
        }
        try:
            producer.send(KAFKA_TOPIC, value=data)
            sent += 1
        except Exception as e:
            # keep going on send errors, but log to stdout
            print(f"Failed to send post {post.id} to Kafka: {e}")

    producer.flush()
    print(f"✅ Sent {sent} Reddit posts directly to Kafka topic '{KAFKA_TOPIC}'")


if __name__ == "__main__":
    main()
