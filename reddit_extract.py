"""Extract posts from Reddit and send them to Kafka (real-time)."""
import datetime
import json
import os

from dotenv import load_dotenv
import praw
import requests
from kafka import KafkaProducer
from prawcore.exceptions import ResponseException

# -----------------------------------------------------
# 1️⃣ Load environment variables (from .env if exists)
# -----------------------------------------------------
load_dotenv()

# -----------------------------------------------------
# 2️⃣ Reddit API credentials (with safe fallbacks)
# -----------------------------------------------------
CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "Y3CHl7Szl3vxdUeOznXghA")
CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "lMIMha0L8x0QLMxUiGwr0vnjxZcDJg")
USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "sentiment_dashboard:v1.0 (by u/Due_Yellow8117)")

# -----------------------------------------------------
# 3️⃣ Kafka settings (env vars supported)
# -----------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")
QUERY = "Galaxy S25 Ultra"


def make_producer(bootstrap_servers: str):
    """Initialize Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )


def extract_posts_with_praw(subreddit_name: str, query: str):
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT
    )
    reddit.read_only = True
    posts = []
    for post in reddit.subreddit(subreddit_name).search(query, limit=50):
        text = f"{post.title} {post.selftext or ''}".strip()
        posts.append(
            {
                "id": post.id,
                "title": post.title,
                "text": text,
                "score": getattr(post, "score", None),
                "date": datetime.datetime.fromtimestamp(post.created_utc).isoformat(),
                "url": getattr(post, "url", None),
                "source": "reddit"
            }
        )
    return posts


def extract_posts_via_http(query: str):
    response = requests.get(
        "https://www.reddit.com/search.json",
        headers={"User-Agent": USER_AGENT},
        params={"q": query, "limit": 50, "sort": "new"},
        timeout=20
    )
    response.raise_for_status()
    posts = []
    for child in response.json().get("data", {}).get("children", []):
        payload = child.get("data", {})
        text = f"{payload.get('title', '')} {payload.get('selftext', '')}".strip()
        created = payload.get("created_utc")
        date = datetime.datetime.fromtimestamp(created).isoformat() if created else None
        posts.append(
            {
                "id": payload.get("id"),
                "title": payload.get("title"),
                "text": text,
                "score": payload.get("score"),
                "date": date,
                "url": payload.get("url_overridden_by_dest") or payload.get("url"),
                "source": "reddit"
            }
        )
    return posts


def collect_posts(subreddit_name: str, query: str):
    try:
        return extract_posts_with_praw(subreddit_name, query)
    except ResponseException as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 401:
            print("⚠️ Reddit API returned 401. Falling back to public endpoint.")
            return extract_posts_via_http(query)
        raise


def main():
    producer = make_producer(KAFKA_BOOTSTRAP)
    sent = 0
    try:
        for data in collect_posts("all", QUERY):
            try:
                producer.send(KAFKA_TOPIC, value=data)
                sent += 1
            except Exception as e:
                print(f"⚠️ Failed to send post {data.get('id')} to Kafka: {e}")
        producer.flush()
        print(f"✅ Sent {sent} Reddit posts directly to Kafka topic '{KAFKA_TOPIC}'")
    except Exception as e:
        print(f"❌ Reddit extraction failed: {e}")


if __name__ == "__main__":
    main()
