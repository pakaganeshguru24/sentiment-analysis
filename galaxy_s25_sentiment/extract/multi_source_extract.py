"""Extract Reddit posts and send them to Kafka."""

import json
import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables first, before importing
load_dotenv(override=True)

import praw

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
# Reddit Extraction
# ------------------------
def extract_reddit(producer, query, subreddit="all", count=50):
    """Extract Reddit posts for any query/subreddit."""
    sent = 0
    try:
        CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
        CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
        USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "sentiment_analysis")
        USERNAME = os.environ.get("REDDIT_USERNAME")
        PASSWORD = os.environ.get("REDDIT_PASSWORD")

        if not CLIENT_ID or not CLIENT_SECRET:
            raise RuntimeError("❌ Missing Reddit credentials in environment variables")

        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT,
            username=USERNAME if USERNAME else None,
            password=PASSWORD if PASSWORD else None
        )

        subreddit_obj = reddit.subreddit(subreddit)
        posts = subreddit_obj.search(query, limit=count)
        
        for post in posts:
            text = f"{post.title} {post.selftext if post.selftext else ''}".strip()
            timestamp = datetime.fromtimestamp(post.created_utc).isoformat()
            data = {
                "id": post.id,
                "title": post.title,
                "text": text,
                "score": post.score,
                "at": timestamp,
                "timestamp": timestamp,
                "source": "reddit",
                "topic": query,
                "subreddit": subreddit
            }
            try:
                producer.send(KAFKA_TOPIC, value=data)
                sent += 1
            except Exception as e:
                print(f"Failed to send Reddit post to Kafka: {e}")
        return sent
    except Exception as exc:
        print(f"❌ Error extracting Reddit posts: {exc}")
        return 0


# ------------------------
# Main Function
# ------------------------
def main():
    parser = argparse.ArgumentParser(description="Extract Reddit posts and send to Kafka")
    parser.add_argument("--topic", type=str, required=True, help="Search topic for Reddit")
    parser.add_argument("--subreddit", type=str, default="all", help="Reddit subreddit (default: all)")
    parser.add_argument("--count", type=int, default=50, help="Number of records to fetch (default: 50)")
    
    args = parser.parse_args()
    
    producer = make_producer(KAFKA_BOOTSTRAP)
    reddit_sent = extract_reddit(producer, args.topic, args.subreddit, args.count)
    producer.flush()
    
    if reddit_sent:
        print(f"✅ Sent {reddit_sent} Reddit posts to Kafka")
    else:
        print("⚠️ No Reddit posts available for this keyword")
    
    print("✅ Extraction complete")


if __name__ == "__main__":
    main()
