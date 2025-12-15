"""Extract Google Play reviews and Reddit posts, send to Kafka."""

import json
import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables first, before importing
load_dotenv(override=True)

from google_play_scraper import reviews, Sort
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
# Google Play Extraction
# ------------------------
def extract_google(producer, app_id, topic, count=100):
    """Extract reviews from Google Play for any app."""
    try:
        result, _ = reviews(
            app_id,
            lang="en",
            country="in",
            sort=Sort.NEWEST,
            count=count
        )
        sent = 0
        for rev in result:
            timestamp = rev.get("at")
            if timestamp:
                timestamp = str(timestamp)
            data = {
                "id": rev.get("reviewId") or rev.get("userName"),
                "title": "",
                "text": rev.get("content", ""),
                "score": rev.get("score"),
                "at": timestamp or "",
                "timestamp": timestamp or "",
                "source": "google",
                "topic": topic
            }
            try:
                producer.send(KAFKA_TOPIC, value=data)
                sent += 1
            except Exception as e:
                print(f"Failed to send Google review to Kafka: {e}")
        return sent
    except Exception as e:
        print(f"❌ Error extracting Google Play reviews: {e}")
        return 0


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
    parser = argparse.ArgumentParser(description="Extract sentiment data from Google Play and Reddit")
    parser.add_argument("--source", type=str, choices=["google", "reddit", "both"], 
                       default="reddit", help="Data source (default: reddit)")
    parser.add_argument("--topic", type=str, help="Search topic for Reddit or app name reference")
    parser.add_argument("--app-id", type=str, help="Google Play app ID (required for Google Play)")
    parser.add_argument("--subreddit", type=str, default="all", help="Reddit subreddit (default: all)")
    parser.add_argument("--count", type=int, default=50, help="Number of records to fetch (default: 50)")
    
    args = parser.parse_args()
    
    if not args.topic:
        print("❌ Error: --topic is required")
        print("\nExample usage:")
        print("  python multi_source_extract.py --topic 'ExampleTopic' --app-id '<com.example.app.id>'")
        print("  python multi_source_extract.py --source reddit --topic 'iPhone' --subreddit 'apple'")
        sys.exit(1)
    
    producer = make_producer(KAFKA_BOOTSTRAP)
    google_sent = 0
    reddit_sent = 0
    
    if args.source in ["google", "both"]:
        if args.app_id:
            google_sent = extract_google(producer, args.app_id, args.topic, args.count)
        else:
            print("⚠️  Skipping Google Play (no --app-id provided)")
    
    if args.source in ["reddit", "both"]:
        reddit_sent = extract_reddit(producer, args.topic, args.subreddit, args.count)
    
    producer.flush()
    
    if args.source in ["google", "both"]:
        if google_sent:
            print(f"✅ Sent {google_sent} Google Play reviews to Kafka")
        else:
            print("⚠️ No Google Play reviews available for this keyword")
    
    if args.source in ["reddit", "both"]:
        if reddit_sent:
            print(f"✅ Sent {reddit_sent} Reddit posts to Kafka")
        else:
            print("⚠️ No Reddit posts available for this keyword")
    
    if google_sent and reddit_sent:
        print("📊 Streamlit dashboard updated with data from both sources")
    
    print("✅ Extraction complete")


if __name__ == "__main__":
    main()
