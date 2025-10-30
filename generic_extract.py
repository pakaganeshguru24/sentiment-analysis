"""Generic extraction for any topic - Google Play, Reddit, with dynamic queries."""
import os
import sys
import json
import argparse
from datetime import datetime
from kafka import KafkaProducer
from google_play_scraper import reviews, Sort
import praw


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")


def make_producer(bootstrap_servers: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        retries=5
    )


def extract_google(producer, app_id: str, topic: str, count: int = 100):
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
                "source": "google_play",
                "topic": topic
            }
            try:
                producer.send(KAFKA_TOPIC, value=data)
                sent += 1
            except Exception as e:
                print(f"Failed to send review: {e}")
        print(f"✅ Sent {sent} Google Play reviews for '{topic}' to Kafka")
        return sent
    except Exception as e:
        print(f"❌ Error extracting Google Play reviews: {e}")
        return 0


def extract_reddit(producer, query: str, subreddit: str = "all", count: int = 50):
    """Extract Reddit posts for any query/subreddit."""
    try:
        CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "DWBQ_jLR87O2tPu_X5hN8w")
        CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "hSs06uR4gi0OmWtK4tMXTrvA0Vh1dw")
        USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "sentiment_analysis")

        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT
        )

        subreddit_obj = reddit.subreddit(subreddit)
        posts = subreddit_obj.search(query, limit=count)
        sent = 0
        for post in posts:
            text = f"{post.title} {post.selftext if post.selftext else ''}".strip()
            data = {
                "id": post.id,
                "title": post.title,
                "text": text,
                "score": post.score,
                "at": datetime.utcfromtimestamp(post.created_utc).isoformat(),
                "source": "reddit",
                "topic": query,
                "subreddit": subreddit
            }
            try:
                producer.send(KAFKA_TOPIC, value=data)
                sent += 1
            except Exception as e:
                print(f"Failed to send post: {e}")
        print(f"✅ Sent {sent} Reddit posts for '{query}' from r/{subreddit} to Kafka")
        return sent
    except Exception as e:
        print(f"❌ Error extracting Reddit posts: {e}")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract sentiment data from any source")
    parser.add_argument("--source", type=str, choices=["google", "reddit", "both"], 
                       default="both", help="Data source")
    parser.add_argument("--topic", type=str, help="Search topic (for Reddit or Google Play)")
    parser.add_argument("--app-id", type=str, help="Google Play app ID")
    parser.add_argument("--subreddit", type=str, default="all", help="Reddit subreddit")
    parser.add_argument("--count", type=int, default=50, help="Number of records to fetch")
    
    args = parser.parse_args()
    
    if not args.topic:
        print("❌ Error: --topic is required")
        sys.exit(1)
    
    producer = make_producer(KAFKA_BOOTSTRAP)
    
    if args.source in ["google", "both"]:
        if args.app_id:
            extract_google(producer, args.app_id, args.topic, args.count)
        else:
            print("⚠️  Skipping Google Play (no --app-id provided)")
    
    if args.source in ["reddit", "both"]:
        extract_reddit(producer, args.topic, args.subreddit, args.count)
    
    producer.flush()
    print("✅ Extraction complete")
