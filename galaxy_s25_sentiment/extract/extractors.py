"""Wrapper functions for Reddit-only extraction used by the dashboards.
This module returns Reddit data in a simple, dashboard-friendly format.
"""
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import praw

load_dotenv(override=True)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")


def extract_reddit_reviews(query: str, subreddit: str = "all", limit: int = 50):
    """Extract Reddit posts and return as list - for dashboard use."""
    try:
        CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
        CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
        USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "sentiment_analysis")
        USERNAME = os.environ.get("REDDIT_USERNAME")
        PASSWORD = os.environ.get("REDDIT_PASSWORD")

        if not CLIENT_ID or not CLIENT_SECRET:
            raise RuntimeError("Missing Reddit credentials in environment variables")

        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT,
            username=USERNAME if USERNAME else None,
            password=PASSWORD if PASSWORD else None
        )

        subreddit_obj = reddit.subreddit(subreddit)
        posts_data = []
        
        for post in subreddit_obj.search(query, limit=limit):
            text = f"{post.title} {post.selftext if post.selftext else ''}".strip()
            posts_data.append({
                "id": post.id,
                "title": post.title,
                "text": text,
                "score": post.score,
                "date": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                "source": "reddit",
                "subreddit": subreddit
            })
        
        return posts_data
    except Exception as e:
        print(f"Error extracting Reddit posts: {e}")
        return []

