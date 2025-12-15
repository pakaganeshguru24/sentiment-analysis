"""Wrapper functions for backward compatibility with dashboard apps.
This module provides simple wrapper functions that return data for dashboards.
"""
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from google_play_scraper import reviews, Sort
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


def extract_google_play_reviews(app_id: str, count: int = 100):
    """Extract Google Play reviews and return as list - for dashboard use."""
    try:
        result, _ = reviews(
            app_id,
            lang="en",
            country="us",
            sort=Sort.NEWEST,
            count=count
        )
        
        reviews_data = []
        for rev in result:
            timestamp = rev.get("at")
            if timestamp:
                timestamp = str(timestamp)
            
            reviews_data.append({
                "reviewId": rev.get("reviewId") or rev.get("userName"),
                "userName": rev.get("userName", ""),
                "content": rev.get("content", ""),
                "score": rev.get("score"),
                "at": timestamp or "",
                "thumbsUpCount": rev.get("thumbsUpCount", 0)
            })
        
        return reviews_data
    except Exception as e:
        print(f"Error extracting Google Play reviews: {e}")
        return []


def extract_both_sources(topic: str, app_id: str = None, subreddit: str = "all", 
                         google_count: int = 100, reddit_count: int = 50):
    """Extract from both Google Play and Reddit - returns combined list."""
    all_data = []
    
    if app_id:
        google_data = extract_google_play_reviews(app_id, count=google_count)
        all_data.extend(google_data)
    
    reddit_data = extract_reddit_reviews(topic, subreddit=subreddit, limit=reddit_count)
    all_data.extend(reddit_data)
    
    return all_data


def search_google_play_app(query: str):
    """Search for Google Play app by query - returns app_id suggestion."""
    # This is a placeholder - actual implementation would use search API
    # For now, return a common format
    return {
        "app_id": "com.android.chrome",  # Example
        "name": query,
        "suggestion": f"Try: com.{query.lower().replace(' ', '.')}"
    }
