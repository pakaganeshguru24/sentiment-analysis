import io
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
from pandas.api.types import is_bool_dtype, is_numeric_dtype
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -----------------------
# Configuration
# -----------------------
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "streamlit-dashboard")
MAX_HISTORY = 500
MAX_FETCH = 200
DEFAULT_REFRESH_SECONDS = 5
KAFKA_TOPICS = os.environ.get("KAFKA_TOPICS")
CONSUMER_TOPICS = tuple(
    topic.strip()
    for topic in (KAFKA_TOPICS.split(",") if KAFKA_TOPICS else [KAFKA_TOPIC])
    if topic.strip()
)
analyzer = SentimentIntensityAnalyzer()
SOURCE_BADGES = {
    "Reddit": "🔴 Reddit",
}

st.set_page_config(page_title="Sentiment Dashboard", layout="wide")
st.title("Real-time Sentiment Analysis Dashboard")
st.caption("Monitor live streaming data from Kafka or explore your own datasets with interactive visualizations.")

# -----------------------
# Sidebar Navigation
"""
Real-time Sentiment Analysis Dashboard for Dynamic Topics.
Fetches data from Reddit, sends to Kafka, analyzes with VADER.
"""

import json
import os
import threading
"""
Real-time Sentiment Analysis Dashboard for Dynamic Topics.
Fetches data from Reddit, sends to Kafka, analyzes with VADER.
"""

import json
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Load environment variables
load_dotenv(override=True)

# Kafka imports
try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:
    KafkaConsumer = None
    KafkaProducer = None

# Import custom extractors (Reddit-only)
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'extract'))
from extractors import extract_reddit_reviews

# -----------------------
# Configuration
# -----------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")
MAX_CACHE = 500  # Max messages to keep in memory
MAX_FETCH_PER_REFRESH = 100

# VADER Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()


# -----------------------
# Streamlit Page Config
# -----------------------
st.set_page_config(
    page_title="Real-time Sentiment Analyzer",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 Real-time Sentiment Analysis Dashboard")
st.markdown("### Analyze sentiment trends for any product or topic dynamically")


# -----------------------
# Session State Initialization
# -----------------------
def initialize_session_state():
    """Initialize all required session state variables."""
    if "data_cache" not in st.session_state:
        st.session_state.data_cache = deque(maxlen=MAX_CACHE)
    
    if "topic" not in st.session_state:
        st.session_state.topic = ""
    
    if "streaming_active" not in st.session_state:
        st.session_state.streaming_active = False
    
    if "last_update_time" not in st.session_state:
        st.session_state.last_update_time = None
    
    if "extraction_thread" not in st.session_state:
        st.session_state.extraction_thread = None
    
    if "producer" not in st.session_state:
        st.session_state.producer = None
    
    if "consumer" not in st.session_state:
        st.session_state.consumer = None


initialize_session_state()


# -----------------------
# Kafka Utilities
# -----------------------
@st.cache_resource
def get_kafka_producer():
    """Get or create Kafka producer."""
    if KafkaProducer is None:
        st.error("kafka-python not installed. Install with: pip install kafka-python")
        return None
    
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
        )
    except Exception as e:
        st.error(f"Failed to connect to Kafka: {e}")
        return None


@st.cache_resource
def get_kafka_consumer():
    """Get or create Kafka consumer."""
    if KafkaConsumer is None:
        st.error("kafka-python not installed. Install with: pip install kafka-python")
        return None
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="sentiment-dashboard",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
            max_poll_records=MAX_FETCH_PER_REFRESH,
        )
        return consumer
    except Exception as e:
        st.error(f"Failed to connect to Kafka: {e}")
        return None


def send_to_kafka(producer, data: Dict):
    """Send a single message to Kafka."""
    if producer is None:
        return False
    
    try:
        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()
        return True
    except Exception as e:
        st.warning(f"Failed to send to Kafka: {e}")
        return False


def consume_from_kafka(consumer, max_records: int = MAX_FETCH_PER_REFRESH) -> List[Dict]:
    """Consume messages from Kafka and perform sentiment analysis."""
    if consumer is None:
        return []
    
    messages = []
    try:
        polled_data = consumer.poll(timeout_ms=500, max_records=max_records)
        for partition_records in polled_data.values():
            for message in partition_records:
                data = message.value
                
                # Perform VADER sentiment analysis
                text = data.get("text", "") or ""
                scores = analyzer.polarity_scores(text)
                
                # Determine sentiment
                compound = scores["compound"]
                if compound >= 0.05:
                    sentiment = "Positive"
                elif compound <= -0.05:
                    sentiment = "Negative"
                else:
                    sentiment = "Neutral"
                
                data["sentiment"] = sentiment
                data["compound_score"] = compound
                data["received_at"] = datetime.now().isoformat()
                
                messages.append(data)
    except Exception as e:
        st.warning(f"Error consuming from Kafka: {e}")
    
    return messages


# -----------------------
# Data Extraction Functions
# -----------------------
def extract_reddit_data(topic: str, producer) -> Tuple[int, int]:
    """Extract Reddit posts and send to Kafka."""
    sent_count = 0
    error_count = 0
    
    try:
        posts = extract_reddit_reviews(topic, limit=50)
        
        for post in posts:
            data = {
                "id": post.get("id"),
                "title": post.get("title", ""),
                "text": post.get("text", ""),
                "score": post.get("score", 0),
                "timestamp": post.get("date", ""),
                "source": "Reddit",
            }
            
            if send_to_kafka(producer, data):
                sent_count += 1
            else:
                error_count += 1
    except Exception as e:
        st.warning(f"Error extracting Reddit data: {e}")
        error_count += 1
    
    return sent_count, error_count


def data_extraction_loop(topic: str, producer, stop_event):
    """Continuously extract data from Reddit."""
    while not stop_event.is_set():
        try:
            reddit_sent, reddit_err = extract_reddit_data(topic, producer)
            
            # Wait before next extraction
            time.sleep(10)
        except Exception as e:
            st.warning(f"Extraction loop error: {e}")
            time.sleep(5)


# -----------------------
# Sentiment Analysis & Processing
# -----------------------
def process_kafka_messages(consumer) -> int:
    """Consume and process messages from Kafka."""
    messages = consume_from_kafka(consumer)
    
    for msg in messages:
        st.session_state.data_cache.append(msg)
    
    st.session_state.last_update_time = datetime.now()
    return len(messages)


def get_sentiment_distribution() -> Dict[str, int]:
    """Calculate sentiment distribution from cached data."""
    distribution = defaultdict(int)
    
    for item in st.session_state.data_cache:
        sentiment = item.get("sentiment", "Unknown")
        distribution[sentiment] += 1
    
    return dict(distribution)


def get_top_reviews(sentiment_type: str, top_n: int = 5) -> pd.DataFrame:
    """Get top N reviews by sentiment and score."""
    filtered = [
        item for item in st.session_state.data_cache
        if item.get("sentiment") == sentiment_type
    ]
    
    if not filtered:
        return pd.DataFrame()
    
    df = pd.DataFrame(filtered)
    df = df.sort_values("score", ascending=(sentiment_type == "Negative"))
    df = df[["text", "source", "score", "sentiment"]].head(top_n)
    
    return df


def get_sentiment_over_time() -> pd.DataFrame:
    """Prepare data for sentiment trend visualization."""
    if not st.session_state.data_cache:
        return pd.DataFrame()
    
    df = pd.DataFrame(st.session_state.data_cache)
    
    if "received_at" in df.columns:
        df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce")
        df = df.sort_values("received_at")
        
        # Group by time and sentiment
        df_grouped = df.groupby([pd.Grouper(key="received_at", freq="1min"), "sentiment"]).size().reset_index(name="count")
        return df_grouped
    
    return pd.DataFrame()


def get_source_distribution() -> Dict[str, int]:
    """Get count of reviews from each source."""
    distribution = defaultdict(int)
    
    for item in st.session_state.data_cache:
        source = item.get("source", "Unknown")
        distribution[source] += 1
    
    return dict(distribution)


# -----------------------
# UI Layout
# -----------------------

# Sidebar Controls
with st.sidebar:
    st.header("⚙️ Controls")
    
    # Topic input
    topic_input = st.text_input(
        "Enter Topic/Product Name:",
        value=st.session_state.topic,
        placeholder="e.g., Pixel 8, iPhone 17, Tesla Model 3",
        help="Search for sentiment data for any product or topic"
    )
    
    if topic_input != st.session_state.topic:
        st.session_state.topic = topic_input
        st.session_state.data_cache.clear()
        st.session_state.last_update_time = None
    
    st.divider()
    
    # Streaming controls
    st.subheader("📡 Streaming")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("▶️ Start Streaming", use_container_width=True):
            if not st.session_state.topic:
                st.error("Please enter a topic first!")
            else:
                st.session_state.streaming_active = True
                st.rerun()
    
    with col2:
        if st.button("⏸️ Stop Streaming", use_container_width=True):
            st.session_state.streaming_active = False
            st.rerun()
    
    refresh_button = st.button(
        "🔄 Refresh Once",
        use_container_width=True,
        help="Manually refresh data from Kafka"
    )
    
    refresh_interval = st.slider(
        "Refresh interval (seconds):",
        min_value=3,
        max_value=30,
        value=5,
        step=1
    )
    
    st.divider()
    
    # Display stats
    st.subheader("📊 Stats")
    st.metric("Cached Items", len(st.session_state.data_cache))
    
    if st.session_state.last_update_time:
        st.caption(f"Last updated: {st.session_state.last_update_time.strftime('%H:%M:%S')}")
    else:
        st.caption("Not updated yet")
    
    st.divider()
    
    st.subheader("ℹ️ Info")
    st.markdown("""
    **How it works:**
    1. Enter a topic name
    2. Click "Start Streaming"
    3. Data fetched from Reddit
    4. Sentiment analyzed with VADER
    5. Results displayed in real-time
    
    **Source:**
    - 🔴 Reddit posts
    """)


# Main Content
if not st.session_state.topic:
    st.info("👈 Enter a topic in the sidebar to get started!")
else:
    # Producer and Consumer setup
    producer = get_kafka_producer()
    consumer = get_kafka_consumer()
    
    # Fetch data if streaming or refresh button clicked
    if st.session_state.streaming_active or refresh_button:
        if producer is None or consumer is None:
            st.error("❌ Kafka connection failed. Make sure Kafka is running on localhost:9092")
        else:
            status_placeholder = st.empty()
            
            with status_placeholder.container():
                st.info(f"🔍 Searching for: **{st.session_state.topic}**")
            
            # Fetch from Reddit only
            reddit_sent, reddit_err = extract_reddit_data(st.session_state.topic, producer)
            
            status_placeholder.empty()
            
            if reddit_sent > 0:
                st.toast(f"✅ Extracted {reddit_sent} items from Reddit")
            
            # Consume and process
            processed = process_kafka_messages(consumer)
    
    # Display results
    if len(st.session_state.data_cache) == 0:
        st.warning("No data yet. Click 'Start Streaming' or 'Refresh Once' to fetch data.")
    else:
        # Key Metrics
        st.subheader("📈 Sentiment Overview")
        
        sentiment_dist = get_sentiment_distribution()
        source_dist = get_source_distribution()
        
        metric_cols = st.columns(4)
        
        with metric_cols[0]:
            total = len(st.session_state.data_cache)
            st.metric("Total Items", total)
        
        with metric_cols[1]:
            positive_count = sentiment_dist.get("Positive", 0)
            positive_pct = (positive_count / total * 100) if total > 0 else 0
            st.metric("😊 Positive", f"{positive_count} ({positive_pct:.1f}%)")
        
        with metric_cols[2]:
            neutral_count = sentiment_dist.get("Neutral", 0)
            neutral_pct = (neutral_count / total * 100) if total > 0 else 0
            st.metric("😐 Neutral", f"{neutral_count} ({neutral_pct:.1f}%)")
        
        with metric_cols[3]:
            negative_count = sentiment_dist.get("Negative", 0)
            negative_pct = (negative_count / total * 100) if total > 0 else 0
            st.metric("😞 Negative", f"{negative_count} ({negative_pct:.1f}%)")
        
        # Visualizations
        st.divider()
        st.subheader("📊 Visualizations")
        
        viz_tabs = st.tabs(["Sentiment Pie", "Sentiment Trend", "Source Distribution", "Top Reviews"])
        
        with viz_tabs[0]:
            # Sentiment pie chart
            if sentiment_dist:
                fig = px.pie(
                    values=list(sentiment_dist.values()),
                    names=list(sentiment_dist.keys()),
                    title=f"Sentiment Distribution for '{st.session_state.topic}'",
                    color_discrete_map={
                        "Positive": "#2ecc71",
                        "Neutral": "#95a5a6",
                        "Negative": "#e74c3c"
                    }
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig, use_container_width=True)
        
        with viz_tabs[1]:
            # Sentiment over time
            df_time = get_sentiment_over_time()
            
            if not df_time.empty:
                fig = px.bar(
                    df_time,
                    x="received_at",
                    y="count",
                    color="sentiment",
                    title="Sentiment Trend Over Time",
                    color_discrete_map={
                        "Positive": "#2ecc71",
                        "Neutral": "#95a5a6",
                        "Negative": "#e74c3c"
                    }
                )
                fig.update_layout(hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough data for trend visualization yet.")
        
        with viz_tabs[2]:
            # Source distribution (Reddit-only)
            if source_dist:
                fig = px.bar(
                    x=list(source_dist.keys()),
                    y=list(source_dist.values()),
                    title="Reviews by Source",
                    labels={"x": "Source", "y": "Count"},
                    color=list(source_dist.keys()),
                    color_discrete_map={
                        "Reddit": "#ff4500",
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with viz_tabs[3]:
            # Top reviews
            review_cols = st.columns(2)
            
            with review_cols[0]:
                st.markdown("### 🟢 Top Positive Reviews")
                top_positive = get_top_reviews("Positive", 5)
                
                if not top_positive.empty:
                    for idx, row in top_positive.iterrows():
                        with st.container(border=True):
                            st.markdown(f"**Source:** {row['source']}")
                            st.write(row['text'][:200] + "..." if len(row['text']) > 200 else row['text'])
                            st.caption(f"Score: {row['score']}")
                else:
                    st.info("No positive reviews yet.")
            
            with review_cols[1]:
                st.markdown("### 🔴 Top Negative Reviews")
                top_negative = get_top_reviews("Negative", 5)
                
                if not top_negative.empty:
                    for idx, row in top_negative.iterrows():
                        with st.container(border=True):
                            st.markdown(f"**Source:** {row['source']}")
                            st.write(row['text'][:200] + "..." if len(row['text']) > 200 else row['text'])
                            st.caption(f"Score: {row['score']}")
                else:
                    st.info("No negative reviews yet.")
        
        # Raw data table
        st.divider()
        st.subheader("📋 Recent Data")
        
        if len(st.session_state.data_cache) > 0:
            df_display = pd.DataFrame(list(st.session_state.data_cache)).iloc[::-1]
            display_cols = ["source", "text", "sentiment", "compound_score", "score"]
            available_cols = [col for col in display_cols if col in df_display.columns]
            
            st.dataframe(
                df_display[available_cols].head(20),
                use_container_width=True,
                height=400
            )


# Auto-refresh
if st.session_state.streaming_active and len(st.session_state.data_cache) > 0:
    time.sleep(refresh_interval)
    st.rerun()