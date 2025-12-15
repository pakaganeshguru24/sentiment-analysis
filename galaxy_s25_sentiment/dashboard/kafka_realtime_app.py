"""Legacy/demo Kafka real-time dashboard.

For the primary real-time experience (multi-source + history view),
use `realtime_dashboard.py`. This app is kept as a simpler demo.
"""

import streamlit as st
import threading
import time
import json
from collections import deque, defaultdict
from datetime import datetime
from typing import Deque

import pandas as pd
import plotly.express as px
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

try:
    from kafka import KafkaProducer, KafkaConsumer
except Exception:
    KafkaProducer = None
    KafkaConsumer = None

# reuse extractor helper
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'extract'))
from extractors import extract_reddit_reviews

# PostgreSQL helper for optional snapshot storage
# Add the project root so we can import `load.postgres_utils` like in tests.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
try:
    from load.postgres_utils import store_topic_snapshot
    POSTGRES_AVAILABLE = True
    POSTGRES_IMPORT_ERROR = None
except Exception as e:
    POSTGRES_AVAILABLE = False
    POSTGRES_IMPORT_ERROR = str(e)

try:
    from wordcloud import WordCloud
    WORDCLOUD_AVAILABLE = True
except Exception:
    WORDCLOUD_AVAILABLE = False


st.set_page_config(page_title="Kafka Real-time Sentiment (Demo)", layout="wide")
st.title("📡 Kafka Real-time Sentiment Stream (Demo)")
st.info("This is a legacy/demo dashboard. For the full multi-source, history-enabled experience, use `realtime_dashboard.py`.")

# Kafka configuration: prefer environment variables, fall back to sensible defaults.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sentiment-stream")

analyzer = SentimentIntensityAnalyzer()


def make_producer(bootstrap_servers: str):
    if KafkaProducer is None:
        return None
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
    )


def make_consumer(bootstrap_servers: str, topic: str):
    if KafkaConsumer is None:
        return None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
            max_poll_records=100,
        )
        return consumer
    except Exception:
        return None


def producer_loop(topic_name: str, stop_event: threading.Event, producer, kafka_topic: str):
    """Continuously fetch Reddit posts for topic_name and send to Kafka."""
    while not stop_event.is_set():
        try:
            posts = extract_reddit_reviews(topic_name, limit=50)
            for post in posts:
                data = {
                    "id": post.get("id"),
                    "title": post.get("title", ""),
                    "text": post.get("text", ""),
                    "score": post.get("score", 0),
                    "timestamp": post.get("date", datetime.utcnow().isoformat()),
                    "source": "Reddit",
                    "topic": topic_name,
                }
                try:
                    if producer:
                        producer.send(kafka_topic, value=data)
                except Exception:
                    # ignore send errors; consumer will handle missing data
                    pass

        except Exception:
            # swallow exceptions to keep thread alive
            pass

        # flush once per cycle if producer exists
        try:
            if producer:
                producer.flush()
        except Exception:
            pass

        # wait a bit before next fetch
        for _ in range(10):
            if stop_event.is_set():
                break
            time.sleep(1)


def consumer_loop(stop_event: threading.Event, consumer, data_cache: Deque, cache_lock: threading.Lock):
    """Consume messages from Kafka and append processed sentiment results to data_cache."""
    while not stop_event.is_set():
        try:
            # Poll records
            if consumer is None:
                time.sleep(1)
                continue

            polled = consumer.poll(timeout_ms=500, max_records=100)
            for records in polled.values():
                for msg in records:
                    data = msg.value
                    text = (data.get("text") or data.get("title") or "")
                    scores = analyzer.polarity_scores(str(text))
                    compound = scores.get("compound", 0.0)
                    if compound >= 0.05:
                        sentiment = "Positive"
                    elif compound <= -0.05:
                        sentiment = "Negative"
                    else:
                        sentiment = "Neutral"

                    entry = {
                        "source": data.get("source", "Unknown"),
                        "text": text,
                        "sentiment": sentiment,
                        "compound_score": compound,
                        "timestamp": data.get("timestamp") or datetime.utcnow().isoformat(),
                    }

                    with cache_lock:
                        data_cache.append(entry)

        except Exception:
            # keep consumer alive
            time.sleep(1)


def build_wordcloud(texts):
    if not WORDCLOUD_AVAILABLE or not texts:
        return None
    joined = " ".join(texts)
    wc = WordCloud(width=800, height=400, background_color="white").generate(joined)
    return wc


def initialize_session():
    if "data_cache" not in st.session_state:
        st.session_state.data_cache = deque(maxlen=2000)
    if "cache_lock" not in st.session_state:
        st.session_state.cache_lock = threading.Lock()
    if "producer_thread" not in st.session_state:
        st.session_state.producer_thread = None
    if "consumer_thread" not in st.session_state:
        st.session_state.consumer_thread = None
    if "producer_stop" not in st.session_state:
        st.session_state.producer_stop = None
    if "consumer_stop" not in st.session_state:
        st.session_state.consumer_stop = None
    if "producer" not in st.session_state:
        st.session_state.producer = None
    if "consumer" not in st.session_state:
        st.session_state.consumer = None
    if "streaming_active" not in st.session_state:
        st.session_state.streaming_active = False
    if "current_topic" not in st.session_state:
        st.session_state.current_topic = ""
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = True


initialize_session()


with st.sidebar:
    st.header("Controls")
    topic = st.text_input("Enter topic name", value=st.session_state.current_topic)
    start_btn = st.button("Start Stream")
    stop_btn = st.button("Stop Stream")
    refresh_interval = st.slider("Refresh interval (sec)", min_value=2, max_value=30, value=5)
    st.session_state.auto_refresh = st.checkbox(
        "Auto-refresh while streaming",
        value=st.session_state.auto_refresh,
        help="Uncheck to pause automatic dashboard refresh.",
    )


def stop_stream():
    # Signal stop events and join threads
    if st.session_state.producer_stop is not None:
        st.session_state.producer_stop.set()
    if st.session_state.consumer_stop is not None:
        st.session_state.consumer_stop.set()

    # join threads if alive
    try:
        if st.session_state.producer_thread is not None and st.session_state.producer_thread.is_alive():
            st.session_state.producer_thread.join(timeout=2)
    except Exception:
        pass

    try:
        if st.session_state.consumer_thread is not None and st.session_state.consumer_thread.is_alive():
            st.session_state.consumer_thread.join(timeout=2)
    except Exception:
        pass

    # close kafka connections
    try:
        if st.session_state.producer is not None:
            st.session_state.producer.close()
    except Exception:
        pass

    try:
        if st.session_state.consumer is not None:
            st.session_state.consumer.close()
    except Exception:
        pass

    st.session_state.producer_thread = None
    st.session_state.consumer_thread = None
    st.session_state.producer_stop = None
    st.session_state.consumer_stop = None
    st.session_state.producer = None
    st.session_state.consumer = None
    st.session_state.streaming_active = False
    st.session_state.current_topic = ""


if stop_btn:
    stop_stream()
    st.success("Streaming stopped")


if start_btn:
    if not topic or not topic.strip():
        st.error("Please enter a topic")
    else:
        # If already streaming a different topic, stop it first
        if st.session_state.streaming_active and st.session_state.current_topic != topic:
            stop_stream()

        st.session_state.current_topic = topic
        st.session_state.streaming_active = True

        # create kafka producer & consumer
        producer = make_producer(KAFKA_BOOTSTRAP)
        consumer = make_consumer(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
        st.session_state.producer = producer
        st.session_state.consumer = consumer

        # create stop events
        prod_stop = threading.Event()
        cons_stop = threading.Event()
        st.session_state.producer_stop = prod_stop
        st.session_state.consumer_stop = cons_stop

        # start producer thread
        p_thread = threading.Thread(target=producer_loop, args=(topic, prod_stop, producer, KAFKA_TOPIC), daemon=True)
        c_thread = threading.Thread(target=consumer_loop, args=(cons_stop, consumer, st.session_state.data_cache, st.session_state.cache_lock), daemon=True)

        st.session_state.producer_thread = p_thread
        st.session_state.consumer_thread = c_thread

        p_thread.start()
        c_thread.start()

        st.success(f"Started streaming for topic: {topic}")


# Display live metrics and charts
st.header("Live sentiment")
cols = st.columns(3)
with st.container():
    with st.session_state.cache_lock:
        df = pd.DataFrame(list(st.session_state.data_cache)) if st.session_state.data_cache else pd.DataFrame()

    if df.empty:
        st.info("No messages yet. Start the stream to begin consuming live messages.")
    else:
        total = len(df)
        pos = (df['sentiment'] == 'Positive').sum()
        neu = (df['sentiment'] == 'Neutral').sum()
        neg = (df['sentiment'] == 'Negative').sum()

        c1, c2, c3 = st.columns(3)
        c1.metric("Total", total)
        c2.metric("Positive", f"{pos} ({pos/total*100:.1f}% )")
        c3.metric("Negative", f"{neg} ({neg/total*100:.1f}% )")

        # Optional manual snapshot to PostgreSQL for later analysis
        if POSTGRES_AVAILABLE:
            if st.button("💾 Save this snapshot to database", use_container_width=False):
                try:
                    # Use the full in-memory cache so the snapshot reflects
                    # everything seen so far for this topic.
                    with st.session_state.cache_lock:
                        messages = list(st.session_state.data_cache)

                    topic_name = st.session_state.current_topic or "unknown-topic"
                    summary_id = store_topic_snapshot(topic_name, messages)
                    if summary_id:
                        st.success(f"Snapshot saved for topic '{topic_name}' (summary id: {summary_id}).")
                    else:
                        st.info("Nothing to save yet (no aggregated messages).")
                except Exception as e:
                    st.error(f"Failed to save snapshot: {e}")
        else:
            msg = "PostgreSQL integration not available; install/configure it to enable saving snapshots."
            if POSTGRES_IMPORT_ERROR:
                msg += f" (Import error: {POSTGRES_IMPORT_ERROR})"
            st.caption(msg)

        # Pie chart
        counts = df['sentiment'].value_counts().reindex(['Positive','Neutral','Negative']).fillna(0)
        fig_pie = px.pie(values=counts.values, names=counts.index, title="Sentiment Distribution")
        st.plotly_chart(fig_pie, use_container_width=True)

        # Recent posts
        st.markdown("### Recent posts")
        st.dataframe(df[['timestamp','source','sentiment','compound_score','text']].head(50), use_container_width=True)

        # Word cloud
        st.markdown("### Word Cloud")
        texts = df['text'].astype(str).tolist()
        if WORDCLOUD_AVAILABLE and texts:
            wc = build_wordcloud(texts)
            if wc is not None:
                import matplotlib.pyplot as plt
                fig, ax = plt.subplots(figsize=(10,4))
                ax.imshow(wc, interpolation='bilinear')
                ax.axis('off')
                st.pyplot(fig)
        else:
            st.info("Install `wordcloud` to see a word cloud, otherwise top terms will appear below.")
            # top terms fallback
            from collections import Counter
            import re
            tokens = []
            for t in texts:
                words = re.findall(r"\w{3,}", t.lower())
                tokens.extend([w for w in words if not w.isnumeric()])
            top = Counter(tokens).most_common(30)
            if top:
                top_df = pd.DataFrame(top, columns=['term','count'])
                fig_terms = px.bar(top_df, x='term', y='count', title='Top terms')
                st.plotly_chart(fig_terms, use_container_width=True)


# Auto-refresh while streaming
if st.session_state.streaming_active and st.session_state.auto_refresh:
    time.sleep(refresh_interval)
    # Streamlit >= 1.32 uses st.rerun; keep a tiny fallback for older versions.
    try:
        st.rerun()
    except AttributeError:  # pragma: no cover
        st.experimental_rerun()
