# Real-time Sentiment Analysis Dashboard - Setup & Usage Guide

## 📋 Overview

This real-time sentiment analysis dashboard allows you to:
- ✅ Enter any product/topic name dynamically
- ✅ Fetch live data from Reddit and Google Play
- ✅ Analyze sentiment using VADER (optimized for social media)
- ✅ Send data through Kafka for streaming
- ✅ Display interactive visualizations with Plotly
- ✅ Store recent data in memory
- ✅ Pause/resume streaming on demand

## 🚀 Quick Start

### 1. Prerequisites

- **Python 3.13.5** (as per repo specification)
- **Kafka Broker** running on `localhost:9092` (or configure `KAFKA_BOOTSTRAP`)
- **Reddit API Credentials** (optional but recommended for Reddit extraction)

### 2. Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
# At minimum, update:
# - REDDIT_CLIENT_ID
# - REDDIT_CLIENT_SECRET
# - REDDIT_USER_AGENT
```

#### Getting Reddit Credentials:
1. Go to https://www.reddit.com/prefs/apps
2. Click "Create Application"
3. Fill in the form with app details
4. Copy `client_id` and `client_secret`
5. Add to `.env`

### 3. Install Dependencies

```bash
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Install requirements
pip install -r galaxy_s25_sentiment/requirements.txt

# Verify vaderSentiment is installed
python -c "from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer; print('✅ VADER installed')"
```

### 4. Start Kafka (if not running)

```bash
# In a separate terminal, start Kafka
# Example using Docker:
docker-compose up -d

# Or if you have Kafka installed locally:
# zkServer.cmd  # Start Zookeeper
# kafka-server-start.bat config\server.properties  # Start Kafka
```

### 5. Run the Dashboard

```bash
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Navigate to project root
cd d:\Sentiment-Analysis

# Start Streamlit app
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
```

The dashboard will open at `http://localhost:8501`

## 📊 Using the Dashboard

### Step 1: Enter a Topic
1. In the left sidebar, type a product/topic name:
     - Examples: "example-topic", "iPhone 17", "Tesla Model 3", "iPad Pro"

### Step 2: Start Streaming
1. Click **"▶️ Start Streaming"** button
2. The app will:
   - Fetch Reddit posts matching your query
   - Fetch Google Play reviews (if app exists)
   - Send data to Kafka
   - Consume and analyze with VADER sentiment

### Step 3: Monitor Metrics
The dashboard displays:
- **Total Items**: Number of reviews/posts analyzed
- **Sentiment Breakdown**: % of Positive, Neutral, Negative
- **Source Distribution**: Reddit vs Google Play split

### Step 4: Explore Visualizations

#### 📈 Sentiment Pie Chart
- Visual breakdown of sentiment distribution
- Color coded: Green (Positive), Gray (Neutral), Red (Negative)

#### 📊 Sentiment Trend
- Time-series view of sentiment changes
- Useful for spotting trends

#### 🔄 Source Distribution
- How many items from each source
- Reddit: #ff4500
- Google Play: #4285f4

#### 🏆 Top Reviews
- **Top Positive Reviews**: Highest scored positive comments
- **Top Negative Reviews**: Lowest scored negative comments
- Shows source and review text

### Step 5: Control Streaming

**Pause Streaming**: Click **"⏸️ Stop Streaming"**

**Refresh Once**: Manually fetch latest data without auto-refresh

**Auto-Refresh**: Adjust interval from 3-30 seconds (default: 5s)

## 🔧 Architecture

```
┌─────────────────────────────────────────────────┐
│     Streamlit Dashboard (realtime_dashboard.py) │
└────────────┬────────────────────────────────────┘
             │
             ├─────────────────────────┬──────────────────────────┐
             │                         │                          │
        ┌────▼────┐            ┌──────▼──────┐         ┌─────────▼────┐
        │ Reddit  │            │ Google Play │         │  Sentiment   │
        │ Extract │            │   Extract   │         │  Analysis    │
        │ (PRAW)  │            │ (g-play-scr)│         │   (VADER)    │
        └────┬────┘            └──────┬──────┘         └──────┬──────┘
             │                         │                       │
             └─────────────────┬───────┘                       │
                               │                               │
                          ┌────▼──────────────────────────────▼───┐
                          │        Kafka Topic (JSON)              │
                          │     sentiment-stream                   │
                          └────┬──────────────────────────────────┘
                               │
                          ┌────▼──────────────────────┐
                          │  Kafka Consumer           │
                          │  (In-Memory Storage)      │
                          │  (Max 500 items)          │
                          └────┬─────────────────────┘
                               │
                        ┌──────▼──────────────┐
                        │ Display & Visualize │
                        │ (Plotly Charts)     │
                        └────────────────────┘
```

## 📁 File Structure

```
galaxy_s25_sentiment/
├── dashboard/
│   ├── app.py                    # Original dashboard
│   └── realtime_dashboard.py    # ✨ NEW - Real-time sentiment dashboard
├── extract/
│   ├── reddit_extract.py        # Reddit data extraction
│   ├── google_extract.py        # Google Play data extraction
│   ├── extractors.py            # ✨ NEW - Unified extraction interface
│   ├── kafka_utils.py           # ✨ NEW - Kafka utilities
│   └── multi_source_extract.py  # Multi-source Kafka producer
├── sentiment/
│   └── sentiment_analysis.py    # Sentiment analysis (VADER)
└── requirements.txt             # ✨ UPDATED - All dependencies
```

## 🎯 Key Features Implemented

### ✅ Dynamic Topic Input
- No hardcoded topic names
- User can search for any product/topic

### ✅ Multi-Source Extraction
- Reddit (using PRAW)
- Google Play (using google-play-scraper)
- Source tags displayed in results

### ✅ Real-time Sentiment Analysis
- VADER sentiment analyzer (optimized for social media)
- Compound score for each review
- Sentiment classification: Positive/Negative/Neutral

### ✅ Kafka Integration
- KafkaProducer: Send extracted data
- KafkaConsumer: Receive and process in real-time
- Topic: `sentiment-stream`

### ✅ Interactive Visualizations
- Pie chart: Sentiment distribution
- Bar chart: Sentiment trends over time
- Bar chart: Source distribution
- Top 5 positive/negative reviews
- Raw data table with latest items

### ✅ In-Memory Storage
- Max 500 items cached
- FIFO eviction (oldest removed when limit reached)
- Fast access and refresh

### ✅ Pause/Resume Streaming
- Toggle streaming on/off
- Manual refresh button
- Configurable refresh interval

## 🔍 Troubleshooting

### "Kafka connection failed"
```
Error: Kafka connection failed
Solution:
- Ensure Kafka is running: docker-compose up -d
- Check KAFKA_BOOTSTRAP in .env (default: localhost:9092)
- Check firewall isn't blocking 9092
```

### "Reddit authentication failed"
```
Error: Error collecting Reddit posts
Solution:
- Verify Reddit credentials in .env
- Go to https://www.reddit.com/prefs/apps
- Create a new "script" application
- Update CLIENT_ID, CLIENT_SECRET, USER_AGENT
```

### "No data received"
```
Possible causes:
1. Query doesn't match any Reddit posts
2. App not available on Google Play
3. Kafka topic not configured
Try:
- Different topic/product name
- Check logs for extraction errors
- Manually verify Reddit has posts with your query
```

### "Dashboard runs but no visualizations"
```
Solution:
- Click "Refresh Once" to manually fetch data
- Check Streamlit logs for errors
- Verify all dependencies installed: pip install -r requirements.txt
```

## 📊 Example Workflows

### Workflow 1: Analyze New Product Launch
```
1. Enter: "example-topic"
2. Click "Start Streaming"
3. Wait 10-15 seconds for data collection
4. View sentiment distribution pie chart
5. Check top positive/negative reviews
6. Monitor trends as more data arrives
```

### Workflow 2: Compare Products
```
1. Analyze "example-topic" - Note sentiment
2. Clear topic, enter "iPhone 17"
3. Compare sentiment distributions
4. Identify strengths/weaknesses
```

### Workflow 3: Monitor Sentiment Trend
```
1. Enter topic name
2. Start streaming
3. Let it run for 30+ seconds
4. Switch to "Sentiment Trend" tab
5. Watch how sentiment changes over time
6. Identify improvement/decline patterns
```

## 🛠️ Configuration

### Adjust Cache Size
In `realtime_dashboard.py`, modify:
```python
MAX_CACHE = 500  # Change this value
MAX_FETCH_PER_REFRESH = 100  # Max items per refresh
```

### Change Refresh Interval
Default is 5 seconds, adjustable in sidebar (3-30 seconds)

### Add More Data Sources
Edit `extractors.py` to add new sources:
```python
def extract_custom_source(query):
    # Your extraction logic
    return list_of_items
```

## 📈 Performance Tips

1. **Reduce Refresh Interval** for faster updates (may increase CPU)
2. **Increase Cache Size** to retain more history
3. **Filter by Source** to reduce processing
4. **Use Popular Products** - more Reddit/Google Play data available

## 🔐 Security Notes

- **Never commit `.env` file** with credentials to git
- Reddit credentials are stored locally only
- Kafka communication is unencrypted by default
- Consider enabling Kafka SSL/TLS for production

## 📚 Additional Resources

- [VADER Sentiment Analysis](https://github.com/cjhutto/vaderSentiment)
- [PRAW (Reddit API)](https://praw.readthedocs.io/)
- [google-play-scraper](https://github.com/JoMingyu/google-play-scraper)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)
- [Streamlit Docs](https://docs.streamlit.io/)

## 📞 Support

For issues:
1. Check the troubleshooting section above
2. Review logs in terminal/IDE console
3. Verify `.env` configuration
4. Ensure all dependencies are installed
5. Check that Kafka is running

---

**Happy Analyzing! 🚀**