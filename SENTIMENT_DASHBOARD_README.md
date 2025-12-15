# 🎯 Real-time Sentiment Analysis Dashboard

## 📊 Overview

A **complete real-time sentiment analysis system** that allows you to analyze sentiment for any product or topic by fetching live data from Reddit and Google Play, streaming through Kafka, and visualizing with interactive Plotly charts.

### Key Features
✨ **Dynamic Topic Input** - Search any product/topic without hardcoding
🔄 **Real-time Streaming** - Live data via Kafka
🌍 **Multi-Source** - Reddit posts + Google Play reviews
🧠 **Smart Analysis** - VADER sentiment analyzer (optimized for social media)
📈 **Beautiful Visualizations** - Interactive Plotly charts
⏸️ **Pause/Resume** - Control data streaming on demand
💾 **Memory Efficient** - 500-item in-memory cache

## 🚀 Quick Start (5 minutes)

### 1. Verify Setup
```bash
python verify_realtime_setup.py
```

### 2. Start Kafka
```bash
docker-compose up -d
```

### 3. Launch Dashboard
```bash
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
```

### 4. Use Dashboard
- Enter any topic (e.g., "Pixel 8", "iPhone 17")
- Click: "▶️ Start Streaming"
- Watch: Real-time sentiment analysis

## 📚 Documentation Guide

Choose the right document for your needs:

### 🎯 **Stuck? Start Here**
→ **PRE_LAUNCH_CHECKLIST.md**
- Complete setup verification
- Step-by-step configuration
- Troubleshooting guide
- Pre-launch tests

### ⚡ **Just Want to Run It**
→ **QUICKSTART.md**
- 5-minute setup guide
- Dashboard walkthrough
- Example workflows
- Pro tips & tricks

### 📖 **Need Deep Understanding**
→ **REALTIME_DASHBOARD_GUIDE.md**
- Complete architecture
- All features explained
- Configuration options
- Advanced workflows

### 🔍 **Want to See It Work First**
→ **demo_sentiment_analysis.py**
```bash
python demo_sentiment_analysis.py
```
- Interactive demonstrations
- Programmatic usage examples
- See all features in action

### 📋 **Implementation Details**
→ **IMPLEMENTATION_SUMMARY.md**
- What was built
- File structure
- Requirements met
- Performance characteristics

## 📁 Project Structure

```
d:\Sentiment-Analysis\
├── galaxy_s25_sentiment/
│   ├── dashboard/
│   │   ├── app.py                    # Original dashboard
│   │   └── realtime_dashboard.py     # ✨ NEW - Real-time dashboard
│   ├── extract/
│   │   ├── reddit_extract.py         # Reddit API
│   │   ├── google_extract.py         # Google Play scraper
│   │   ├── extractors.py             # ✨ NEW - Unified interface
│   │   ├── kafka_utils.py            # ✨ NEW - Kafka helpers
│   │   └── multi_source_extract.py   # Multi-source producer
│   ├── sentiment/
│   │   └── sentiment_analysis.py     # VADER analyzer
│   └── requirements.txt              # ✨ UPDATED - Dependencies
├── verify_realtime_setup.py          # ✨ NEW - Verification
├── demo_sentiment_analysis.py        # ✨ NEW - Interactive demo
├── .env.example                      # ✨ NEW - Config template
├── QUICKSTART.md                     # ✨ NEW - Quick start
├── REALTIME_DASHBOARD_GUIDE.md       # ✨ NEW - Full guide
├── IMPLEMENTATION_SUMMARY.md         # ✨ NEW - Summary
└── PRE_LAUNCH_CHECKLIST.md          # ✨ NEW - Checklist
```

## 🎬 Dashboard Features

### Sidebar Controls
```
📍 Topic Input        → Enter any product name
▶️  Start Streaming   → Begin data collection
⏸️  Stop Streaming    → Pause collection
🔄 Refresh Once      → Manual refresh
⏱️ Refresh Interval   → Set update speed (3-30s)
📊 Stats             → See cache size & last update
```

### Main Display (4 Tabs)
```
1️⃣  Sentiment Pie    → Overall sentiment distribution
2️⃣  Sentiment Trend  → How sentiment changes over time
3️⃣  Source Dist      → Reddit vs Google Play comparison
4️⃣  Top Reviews      → Best positive/negative comments
```

### Metrics Panel
```
Total Items  │  😊 Positive %  │  😐 Neutral %  │  😞 Negative %
```

## 💻 System Requirements

- Python 3.10+ ✅ (3.13.5 recommended)
- Docker (for Kafka) ✅
- 2GB+ RAM ✅
- Internet connection ✅

## 🔧 Installation

### Step 1: Activate Virtual Environment
```powershell
.\venv\Scripts\Activate.ps1
```

### Step 2: Install Dependencies
```powershell
pip install -r galaxy_s25_sentiment/requirements.txt
```

### Step 3: Configure Environment
```powershell
cp .env.example .env
# Edit .env with your Reddit credentials (optional)
```

### Step 4: Verify Setup
```powershell
python verify_realtime_setup.py
```

## ▶️ Running the Dashboard

### Method 1: Direct Command
```powershell
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
```

### Method 2: Using Demo First
```powershell
# See it in action
python demo_sentiment_analysis.py

# Then launch dashboard
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
```

## 📊 How It Works

```
User Input: "<your-topic>"
           ↓
    Extract Data
    ├─ Reddit (PRAW)
    └─ Google Play (Scraper)
           ↓
    Send to Kafka
    ├─ JSON messages
    └─ sentiment-stream topic
           ↓
    Consume & Analyze
    ├─ Read from Kafka
    ├─ Run VADER sentiment
    └─ Cache in memory (max 500)
           ↓
    Display Results
    ├─ Sentiment metrics
    ├─ Plotly visualizations
    └─ Top reviews + trends
```

## 🎯 Example Workflows

### Workflow 1: Analyze New Product
```
1. Enter any topic (e.g., "Pixel 8")
2. Click: "Start Streaming"
3. Wait: 15 seconds
4. View: Sentiment distribution
5. Check: Top positive/negative reviews
```

### Workflow 2: Compare Products
```
1. Run: "example-topic" → Note sentiment
2. Clear & Run: "iPhone 17" → Compare
3. Compare: Sentiment percentages
4. Identify: Strengths/weaknesses
```

### Workflow 3: Monitor Trends
```
1. Start: "Tesla Model 3"
2. Wait: 2-3 minutes
3. Watch: Sentiment Trend tab
4. Observe: How sentiment changes
```

## ⚙️ Configuration

### Kafka Settings (.env)
```
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=sentiment-stream
```

### Reddit Credentials (.env)
```
REDDIT_CLIENT_ID=<from reddit.com/prefs/apps>
REDDIT_CLIENT_SECRET=<from reddit.com/prefs/apps>
REDDIT_USER_AGENT=YourApp/1.0
```

### Dashboard Settings (realtime_dashboard.py)
```python
MAX_CACHE = 500              # Max items to cache
MAX_FETCH_PER_REFRESH = 100  # Max to fetch per update
```

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| "Kafka connection failed" | Run: `docker-compose up -d` |
| "No Reddit data" | Check .env credentials at reddit.com/prefs/apps |
| "No Google Play reviews" | Try different topic (e.g., "Samsung") |
| "Dashboard won't load" | Check Streamlit: `streamlit --version` |
| "ModuleNotFoundError" | Run: `pip install -r galaxy_s25_sentiment/requirements.txt` |

More solutions in: **PRE_LAUNCH_CHECKLIST.md**

## 🔐 Security Notes

- `.env` file contains credentials - **never commit to git**
- Reddit credentials stored locally only
- No sensitive data stored (public reviews only)
- Kafka unencrypted by default (fine for local dev)

## 📈 Performance

| Metric | Value |
|--------|-------|
| Reddit Extraction | 5-15 seconds |
| Google Play Extraction | 5-10 seconds |
| VADER Analysis | <100ms per item |
| Dashboard Refresh | 1-2 seconds |
| Memory Usage | 200-300 MB |

## 🎓 Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| UI | Streamlit | Dashboard interface |
| Visualization | Plotly | Interactive charts |
| Data Processing | Pandas/NumPy | Data manipulation |
| Sentiment | VADER | Sentiment analysis |
| Reddit | PRAW | Post collection |
| Google Play | google-play-scraper | Review scraping |
| Streaming | Kafka | Real-time pipeline |
| Language | Python 3.13 | All code |

## 🔄 Data Pipeline

```
┌──────────────┐
│ Reddit (PRAW)│         ┌────────────┐
└──────┬───────┘         │   Kafka    │      ┌──────────┐
       │                 │sentiment-  │      │Streamlit │
┌──────▼────────────┐    │  stream    │      │Dashboard │
│Google Play Scraper├───▶│            │◀─────┤          │
└──────────────────┘    └────────────┘      └──────────┘
                              ▲
                              │
                       ┌──────┴──────┐
                       │VADER Analyzer│
                       │Memory Cache  │
                       └──────────────┘
```

## ✨ What Makes This Special

✅ **Fully Dynamic** - No hardcoded values
✅ **Multi-Source** - Reddit + Google Play integrated
✅ **Real-Time** - Kafka-powered streaming
✅ **User-Friendly** - Pause/resume, manual refresh
✅ **Well-Documented** - 5 comprehensive guides
✅ **Production-Ready** - Error handling, verification, testing
✅ **Scalable** - Easy to add more sources
✅ **Beautiful** - Interactive Plotly visualizations

## 📞 Getting Help

1. **First Time?** → Read **QUICKSTART.md**
2. **Setup Issues?** → Run **verify_realtime_setup.py**
3. **Need to Configure?** → Follow **PRE_LAUNCH_CHECKLIST.md**
4. **Want Full Guide?** → Read **REALTIME_DASHBOARD_GUIDE.md**
5. **See It Work First?** → Run **demo_sentiment_analysis.py**
6. **Technical Details?** → See **IMPLEMENTATION_SUMMARY.md**

## 🚀 Getting Started Now

### 30-Second Quick Start
```bash
# 1. Check setup
python verify_realtime_setup.py

# 2. Start Kafka
docker-compose up -d

# 3. Run dashboard
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py

# 4. Open browser to http://localhost:8501
# 5. Enter a topic (e.g., "Pixel 8")
# 6. Click "Start Streaming"
# 7. Watch sentiment analysis in real-time!
```

### Need More Guidance?
→ See **QUICKSTART.md** (5-minute detailed guide)

### Having Issues?
→ See **PRE_LAUNCH_CHECKLIST.md** (step-by-step verification)

## 📝 Requirements Fulfilled

✅ Dynamic product/topic input
✅ Multi-source extraction (Reddit + Google Play)
✅ Kafka integration (producer + consumer)
✅ VADER sentiment analysis
✅ Real-time visualizations (Plotly)
✅ Sentiment distribution metrics
✅ Top 5 positive/negative reviews
✅ Interactive charts (pie, bar, line)
✅ In-memory storage (500 items)
✅ Source tags displayed
✅ Pause/resume streaming
✅ User-controlled refresh
✅ Auto-refresh capability

## 🎉 You're All Set!

Everything you need is ready:
- ✅ Extraction modules
- ✅ Kafka utilities
- ✅ Sentiment analyzer
- ✅ Streamlit dashboard
- ✅ Documentation
- ✅ Verification scripts
- ✅ Demo scripts

**Ready to analyze sentiment? Let's go!**

```bash
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
```

---

**Happy Analyzing! 🚀📊✨**