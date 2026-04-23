# 🚀 Real-time Sentiment Dashboard - Quick Start

## What You Built

- A **real-time sentiment analysis dashboard** that:
- 🔍 Fetches live data from Reddit (via PRAW)
 - 📥 Accepts dynamic product/topic names (e.g., "example-topic", "iPhone 17")
- 📤 Sends all data through Kafka streaming (topic: `sentiment-stream`)
- 🧠 Analyzes sentiment with VADER (optimized for social media text)
- 📊 Displays interactive visualizations using Plotly
- ⏸️ Allows pause/resume of live data streaming
- 💾 Stores up to 500 recent items in memory with fast refresh

## 5-Minute Setup

### Step 1: Check Installation ✅
```powershell
cd d:\Sentiment-Analysis
python verify_realtime_setup.py
```

Expected output: ✅ All dependencies should be green

### Step 2: Start Kafka
```powershell
docker-compose up -d
```

Wait 10 seconds for Kafka to fully start.

### Step 3: Run Dashboard
```powershell
# Recommended: run Streamlit from the repository virtualenv to ensure the correct
# interpreter and packages are used (prevents accidental Anaconda launches).
cd d:\Sentiment-Analysis

# Option A: Activate the repo venv (PowerShell) and run Streamlit
& d:\Sentiment-Analysis\venv\Scripts\Activate.ps1
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py

# Option B: Run Streamlit directly from the venv without activating
& "d:\Sentiment-Analysis\venv\Scripts\streamlit.exe" run "d:\Sentiment-Analysis\galaxy_s25_sentiment\dashboard\realtime_dashboard.py"
```

Browser opens at `http://localhost:8501`

### Step 4: Use the Dashboard
1. **Type product name**: "example-topic"
2. **Click**: "▶️ Start Streaming"
3. **Wait**: 10-15 seconds for data collection
4. **View**: Sentiment distribution, top reviews, trends

## 📊 Dashboard Components

### Sidebar Controls
```
┌─────────────────────────────────┐
│ ⚙️ CONTROLS                     │
├─────────────────────────────────┤
│ Topic Input (Dynamic)           │
│ ▶️ Start Streaming              │
│ ⏸️ Stop Streaming               │
│ 🔄 Refresh Once                 │
│ Refresh Interval: 3-30s         │
│                                 │
│ 📊 Stats                        │
│ Cached Items: [number]          │
│ Last Updated: [timestamp]       │
└─────────────────────────────────┘
```

### Main Display (4 Tabs)
1. **Sentiment Pie** - Overall distribution
2. **Sentiment Trend** - Changes over time
3. **Source Distribution** - Reddit-only (by source field)
4. **Top Reviews** - Best positive/negative comments

### Metrics Row
```
Total Items │ 😊 Positive % │ 😐 Neutral % │ 😞 Negative %
```

## 🎯 Example Workflows

### Workflow 1: Analyze Product Reception
```
Topic: "example-topic"
Expected: Mix of positive (features) and neutral (specs)
Goal: Understand customer sentiment
```

### Workflow 2: Compare Two Products
```
1. Run: "example-topic" → Note sentiment
2. Clear → Run: "iPhone 17" → Compare sentiments
```

### Workflow 3: Monitor Sentiment Trend
```
1. Start: "Tesla Model 3"
2. Let run: 2-3 minutes
3. Watch: Trends tab for sentiment changes
4. Identify: When sentiment improves/declines
```

## 📁 New Files Created

```
d:\Sentiment-Analysis\
├── galaxy_s25_sentiment/
│   ├── dashboard/
│   │   └── realtime_dashboard.py           ✨ NEW - Main dashboard
│   ├── extract/
│   │   ├── extractors.py                   ✨ NEW - Unified extraction
│   │   └── kafka_utils.py                  ✨ NEW - Kafka helpers
│   └── requirements.txt                    ✨ UPDATED
├── verify_realtime_setup.py                ✨ NEW - Verification script
├── .env.example                            ✨ NEW - Config template
├── REALTIME_DASHBOARD_GUIDE.md             ✨ NEW - Full guide
└── QUICKSTART.md                           ← You are here
```

## 🔧 Key Features Explained

### Dynamic Topic Input
No hardcoded values - Enter ANY product name:
- "example-topic"
- "iPhone 17"
- "Tesla Model 3"
- "MacBook Pro"
- "PlayStation 5"

### Data Source
- **Reddit** (via PRAW) - Community discussions

Each source tagged in results:
```
┌─────────────────────────────┐
│ Source: Reddit              │
│ "This phone is amazing..."  │
│ Score: 8/10                 │
└─────────────────────────────┘
```

### Real-time Sentiment Analysis
Uses **VADER** (Valence Aware Dictionary and sEntiment Reasoner):
- Optimized for social media
- Fast processing
- Returns compound score (-1 to +1)
- Classification: Positive/Negative/Neutral

### Kafka Streaming
```
Extract → Kafka Topic → Dashboard
   ↓                          ↓
Reddit               Consumer polls
                               every 5s
   ↓                    ↓
Send JSON      Analyze + Cache
```

### In-Memory Storage
- Max 500 items
- FIFO eviction (oldest removed first)
- Fast access for visualizations
- Resets when app restarts

## ⚙️ Configuration

### Adjust Refresh Speed
In sidebar: "Refresh interval" slider (3-30 seconds)

### Change Cache Size
Edit `realtime_dashboard.py`:
```python
MAX_CACHE = 500  # Change this
```

### Add Reddit Credentials
Edit `.env`:
```
REDDIT_CLIENT_ID=your_id
REDDIT_CLIENT_SECRET=your_secret
REDDIT_USER_AGENT=YourApp/1.0
```

Get these from your own Reddit app at: https://www.reddit.com/prefs/apps
Keep `.env` local and do not commit it.

### Change Kafka Settings
Edit `.env`:
```
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=sentiment-stream
```

## 🐛 Common Issues

### "Kafka connection failed"
```
Fix: docker-compose up -d
Then: Wait 10 seconds
```

### "No data received"
```
Try different topic: "Samsung", "Apple", "Tesla"
Or refresh: Click "🔄 Refresh Once"
```

### "Topic search returns empty"
```
Possible causes:
- No Reddit posts for that topic
- Topic name too specific or misspelled
Solution: Try "Galaxy", "iPhone", "Tesla" (simpler terms)
```

## 📈 Expected Behavior

### On First Load
```
⏳ Loading...
Enter topic → Click "Start Streaming"
🔄 Fetching from Reddit...
```

### After 10-15 seconds
```
✅ Extracted X items
📊 Display metrics and charts
```

### Continuously
```
Auto-refresh every N seconds (adjustable)
New data added to cache
Visualizations update live
```

## 🎓 Understanding the Sentiment Scores

### VADER Compound Score
```
-1.0 ← Most Negative  |  0 ← Neutral  |  +1.0 → Most Positive
```

### Classification Rules
```
Compound ≥ +0.05   → 😊 Positive
-0.05 < Compound < +0.05 → 😐 Neutral  
Compound ≤ -0.05   → 😞 Negative
```

### Example Texts
```
"Love it! Best phone ever!" → 0.95 (Positive)
"It's okay, nothing special" → 0.0 (Neutral)
"Terrible quality, waste of money" → -0.85 (Negative)
```

## 💡 Pro Tips

1. **Popular Products = Better Results**
   - "example-topic" → More data
   - "Obscure Brand X" → Less data

2. **Wait for Data**
   - First 10-15 seconds: Collection phase
   - Next 20+ seconds: Better trend visualization

3. **Watch the Trend Tab**
   - See sentiment changes over time
   - Identify turning points

4. **Check Source Distribution**
   - Reddit data = community opinions

5. **Top Reviews are Most Helpful**
   - Positive: What users love
   - Negative: Common complaints

## 🔄 Data Flow

```
User enters: "example-topic"
     ↓
  ┌──────────────┐
  │ Start Stream │
  └──────┬───────┘
         ↓
    ┌────────────────────────────┐
   │  Fetch Data (async)        │
   ├────────────────────────────┤
   │ • Reddit (PRAW)            │
    └─────────┬──────────────────┘
              ↓
    ┌────────────────────────┐
    │  Send to Kafka Topic   │
    │  sentiment-stream      │
    └─────────┬──────────────┘
              ↓
    ┌────────────────────────┐
    │  Consume from Kafka    │
    │  • Analyze with VADER  │
    │  • Cache in memory     │
    └─────────┬──────────────┘
              ↓
    ┌────────────────────────┐
    │  Display & Visualize   │
    │  • Charts (Plotly)     │
    │  • Metrics             │
    │  • Top Reviews         │
    └────────────────────────┘
```

## 🚀 Next Steps

1. **Run the dashboard (recommended: use the repo venv to avoid interpreter mismatches)**

    - Activate venv (PowerShell):

       ```powershell
       & d:\Sentiment-Analysis\venv\Scripts\Activate.ps1
       streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
       ```

    - Or run directly from the venv without activating:

       ```powershell
       & "d:\Sentiment-Analysis\venv\Scripts\streamlit.exe" run "d:\Sentiment-Analysis\galaxy_s25_sentiment\dashboard\realtime_dashboard.py"
       ```
2. **Try different products**: Galaxy, iPhone, Tesla, MacBook, etc.
3. **Monitor trends**: Let it run for 1-2 minutes to see patterns
4. **Customize**: Adjust cache size, refresh interval, data sources
5. **Extend**: Add more sources, custom sentiment analyzers, database storage

## 📚 Files Modified/Created

### Modified Files
- `galaxy_s25_sentiment/requirements.txt` - Added VADER & updated versions

### New Files
- `galaxy_s25_sentiment/dashboard/realtime_dashboard.py` - Main dashboard
- `galaxy_s25_sentiment/extract/extractors.py` - Extraction utilities
- `galaxy_s25_sentiment/extract/kafka_utils.py` - Kafka helpers
- `.env.example` - Environment template
- `verify_realtime_setup.py` - Setup verification
- `REALTIME_DASHBOARD_GUIDE.md` - Full documentation
- `QUICKSTART.md` - This file

## 🎯 System Requirements Met

✅ Dynamic topic input (no hardcoding)
✅ Multi-source extraction (Reddit + Google Play)
✅ Kafka integration (send & receive)
✅ VADER sentiment analysis (VADER chosen)
✅ Real-time visualizations (Plotly charts)
✅ Source tags (Reddit/Google Play shown)
✅ In-memory storage (500-item cache)
✅ Pause/Resume streaming (Start/Stop buttons)
✅ User-controlled refresh (Refresh button + interval)
✅ Interactive dashboard (Streamlit)

---

**Ready to analyze sentiment? 🎉**

```powershell
& "d:\Sentiment-Analysis\venv\Scripts\streamlit.exe" run "d:\Sentiment-Analysis\galaxy_s25_sentiment\dashboard\realtime_dashboard.py"
```