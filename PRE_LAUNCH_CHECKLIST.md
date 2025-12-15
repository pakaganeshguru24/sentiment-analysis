# Pre-Launch Checklist ✅

Before running the real-time sentiment dashboard, complete this checklist to ensure everything is configured correctly.

## 🔧 System Requirements
- [ ] Python 3.10+ (check with: `python --version`)
- [ ] pip package manager installed
- [ ] 2GB+ RAM available
- [ ] Internet connection active
- [ ] Docker installed (for Kafka)

## 📦 Dependencies Installation

### Step 1: Create/Activate Virtual Environment
```powershell
# If venv doesn't exist
python -m venv venv

# Activate venv
.\venv\Scripts\Activate.ps1
```
- [ ] Virtual environment created
- [ ] Virtual environment activated

### Step 2: Install Dependencies
```powershell
pip install -r galaxy_s25_sentiment/requirements.txt
```
- [ ] All packages installed successfully
- [ ] No error messages in output

### Step 3: Verify Core Packages
```powershell
# Test VADER
python -c "from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer; print('✅ VADER works')"

# Test Streamlit
python -c "import streamlit; print('✅ Streamlit works')"

# Test Kafka
python -c "from kafka import KafkaProducer, KafkaConsumer; print('✅ Kafka works')"

# Test extractors
python -c "import sys; sys.path.insert(0, 'galaxy_s25_sentiment/extract'); from extractors import extract_reddit_reviews; print('✅ Extractors work')"
```
- [ ] VADER import successful
- [ ] Streamlit import successful
- [ ] Kafka import successful
- [ ] Extractors import successful

## ⚙️ Configuration Setup

### Step 1: Create .env File
```powershell
# Copy the example
cp .env.example .env
```
- [ ] `.env` file created in project root

### Step 2: Configure Kafka Settings (Required)
Edit `.env`:
```
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=sentiment-stream
```
- [ ] KAFKA_BOOTSTRAP set to `localhost:9092`
- [ ] KAFKA_TOPIC set to `sentiment-stream`

### Step 3: Configure Reddit API (Recommended but Optional)

To enable Reddit extraction:
1. Go to: https://www.reddit.com/prefs/apps
2. Click: "Create Application" or "Create Another App"
3. Select: **"script"** type
4. Fill in:
   - App name: (e.g., "Sentiment-Analysis")
   - Description: (e.g., "Sentiment analysis tool")
   - About URL: (leave empty or enter project URL)
5. Click: "Create app"
6. Copy the credentials to `.env`:

```
REDDIT_CLIENT_ID=<your_client_id>
REDDIT_CLIENT_SECRET=<your_client_secret>
REDDIT_USER_AGENT=SentimentAnalysis/1.0 by <your_reddit_username>
REDDIT_USERNAME=<your_reddit_username>
REDDIT_PASSWORD=<your_reddit_password>
```

- [ ] Client ID copied to `.env`
- [ ] Client Secret copied to `.env`
- [ ] User Agent set to `.env`
- [ ] Reddit username in `.env` (optional but helpful)
- [ ] Reddit password in `.env` (optional but helpful)

### Step 4: Verify .env File
```powershell
cat .env
```
- [ ] .env file contains KAFKA settings
- [ ] .env file contains Reddit credentials (if using)
- [ ] No "your_..." placeholders remain (unless intentionally left blank)

## 🔌 Kafka Setup

### Step 1: Start Kafka
```powershell
# Using Docker Compose (recommended)
docker-compose up -d
```

Wait 15-20 seconds for Kafka to fully start.

- [ ] Docker daemon running
- [ ] Kafka container started
- [ ] Zookeeper container started

### Step 2: Verify Kafka Connection
```powershell
python -c "from kafka import KafkaProducer; p = KafkaProducer(bootstrap_servers='localhost:9092'); print('✅ Kafka connected'); p.close()"
```
- [ ] Kafka connection successful
- [ ] No connection refused errors

### Step 3: Check Kafka Topic
```powershell
# Verify sentiment-stream topic exists
# (This will be created automatically on first use)
```
- [ ] Kafka is ready to accept messages

## 🔍 Verification Script

### Run Setup Verification
```powershell
python verify_realtime_setup.py
```

Expected output should show:
- ✅ Python version
- ✅ All core dependencies
- ✅ Environment configuration
- ✅ Kafka connection
- ✅ Extraction modules
- ✅ VADER analyzer
- ✅ Streamlit

- [ ] All verification checks passed
- [ ] No ❌ marks in output
- [ ] No error messages

## 📊 Dashboard Files

### Verify Files Exist
```powershell
# Dashboard
ls galaxy_s25_sentiment/dashboard/realtime_dashboard.py

# Extractors
ls galaxy_s25_sentiment/extract/extractors.py
ls galaxy_s25_sentiment/extract/kafka_utils.py

# Utilities
ls verify_realtime_setup.py
ls demo_sentiment_analysis.py

# Docs
ls REALTIME_DASHBOARD_GUIDE.md
ls QUICKSTART.md
```

- [ ] `realtime_dashboard.py` exists
- [ ] `extractors.py` exists
- [ ] `kafka_utils.py` exists
- [ ] `verify_realtime_setup.py` exists
- [ ] Documentation files exist

## 🧪 Pre-Launch Tests

### Test 1: Basic VADER Sentiment
```powershell
python -c "from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer; a = SentimentIntensityAnalyzer(); print(a.polarity_scores('I love this!'))"
```
Expected: `compound` score between 0 and 1
- [ ] VADER returns sentiment scores

### Test 2: Extraction Modules
```powershell
python -c "import sys; sys.path.insert(0, 'galaxy_s25_sentiment/extract'); from extractors import extract_reddit_reviews; print('Extractors loaded successfully')"
```
- [ ] Extractors module loads without errors

### Test 3: Streamlit Availability
```powershell
streamlit --version
```
Expected: Version number (e.g., "Streamlit, version 1.49.1")
- [ ] Streamlit is installed and runnable

### Test 4: Demo Script (Optional)
```powershell
python demo_sentiment_analysis.py
```
Choose option "1" to test basic sentiment analysis
- [ ] Demo script runs without errors
- [ ] Sentiment analysis produces valid scores

## 🚀 Ready to Launch

### Final Checks
- [ ] All dependencies installed ✅
- [ ] .env file configured with Kafka ✅
- [ ] Reddit credentials added (or skipped intentionally) ✅
- [ ] Kafka running and verified ✅
- [ ] Verification script passed ✅
- [ ] All required files exist ✅
- [ ] Pre-launch tests passed ✅

### Launch Dashboard
```powershell
# Make sure you're in the project directory
cd d:\Sentiment-Analysis

# Verify venv is activated (should see (venv) in prompt)

# Start the dashboard
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
```

Expected behavior:
- Streamlit welcome message appears
- Dashboard URL: http://localhost:8501 (or similar)
- Browser opens with dashboard
- Dashboard loads with empty state

- [ ] Streamlit starts successfully
- [ ] Dashboard loads in browser
- [ ] No error messages in terminal

## 📝 First Run Guide

### 1. Enter Topic Name
```
Type in sidebar: "example-topic"
```
- [ ] Can type in input box

### 2. Start Streaming
```
Click: "▶️ Start Streaming"
```
- [ ] Button click registers
- [ ] Status shows "🔍 Searching for..."

### 3. Wait for Data
```
Wait: 10-15 seconds
```
- [ ] See status: "✅ Extracted X items"
- [ ] Dashboard shows metrics

### 4. View Results
- [ ] Metrics panel shows sentiment breakdown
- [ ] Pie chart displays sentiment distribution
- [ ] At least one visualization works

## ⚠️ Troubleshooting Steps

If you encounter issues:

### Issue: "Kafka connection failed"
1. Check if Docker is running: `docker ps`
2. Start Kafka: `docker-compose up -d`
3. Wait 15 seconds
4. Test connection: `python verify_realtime_setup.py`

### Issue: "ModuleNotFoundError"
1. Activate venv: `.\venv\Scripts\Activate.ps1`
2. Reinstall: `pip install -r galaxy_s25_sentiment/requirements.txt`
3. Check Python version: `python --version` (should be 3.10+)

### Issue: "No data after starting stream"
1. Check .env Reddit credentials are correct
2. Try different topic: "Galaxy", "Samsung", "Apple"
3. Check internet connection
4. Check terminal for error messages

### Issue: "Streamlit not found"
1. Activate venv: `.\venv\Scripts\Activate.ps1`
2. Install: `pip install streamlit`
3. Verify: `streamlit --version`

### Issue: "ImportError for extractors"
1. Ensure extractors.py exists: `ls galaxy_s25_sentiment/extract/extractors.py`
2. Check path in realtime_dashboard.py
3. Restart terminal to clear Python cache

## 📞 Support Resources

- Full Guide: `REALTIME_DASHBOARD_GUIDE.md`
- Quick Start: `QUICKSTART.md`
- Demo Script: `python demo_sentiment_analysis.py`
- Verification: `python verify_realtime_setup.py`

## ✅ Sign-Off

I confirm that:
- [ ] All checklist items are complete
- [ ] All tests passed
- [ ] Dashboard is ready to launch
- [ ] I'm ready to start analyzing sentiment

---

**You're ready to go! 🎉**

```powershell
streamlit run galaxy_s25_sentiment/dashboard/realtime_dashboard.py
```

Then:
1. Open http://localhost:8501 in your browser
2. Enter a product/topic name
3. Click "Start Streaming"
4. Watch sentiment analysis in real-time!