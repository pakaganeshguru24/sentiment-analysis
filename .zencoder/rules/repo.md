---
description: Repository Information Overview
alwaysApply: true
---

# Galaxy S25 Sentiment Analysis Information

## Summary
This project extracts, transforms, analyzes, and visualizes sentiment data from multiple sources for the Samsung Galaxy S25. It follows an ETL (Extract, Transform, Load) pattern to collect reviews and comments from various platforms, process the data, perform sentiment analysis, and present the results in a Streamlit dashboard.

## Structure
- **galaxy_s25_sentiment/**: Main project directory
  - **extract/**: Scripts for extracting data from various sources (Amazon, Flipkart, Google, Reddit, Twitter)
  - **transform/**: Data cleaning and preprocessing scripts
  - **sentiment/**: Sentiment analysis implementation
  - **load/**: Database storage scripts
  - **dashboard/**: Streamlit visualization dashboard
  - **data/**: Stores extracted and processed data files
- **data/**: Root-level data directory with CSV files
- **venv/**: Python virtual environment

## Language & Runtime
**Language**: Python
**Version**: 3.13.5
**Package Manager**: pip

## Dependencies
**Main Dependencies**:
- **Data Processing**: pandas, numpy
- **Web Scraping**: selenium, beautifulsoup4, requests, google-play-scraper, praw (Reddit API)
- **Visualization**: matplotlib, seaborn, streamlit
- **Sentiment Analysis**: vaderSentiment
- **Database**: sqlite3 (built-in)

**Development Dependencies**:
- webdriver-manager (for Selenium)
- watchdog (for Streamlit)

## Data Sources
The project extracts data from multiple sources:
- **Amazon**: Using SerpAPI to fetch product reviews
- **Flipkart**: Web scraping with Selenium and BeautifulSoup
- **Google Play**: Using google-play-scraper for app reviews
- **Reddit**: Using PRAW (Python Reddit API Wrapper)
- **Twitter**: Using snscrape (currently disabled due to Python 3.13 compatibility issues)

## Data Pipeline
1. **Extraction**: Multiple scripts in the `extract/` directory collect data from different sources
2. **Transformation**: `clean_data.py` merges and cleans the collected data
3. **Sentiment Analysis**: Analyzes the cleaned data to determine sentiment
4. **Loading**: `save_to_db.py` stores processed data in SQLite database
5. **Visualization**: Streamlit dashboard in `dashboard/app.py` presents the results

## Build & Installation
```bash
# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r galaxy_s25_sentiment/requirements.txt

# Additional dependencies not listed in requirements.txt
pip install pandas numpy matplotlib seaborn beautifulsoup4 selenium webdriver-manager
pip install requests google-play-scraper praw vaderSentiment snscrape
```

## Running the Application
```bash
# Extract data from sources
python galaxy_s25_sentiment/extract/amazon_extract.py
python galaxy_s25_sentiment/extract/flipkart_extract.py
python galaxy_s25_sentiment/extract/google_extract.py
python galaxy_s25_sentiment/extract/reddit_extract.py

# Clean and transform data
python galaxy_s25_sentiment/transform/clean_data.py

# Perform sentiment analysis
python galaxy_s25_sentiment/sentiment/sentiment_analysis.py

# Load data to database
python galaxy_s25_sentiment/load/save_to_db.py

# Run the dashboard
streamlit run galaxy_s25_sentiment/dashboard/app.py
```

## Testing
No formal testing framework was identified in the repository. The project appears to use manual verification of outputs.