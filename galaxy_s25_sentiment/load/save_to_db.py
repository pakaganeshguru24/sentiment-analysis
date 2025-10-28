# Save processed data to database
import sqlite3
import pandas as pd

# Load sentiment data
df = pd.read_csv("data/sentiment_tweets.csv")

# Connect to SQLite (creates file if not exists)
conn = sqlite3.connect("data/sentiment_analysis.db")

# Save to table
df.to_sql("tweets_sentiment", conn, if_exists="replace", index=False)

# Confirm row count
rows = conn.execute("SELECT COUNT(*) FROM tweets_sentiment").fetchone()[0]
print(f"✅ Data loaded into SQLite. Total rows: {rows}")

conn.close()

