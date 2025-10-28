
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os

# Load cleaned data
df = pd.read_csv("data/cleaned_all.csv")

# Sentiment analysis function
analyzer = SentimentIntensityAnalyzer()
def get_sentiment(text):
        score = analyzer.polarity_scores(str(text))["compound"]
        if score >= 0.05:
                return "positive"
        elif score <= -0.05:
                return "negative"
        else:
                return "neutral"

df["sentiment"] = df["text"].fillna("").astype(str).apply(get_sentiment)

# Save to sentiment_all.csv for dashboard
os.makedirs("data", exist_ok=True)
df.to_csv("data/sentiment_all.csv", index=False)
print(f"✅ Sentiment-labeled data saved to data/sentiment_all.csv with {len(df)} records")
