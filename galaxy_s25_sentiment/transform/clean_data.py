# Clean and merge data
import pandas as pd
import os

data_dir = "data"

# Load Reddit data (if exists)
reddit_file = os.path.join(data_dir, "reddit_posts.csv")
reddit_df = pd.read_csv(reddit_file) if os.path.exists(reddit_file) else pd.DataFrame()

if not reddit_df.empty:
    reddit_df["source"] = "reddit"
    reddit_df.rename(columns={"title": "text"}, inplace=True)
else:
    reddit_df = pd.DataFrame(columns=["text", "source"])

# Load Google Play reviews (if exists)
google_file = os.path.join(data_dir, "google_reviews.csv")
google_df = pd.read_csv(google_file) if os.path.exists(google_file) else pd.DataFrame()

if not google_df.empty:
    google_df["source"] = "google"
    google_df.rename(columns={"content": "text"}, inplace=True)
else:
    google_df = pd.DataFrame(columns=["text", "source"])


# Load Amazon reviews (if exists)
amazon_file = os.path.join(data_dir, "amazon_reviews_selenium.csv")
amazon_df = pd.read_csv(amazon_file) if os.path.exists(amazon_file) else pd.DataFrame()
if not amazon_df.empty:
    amazon_df["source"] = "amazon"
    amazon_df.rename(columns={"review": "text"}, inplace=True)
else:
    amazon_df = pd.DataFrame(columns=["text", "source"])


# Load Flipkart reviews (if exists)
flipkart_file = os.path.join(data_dir, "flipkart_reviews_selenium.csv")
flipkart_df = pd.read_csv(flipkart_file) if os.path.exists(flipkart_file) else pd.DataFrame()
if not flipkart_df.empty:
    flipkart_df["source"] = "flipkart"
    flipkart_df.rename(columns={"review": "text"}, inplace=True)
else:
    flipkart_df = pd.DataFrame(columns=["text", "source"])

# Merge all sources
combined_df = pd.concat([
    reddit_df[["text", "source"]],
    google_df[["text", "source"]],
    amazon_df[["text", "source"]],
    flipkart_df[["text", "source"]]
], ignore_index=True)


# Basic cleaning
combined_df["text"] = combined_df["text"].astype(str).str.lower()
combined_df["text"] = combined_df["text"].str.replace(r"http\S+|www\S+", "", regex=True)  # remove links
combined_df["text"] = combined_df["text"].str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)  # remove special chars

# Remove empty reviews
combined_df = combined_df[combined_df["text"].str.strip() != ""]

# Optionally trim long reviews for dashboard display (e.g., 500 chars max)
combined_df["text"] = combined_df["text"].str.slice(0, 500)

# Save cleaned dataset
os.makedirs(data_dir, exist_ok=True)
cleaned_file = os.path.join(data_dir, "cleaned_all.csv")
combined_df.to_csv(cleaned_file, index=False)

print(f"✅ Cleaned data saved to {cleaned_file} with {len(combined_df)} records")
