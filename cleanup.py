import os
import shutil
from pathlib import Path

base = r"d:\Sentiment-Analysis"
os.chdir(base)

# Directories to delete
dirs = ['.venv', 'kafka_2.13-4.1.0']
for d in dirs:
    try:
        if os.path.isdir(d):
            shutil.rmtree(d)
            print(f"✓ Deleted directory: {d}")
    except Exception as e:
        print(f"✗ Failed {d}: {e}")

# Files to delete
files = [
    'kafka_2.13-4.1.0.tgz',
    'kafka_start_output.txt',
    'data/amazon_product_debug.html',
    'data/amazon_reviews_debug.html',
    'data/flipkart_reviews_debug.html',
    'data/amazon_reviews_selenium.csv',
    'data/flipkart_reviews_s25_ultra.csv',
    'data/flipkart_reviews_selenium.csv',
    'data/flipkart_reviews.csv',
    'galaxy_s25_sentiment/data/sentiment_analysis.db',
]

for f in files:
    try:
        if os.path.isfile(f):
            os.remove(f)
            print(f"✓ Deleted file: {f}")
    except Exception as e:
        print(f"✗ Failed {f}: {e}")

print("\n✓ Cleanup complete!")
