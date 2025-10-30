from serpapi import GoogleSearch
import json
import os
import pandas as pd

# Your SerpApi API key - Replace with your actual key
api_key = "YOUR_SERPAPI_API_KEY"

# Amazon product ASIN (from the URL)
asin = "B0DSKNKCYX"

# Parameters for the search
params = {
    "engine": "amazon",
    "asin": asin,
    "domain": "amazon.in",
    "api_key": api_key,
    "amazon_domain": "amazon.in",
}

try:
    # Create the search
    search = GoogleSearch(params)
    results = search.get_dict()

    # Print basic product info
    if "title" in results:
        print(f"Product: {results['title']}")

    # Extract reviews if available
    all_reviews = []
    if "reviews" in results:
        for review in results["reviews"]:
            if "content" in review:
                all_reviews.append(review["content"])

    print(f"Found {len(all_reviews)} reviews")

    # Save reviews to CSV
    if all_reviews:
        df = pd.DataFrame({"review": all_reviews})
        os.makedirs("data", exist_ok=True)
        df.to_csv("data/amazon_reviews_serpapi.csv", index=False)
        print(f"✅ Saved reviews to data/amazon_reviews_serpapi.csv")

except Exception as e:
    print(f"Error fetching reviews: {e}")