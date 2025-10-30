

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
 # ...existing code...
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time


# Setup Chrome driver (no headless mode)
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)


url = "https://www.amazon.in/Samsung-Smartphone-Silverblue-Snapdragon-ProVisual/dp/B0DSKNKCYX/ref=sr_1_3?crid=2UDHVVPSEZDL0&dib=eyJ2IjoiMSJ9.TnXF_RGJpd3TbN40AampmBIsYcDjEXFFRfMMeVooMalAs_MyQZfvxKFclyrXK93PfWKTpJfqqbrZDjewvKJkQlPiNIXCjC6Kf0V7SNdMdc-S_kRS2m750csuY_18U8e-ro8zU6ZrZZWg2AH7fue_dlM41juQuaRPm2gM8oSYUdxL9ZPrWgOHXn73WQ04glQvBAMuk7RPg1z50kftl24s-_oihIU4GeM6k_1Rcx2-pMY.etNod6vRlthV4betAj7xtEH_BH3Bki0gf8jDHDjsMdY&dib_tag=se&keywords=s25%2Bultra&qid=1759998740&sprefix=%2Caps%2C231&sr=8-3&th=1"
driver.get(url)
time.sleep(3)

# Save the page source for inspection
import os
os.makedirs("data", exist_ok=True)
with open("data/amazon_product_debug.html", "w", encoding="utf-8") as f:
    f.write(driver.page_source)

# Scroll to bottom to trigger dynamic content
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(2)

# Click "See all reviews" if available
try:
    see_all = driver.find_element(By.PARTIAL_LINK_TEXT, "See all reviews")
    see_all.click()
    time.sleep(3)
except:
    print("No 'See all reviews' link found.")



# Only extract reviews from the first page, no ratings, no pagination, no CSV save
all_reviews = []
review_blocks = driver.find_elements(By.CSS_SELECTOR, "div[data-hook='review']")
print(f"Found {len(review_blocks)} review blocks on first page.")
for block in review_blocks:
    review_text = ""
    try:
        review_elem = block.find_element(By.CSS_SELECTOR, ".review-text-content span")
        review_text = review_elem.text.strip()
    except:
        pass
    if review_text:
        all_reviews.append(review_text)
driver.quit()
print(f"Extracted {len(all_reviews)} reviews from first page.")

