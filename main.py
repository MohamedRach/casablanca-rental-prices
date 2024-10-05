from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.chrome.options import Options
from typing import Union
from fastapi import FastAPI

app = FastAPI()

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1024,768")


def get_listing_data(listing):
    try:
        space = listing.find_element(
            By.CSS_SELECTOR, "h4.listingH4.floatR"
        ).text.strip()
    except:
        space = listing.find_element(By.CSS_SELECTOR, "p").text.strip()
    location = listing.find_element(By.CSS_SELECTOR, "h3.listingH3").text.strip()
    date_of_publication = listing.find_element(
        By.CSS_SELECTOR, "span.listingDetails.iconPadR"
    ).text.strip()
    price = listing.find_element(By.CSS_SELECTOR, "span.priceTag").text.strip()

    listing_data = {
        "area": space,
        "price": price,
        "location": location,
        "Date": date_of_publication,
    }
    return listing_data


def scrape_data():
    data = []
    i = 1
    driver = webdriver.Chrome(
        options=chrome_options, service=Service(ChromeDriverManager().install())
    )
    while i < 4:
        driver.get(
            "https://www.mubawab.ma/fr/ct/casablanca/immobilier-a-louer:p:" + str(i)
        )
        time.sleep(3)
        listings = driver.find_elements(By.CSS_SELECTOR, "li.listingBox.w100")
        for listing in listings:
            try:
                listing_data = get_listing_data(listing)
                data.append(listing_data)
            except Exception:
                print("can't get data")

        i += 1
    driver.quit()
    return data


@app.get("/")
def read_root():
    return scrape_data()
