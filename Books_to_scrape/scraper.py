from bs4 import BeautifulSoup as bs
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import urljoin
from time import sleep
import pandas as pd
import requests
import random


base_url = "http://books.toscrape.com"

# Configure retries for requests
session = requests.Session()
retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

def get_book_urls(doc):
    book_links = doc.select("article h3 a")
    return [f"{base_url}/catalogue/{link['href']}" for link in book_links]





# This function extracts raw data about a book from its detail page and returns it as a python dictionary
def get_book_data(book_url):
    
    try:
        response = session.get(book_url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {book_url}: {e}")
        return None

    doc = bs(response.content, "html.parser")
    

    title_element = doc.select_one("article h1")
    rating_element = doc.select_one("article .star-rating").get("class")[1]
    category_element = doc.select_one(".breadcrumb li:nth-child(3) a")
    description_element = doc.find("article").find_all("p")[3]
    image_element = doc.select_one("article img")
    upc_element = doc.select_one("tr td")
    product_type_element = doc.select_one("tr:nth-child(2) td")
    price_excl_tax_element = doc.select_one("tr:nth-child(3) td")
    price_incl_tax_element = doc.select_one("tr:nth-child(4) td")
    tax_element = doc.select_one("tr:nth-child(5) td")
    availability_element = doc.select_one("tr:nth-child(6) td")
    review_count_element = doc.select_one("tr:nth-child(7) td")

    raw_book_data = {
        "title": title_element.get_text(strip=True) if title_element else None,
        "rating": rating_element if rating_element else None,
        "category": category_element.text if category_element else None,
        "description": description_element.get_text(strip=True) if description_element else None,
        "image_url": urljoin(base_url, image_element.get('src')) if image_element else None,
        "upc": upc_element.get_text(strip=True) if upc_element else None,
        "product_type": product_type_element.get_text(strip=True) if product_type_element else None,
        "price_excl_tax": price_excl_tax_element.get_text(strip=True) if price_excl_tax_element else None,
        "price_incl_tax": price_incl_tax_element.get_text(strip=True) if price_incl_tax_element else None,
        "tax": tax_element.get_text(strip=True) if tax_element else None,
        "availability": availability_element.get_text(strip=True) if availability_element else None,
        "review_count": review_count_element.get_text(strip=True) if review_count_element else None
    }
    
    return raw_book_data



# Get all book data from a single page
def page_scraper(page_url):

    page_data = []
    response = session.get(page_url)
    doc = bs(response.content, "html.parser")

    sleep(random.uniform(0.1, 0.5))

    page_book_urls = get_book_urls(doc)
    for book_url in page_book_urls:
        # print(f"Scraping {book_url}")
        book_data = get_book_data(book_url)
        page_data.append(book_data)
        sleep(random.uniform(0.1, 0.5))

    return page_data



# The function gets all book data from multiple pages and returns a pandas DataFrame
def scrape_pages(number_of_pages):
    all_books_data = []

    for page_number in range(1, number_of_pages + 1):

        page_url = f"{base_url}/catalogue/page-{page_number}.html"
        print(f"Scraping page {page_number} of {number_of_pages}")

        page_data = page_scraper(page_url)
        all_books_data.extend(page_data)
        sleep(random.uniform(0.1, 0.5))
    

    return pd.DataFrame(all_books_data)


# save the data to a CSV file
def save_to_csv(df, filename):
    df.to_csv(filename, index=False)

# The main function that controls the scraping process
def main(number_of_pages):
    print("Starting the web scraping process...")
    df = scrape_pages(number_of_pages)
    save_to_csv(df, "raw_books_data.csv")

main(50)


# connection timeout
# to many requests to quickly