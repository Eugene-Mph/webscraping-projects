import pandas as pd
import re

# this file does basic data cleaning for the book scraping project

print("Starting data cleaning process...")

df = pd.read_csv("raw_books_data.csv")
print("Raw data loaded successfully.")

# regex patterns
number_pattern = re.compile(r'\d*\.\d*')
currency_pattern = re.compile(r'\D')

def get_number(text):
    match = number_pattern.search(text)
    return float(match.group()) if match else 0.0

# function to extract currency symbol
def get_currency(text):
    match = currency_pattern.search(text)
    return match.group() if match else ""

# dictionary used to convert ratings to numbers
converter = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5
}


# converting ratings to numbers
df['rating'] = df['rating'].apply(lambda text: converter.get(text, text))
print("Ratings successfully converted from string to integer.")

# convert availability to integer
df["availability"] = df["availability"].apply(lambda text: int(re.search(r'\d+', text).group()))
print("Availability successfully converted from string to integer.")


# create a currency symbol column
df["currency"] = df["tax"].apply(get_currency)
print("Currency symbols extracted successfully.")


for col in ["tax", "price_excl_tax", "price_incl_tax"]:
    df[col] = df[col].apply(get_number)
    print(f"Column '{col}' successfully converted from string to float.")



# removing tax column if it has no data
for col in ["tax", "review_count"]:
    if not df[col].gt(0).any():
        df.drop(columns=[col], inplace=True)
    print(f"Column '{col}' identified as empty and successfully dropped from the DataFrame.")



# compare price_excl_tax and price_incl_tax merge columns if they are the same
if df["price_excl_tax"].equals(df["price_incl_tax"]):
    df["price"] = df["price_excl_tax"]
    df.drop(columns=["price_excl_tax", "price_incl_tax"], inplace=True)
    print("Price_excl_tax and price_incl_tax columns merged successfully to form a single price column.")

# reindex columns for better view in csv format
df = df.reindex(columns=[
    "upc", "product_type", 
    "title", 
    "category", 
    "currency", 
    "price", 
    "rating", 
    "availability", 
    "image_url", "description"

])
print("DataFrame reindexed successfully.")

# save the clean data to a new csv file
df.to_csv("clean_books_data.csv", index=False)