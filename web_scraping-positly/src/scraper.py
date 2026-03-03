# poetry run python scraper.py

# a = 1
# print (a)

# scraper.py
# Step 1: Read and clean the URLs from links.xlsx

import pandas as pd  # library to read Excel files
import os            # library to work with file paths

def load_urls(filepath):
    """
    Reads the links.xlsx file and returns a clean list of URLs.
    filepath = the path to your Excel file
    """

    # Read the Excel file into a pandas DataFrame (like a table)
    df = pd.read_excel(filepath, header=None)

    # Give a name to the only column we have
    df.columns = ['url']

    # Remove invisible characters and spaces from the URL
    df['url'] = df['url'].astype(str).str.strip()

    # Remove the " -" that appeared in row 1
    df['url'] = df['url'].str.replace(r'\s*-\s*$', '', regex=True).str.strip()

    # Remove the "#" at the end of URLs
    df['url'] = df['url'].str.rstrip('#').str.strip()

    # Remove rows that are not real URLs (like the UNAM row)
    df = df[df['url'].str.startswith('http')]

    # Remove duplicate URLs
    df = df.drop_duplicates(subset='url')

    # Reset the row numbers after removing rows
    df = df.reset_index(drop=True)

    return df['url'].tolist()  # return a simple list of URLs


# --- TEST: run this file directly to see if it works ---
if __name__ == '__main__':

    # scraper.py lives in src/
    # links.xlsx lives in data/
    # so we need to go one level up with ".."
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # finds the src/ folder
    filepath = os.path.join(BASE_DIR, '..', 'data', 'links.xlsx')  # goes up to data/

    # Load the URLs
    urls = load_urls(filepath)

    # Print how many URLs we have
    print(f'Total URLs loaded: {len(urls)}')
    print()

    # Print each URL with its number
    for i, url in enumerate(urls, 1):
        print(f'{i}: {url}')