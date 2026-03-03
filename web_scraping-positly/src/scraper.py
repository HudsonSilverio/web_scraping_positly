# scraper.py
# Step 1: Read and clean the URLs from links.xlsx
# Step 2: Open a URL and download its HTML

import pandas as pd    # library to read Excel files
import os              # library to work with file paths
import time            # library to add waiting time between requests
import requests        # library to open URLs and download HTML


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


def get_page_html(url):
    """
    Opens a URL and returns the HTML content of the page.
    url = the URL we want to open
    """

    # Pretend to be a real Chrome browser so the site does not block us
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    # Try to open the page up to 3 times before giving up
    for attempt in range(1, 4):  # attempt = 1, 2, 3

        try:
            # Open the URL and wait max 15 seconds for a response
            response = requests.get(url, headers=headers, timeout=15)

            # Check if the page loaded successfully (code 200 = success)
            response.raise_for_status()

            # Return the HTML content of the page
            return response.text

        except requests.exceptions.Timeout:
            # Page took too long to load
            print(f'  ⏱ Timeout on attempt {attempt} for: {url}')

        except requests.exceptions.HTTPError as e:
            # Page returned an error like 404 (not found) or 403 (blocked)
            print(f'  ❌ HTTP error on attempt {attempt}: {e}')
            break  # no point retrying if the page does not exist

        except requests.exceptions.ConnectionError:
            # No internet or site is down
            print(f'  🔌 Connection error on attempt {attempt} for: {url}')

        # Wait before trying again (2 seconds on attempt 1, 5 seconds on attempt 2)
        wait_time = 2 if attempt == 1 else 5
        print(f'  ⏳ Waiting {wait_time} seconds before retry...')
        time.sleep(wait_time)

    # If all 3 attempts failed, return None
    print(f'  💀 Failed after 3 attempts: {url}')
    return None


# --- TEST: run this file directly to see if it works ---
if __name__ == '__main__':

    # scraper.py lives in src/
    # links.xlsx lives in data/
    # so we need to go one level up with ".."
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # finds the src/ folder
    filepath = os.path.join(BASE_DIR, '..', 'data', 'links.xlsx')  # goes up to data/

    # Step 1: load the URLs
    urls = load_urls(filepath)
    print(f'Total URLs loaded: {len(urls)}')
    print()

    # Step 2: test with only the FIRST URL
    # Yale is less strict than Harvard - better for testing
    test_url = urls[10]
    print(f'Testing with: {test_url}')
    print()

    print('⏳ Calling get_page_html now...')  # ← add this line
    html = get_page_html(test_url)
    print('✅ get_page_html finished!')       # ← add this line

    html = get_page_html(test_url)

    # Check if we got the HTML back
    if html:
        print(f'✅ Success! Downloaded {len(html)} characters of HTML')
        print()
        print('First 200 characters of HTML:')
        print(html[:200])
    else:
        print('❌ Failed to download the page')