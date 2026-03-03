# scraper.py
# Step 1: Read and clean the URLs from links.xlsx
# Step 2: Open a URL and download its HTML
# Step 3: Find emails inside the HTML
# Step 4: Find names + emails together (contacts)
# Step 5: Handle pagination (multiple pages)
# Step 6: Handle Scenario 3 (click links with Selenium)
# Step 7: Error handling + errors_log.csv

import pandas as pd                                        # library to read Excel files
import os                                                  # library to work with file paths
import time                                                # library to add waiting time between requests
import requests                                            # library to open URLs and download HTML
import re                                                  # library to find patterns in text
import csv                                                 # library to write CSV files
from datetime import datetime                              # library to get current date and time
from bs4 import BeautifulSoup                              # library to read HTML structure
from selenium import webdriver                             # controls Chrome browser
from selenium.webdriver.chrome.service import Service      # manages Chrome driver
from selenium.webdriver.common.by import By                # finds elements on page
from selenium.webdriver.support.ui import WebDriverWait    # waits for page to load
from selenium.webdriver.support import expected_conditions as EC  # wait conditions
from webdriver_manager.chrome import ChromeDriverManager   # installs Chrome driver automatically


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


def find_emails(html):
    """
    Searches inside the HTML and returns a list of clean emails found.
    html = the raw HTML text downloaded by get_page_html()
    """

    # Parse the HTML with BeautifulSoup so we can search inside it
    soup = BeautifulSoup(html, 'html.parser')

    # Create an empty set to store emails (set avoids duplicates automatically)
    emails_found = set()

    # --- Strategy 1: Find mailto: links ---
    # Many sites write emails as: <a href="mailto:john@yale.edu">
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        if href.startswith('mailto:'):
            # Remove the "mailto:" part and clean the email
            email = href.replace('mailto:', '').strip().lower()
            if email:
                emails_found.add(email)

    # --- Strategy 2: Find emails written as plain text ---
    # Some sites write: john.smith@yale.edu directly in the page text
    page_text = soup.get_text()

    # regex pattern that matches most email formats
    email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    text_emails = re.findall(email_pattern, page_text)
    for email in text_emails:
        emails_found.add(email.lower())

    # --- Strategy 3: Fix obfuscated emails ---
    # Some sites hide emails like: john [at] yale [dot] edu
    obfuscated = re.findall(
        r'[a-zA-Z0-9_.+-]+\s*[\[\(]at[\]\)]\s*[a-zA-Z0-9-]+\s*[\[\(]dot[\]\)]\s*[a-zA-Z]{2,}',
        page_text
    )
    for em in obfuscated:
        # Fix [at] → @ and [dot] → .
        fixed = re.sub(r'\s*[\[\(]at[\]\)]\s*', '@', em)
        fixed = re.sub(r'\s*[\[\(]dot[\]\)]\s*', '.', fixed)
        emails_found.add(fixed.lower())

    # --- Clean all emails found ---
    clean_emails = set()
    for email in emails_found:

        # Remove words that get attached AFTER the domain
        # Example: yale.eduphone → yale.edu
        email = re.sub(r'\.(edu|com|org|ac|uk)(phone|website|fax|email|mail|contact|web)',
                       r'.\1', email)

        # Remove words that get attached BEFORE the @
        # Example: professorshirley.wang@ → shirley.wang@
        email = re.sub(r'^(professor|prof|dr|mr|mrs|ms|st\.)', '', email)

        # Remove any character that is not valid in an email
        email = re.sub(r'[^a-zA-Z0-9_.+-@]', '', email)

        # Only keep emails that have exactly one @ and a valid domain
        if email.count('@') == 1 and '.' in email.split('@')[1]:
            clean_emails.add(email.strip())

    return list(clean_emails)


def find_contacts(html, url):
    """
    Finds name + email pairs from a page.
    html = the raw HTML of the page
    url  = the URL of the page (used to fill the department_url column)
    """

    # Parse the HTML
    soup = BeautifulSoup(html, 'html.parser')

    # List to store all contacts found as dictionaries
    contacts = []

    # --- Strategy: find all mailto: links on the page ---
    for mailto_tag in soup.find_all('a', href=True):
        href = mailto_tag['href']

        # Only process real mailto links
        if not href.startswith('mailto:'):
            continue

        # Extract and clean the email
        email = href.replace('mailto:', '').strip().lower()
        if not email or '@' not in email:
            continue

        # Clean dirty suffixes like "phone" or "website"
        email = re.sub(
            r'\.(edu|com|org|ac|uk)(phone|website|fax|email|mail|contact|web)',
            r'.\1', email
        )

        # Now try to find the name near this email link
        # We go UP the HTML tree to find the parent container
        name = ''
        parent = mailto_tag.parent  # go one level up

        # Try up to 5 levels up to find a name
        for _ in range(5):
            if parent is None:
                break

            # Look for a heading tag (h1, h2, h3, h4) inside this container
            heading = parent.find(['h1', 'h2', 'h3', 'h4'])
            if heading:
                name = heading.get_text(strip=True)
                break

            # If no heading, look for common name CSS classes
            name_tag = parent.find(class_=lambda c: c and any(
                word in c.lower() for word in ['name', 'title', 'person', 'faculty']
            ))
            if name_tag:
                name = name_tag.get_text(strip=True)
                break

            # Go one more level up and try again
            parent = parent.parent

        # --- Clean the name ---
        # Remove the » character that some sites add
        name = name.replace('»', '').strip()

        # Remove titles like PhD, MD, Dr that get attached to the name
        name = re.sub(r'\b(PhD|MD|Dr|Prof|MSc|BA|MA)\b', '', name)

        # Remove extra spaces (Ran   An → Ran An)
        name = re.sub(r'\s+', ' ', name).strip()

        # If we still have no name, use the email username as fallback
        # Example: ilker.yildirim@yale.edu → Ilker Yildirim
        if not name:
            username = email.split('@')[0]          # get part before @
            name = username.replace('.', ' ').replace('-', ' ').title()

        # Extract university name from the URL
        # Example: psychology.yale.edu → Yale
        domain = url.split('/')[2]                  # get the domain part
        university = domain.replace('www.', '').split('.')[1].title()

        # Build the contact dictionary with all columns for our CSV
        contact = {
            'full_name':        name,
            'university_name':  university,
            'department':       'Psychology',
            'department_url':   url,
            'email':            email,
            'research_lines':   '',         # will be empty for now
            'confidence_score': 1.0 if name and '@' in email else 0.4
        }

        contacts.append(contact)

    return contacts


def get_next_page_url(html, current_url):
    """
    Looks for a 'next page' link in the HTML.
    Returns the next page URL if found, or None if this is the last page.
    html        = the raw HTML of the current page
    current_url = the URL we are currently on
    """

    soup = BeautifulSoup(html, 'html.parser')

    # --- Strategy 1: Look for a <a> tag with "next" text ---
    next_tag = soup.find('a', string=lambda t: t and 'next' in t.lower())
    if next_tag and next_tag.get('href'):
        href = next_tag['href']
        if href.startswith('http'):
            return href
        domain = '/'.join(current_url.split('/')[:3])
        return domain + href

    # --- Strategy 2: Look for next page by class name ---
    for tag in soup.find_all('a', href=True):
        classes = tag.get('class', [])
        class_str = ' '.join(classes).lower()
        if any(word in class_str for word in ['next', 'pager-next', 'pagination-next']):
            href = tag['href']
            if href.startswith('http'):
                return href
            domain = '/'.join(current_url.split('/')[:3])
            return domain + href

    # --- Strategy 3: Detect page=N pattern in the URL ---
    page_match = re.search(r'page=(\d+)', current_url)
    if page_match:
        current_page_num = int(page_match.group(1))
        next_page_num = current_page_num + 1
        # Keep the FULL URL and only replace the page number
        next_url = re.sub(r'page=\d+', f'page={next_page_num}', current_url)
        return next_url

    # No pagination pattern found
    return None


def scrape_all_pages(start_url):
    """
    Scrapes ALL pages of a paginated site starting from start_url.
    Stops automatically after 2 consecutive empty pages.
    start_url = the first page URL
    """

    all_contacts = []       # store all contacts from all pages
    current_url = start_url
    page_number = 1
    max_pages = 20          # safety limit to avoid infinite loops
    empty_pages_count = 0   # counts consecutive empty pages

    while current_url and page_number <= max_pages:

        print(f'  📄 Scraping page {page_number}: {current_url}')

        # Download the HTML of the current page
        html = get_page_html(current_url)

        if not html:
            print(f'  ❌ Failed to load page {page_number}')
            break

        # Find contacts on this page
        contacts = find_contacts(html, start_url)
        all_contacts.extend(contacts)
        print(f'  ✅ Found {len(contacts)} contacts on page {page_number}')

        # Count consecutive empty pages
        if len(contacts) == 0:
            empty_pages_count += 1
        else:
            empty_pages_count = 0  # reset counter when page has contacts

        # Stop if 2 consecutive pages are empty
        if empty_pages_count >= 2:
            print(f'  🏁 2 consecutive empty pages — stopping pagination')
            break

        # Look for the next page
        next_url = get_next_page_url(html, current_url)

        # Wait 2 seconds before loading next page
        if next_url and next_url != current_url:
            time.sleep(2)
            current_url = next_url
            page_number += 1
        else:
            print(f'  🏁 No more pages. Stopped at page {page_number}')
            break

    return all_contacts


def get_selenium_driver():
    """
    Creates and returns a Selenium Chrome browser in hidden mode.
    We use headless mode so Chrome runs invisibly in the background.
    """

    # Configure Chrome options
    options = webdriver.ChromeOptions()

    # Run Chrome invisibly in the background (no window opens)
    options.add_argument('--headless')

    # Disable GPU (required for headless mode on Windows)
    options.add_argument('--disable-gpu')

    # Set window size so the page renders correctly
    options.add_argument('--window-size=1920,1080')

    # Pretend to be a real Chrome browser
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    # Automatically download and install the correct Chrome driver
    service = Service(ChromeDriverManager().install())

    # Create and return the browser
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def scrape_with_clicks(start_url):
    """
    Opens a page with Selenium, finds profile links, clicks each one,
    and collects name + email from each profile page.
    Used for Scenario 3 where emails are hidden behind a click.
    start_url = the main listing page URL
    """

    all_contacts = []   # store all contacts found
    driver = None       # browser starts as None

    try:
        # Start the Chrome browser
        print(f'  🌐 Opening Chrome browser...')
        driver = get_selenium_driver()

        # Open the main page
        driver.get(start_url)

        # Wait up to 10 seconds for the page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )

        # Get the page HTML and parse it
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # --- Find all profile links on the main page ---
        profile_links = []
        for tag in soup.find_all('a', href=True):
            href = tag['href']

            # Build full URL if it is a relative link
            if href.startswith('/'):
                domain = '/'.join(start_url.split('/')[:3])
                href = domain + href

            # Only keep links that look like profile pages
            if any(word in href.lower() for word in [
                '/people/', '/staff/', '/faculty/', '/person/',
                '/profile/', '/academics/', '/directory/'
            ]):
                # Avoid links that go back to the listing page
                if href != start_url and href not in profile_links:
                    profile_links.append(href)

        print(f'  🔗 Found {len(profile_links)} profile links')

        # --- Visit each profile page and collect contact info ---
        for i, profile_url in enumerate(profile_links[:50], 1):  # limit to 50 profiles

            try:
                # Open the profile page
                driver.get(profile_url)

                # Wait for the page to load
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )

                # Get the profile page HTML
                profile_soup = BeautifulSoup(driver.page_source, 'html.parser')

                # --- Find email on this profile page ---
                email = ''

                # Strategy 1: look for mailto link
                mailto = profile_soup.find('a', href=lambda h: h and h.startswith('mailto:'))
                if mailto:
                    email = mailto['href'].replace('mailto:', '').strip().lower()

                # Strategy 2: look for email in plain text
                if not email:
                    page_text = profile_soup.get_text()
                    email_match = re.search(
                        r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+',
                        page_text
                    )
                    if email_match:
                        email = email_match.group(0).lower()

                # --- Find name on this profile page ---
                name = ''

                # Words that indicate a cookie popup or non-name h1
                skip_words = [
                    'cookie', 'privacy', 'consent', 'your choice',
                    'we use', 'accept', 'policy'
                ]

                # Look through ALL h1 tags and skip cookie-related ones
                for h1 in profile_soup.find_all('h1'):
                    h1_text = h1.get_text(strip=True)
                    if not any(word in h1_text.lower() for word in skip_words):
                        name = h1_text
                        break  # use the first valid h1 found

                # --- Clean the name ---
                # Remove titles at the START of the name
                name = re.sub(r'^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss)\s+', '', name)

                # Remove credentials at the END of the name
                name = re.sub(r'(BSc|MSc|PhD|BA|MA|DSc|FHEA|FBA|CPsychol|FBPsS|C\.Psychol).*$', '', name)

                # Remove leftover commas and special characters
                name = re.sub(r'[,»]+', '', name)

                # Remove extra spaces
                name = re.sub(r'\s+', ' ', name).strip()

                # Use email username as fallback if no name found
                if not name and email:
                    username = email.split('@')[0]
                    name = username.replace('.', ' ').replace('-', ' ').title()

                # Only save if we found at least an email
                if email and '@' in email:

                    # Extract university from URL
                    domain = start_url.split('/')[2]
                    university = domain.replace('www.', '').split('.')[1].title()

                    contact = {
                        'full_name':        name,
                        'university_name':  university,
                        'department':       'Psychology',
                        'department_url':   start_url,
                        'email':            email,
                        'research_lines':   '',
                        'confidence_score': 1.0 if name and email else 0.5
                    }
                    all_contacts.append(contact)
                    print(f'  ✅ [{i}] {name} → {email}')
                else:
                    print(f'  ⚠️  [{i}] No email found on: {profile_url}')

                # Wait 1 second between profile pages to be polite
                time.sleep(1)

            except Exception as e:
                # If one profile fails, skip it and continue
                print(f'  ❌ [{i}] Failed to load profile: {e}')
                continue

    except Exception as e:
        print(f'  ❌ Selenium error: {e}')

    finally:
        # Always close the browser when done
        if driver:
            driver.quit()
            print(f'  🔒 Browser closed')

    return all_contacts


def log_error(url, error_type, error_message, retry_count, filepath):
    """
    Saves a failed URL to the errors_log.csv file.
    url           = the URL that failed
    error_type    = TIMEOUT, NOT_FOUND, NO_EMAIL, BLOCKED, UNKNOWN
    error_message = the detailed error message
    retry_count   = how many times we tried before giving up
    filepath      = path to the errors_log.csv file
    """

    # Check if the file already exists to decide if we need to write the header
    file_exists = os.path.isfile(filepath)

    # Open the file in append mode (add new rows without deleting old ones)
    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'url', 'error_type', 'error_message', 'timestamp', 'retry_count'
        ])

        # Write the header only if the file is new
        if not file_exists:
            writer.writeheader()

        # Write the error row
        writer.writerow({
            'url':           url,
            'error_type':    error_type,
            'error_message': error_message,
            'timestamp':     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'retry_count':   retry_count
        })


def scrape_url_safe(url, errors_log_path):
    """
    Safely scrapes a single URL handling all possible errors.
    Automatically detects which scenario the URL needs.
    url             = the URL to scrape
    errors_log_path = path to save errors_log.csv
    """

    print(f'\n🔍 Scraping: {url}')

    try:
        # --- First try with simple requests (fast) ---
        html = get_page_html(url)

        # If requests failed, try with Selenium (slower but more powerful)
        if not html:
            print(f'  ⚠️  requests failed — trying Selenium...')
            contacts = scrape_with_clicks(url)

            if not contacts:
                log_error(
                    url=url,
                    error_type='BLOCKED',
                    error_message='Both requests and Selenium failed to load the page',
                    retry_count=3,
                    filepath=errors_log_path
                )
                return []

            return contacts

        # --- requests worked — check if emails exist on this page ---
        contacts = find_contacts(html, url)

        if contacts:
            # Check if there are more pages (pagination)
            next_url = get_next_page_url(html, url)

            if next_url and next_url != url:
                # Scenario 2: has pagination
                print(f'  📄 Pagination detected — scraping all pages...')
                contacts = scrape_all_pages(url)
            else:
                # Scenario 1: no pagination
                print(f'  ✅ Found {len(contacts)} contacts (no pagination)')

        else:
            # Scenario 3: no emails visible — try clicking links
            print(f'  ⚠️  No emails on page — trying profile links with Selenium...')
            contacts = scrape_with_clicks(url)

            if not contacts:
                log_error(
                    url=url,
                    error_type='NO_EMAIL',
                    error_message='Page loaded but no emails found after clicking links',
                    retry_count=1,
                    filepath=errors_log_path
                )
                return []

        return contacts

    except Exception as e:
        print(f'  ❌ Unexpected error: {e}')
        log_error(
            url=url,
            error_type='UNKNOWN',
            error_message=str(e),
            retry_count=1,
            filepath=errors_log_path
        )
        return []


# --- TEST: run this file directly to see if it works ---
if __name__ == '__main__':

    # Find the correct paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(BASE_DIR, '..', 'data', 'links.xlsx')
    errors_log_path = os.path.join(BASE_DIR, '..', 'data', 'errors_log.csv')

    # Step 1: load the URLs
    urls = load_urls(filepath)
    print(f'✅ Step 1 OK — Total URLs loaded: {len(urls)}')
    print()

    # Test with 3 different scenarios
    test_urls = [
        urls[10],   # Yale       → Scenario 1 (emails visible)
        urls[69],   # Pittsburgh → Scenario 2 (pagination)
        urls[61],   # Birmingham → Scenario 3 (click links)
    ]

    all_contacts = []

    for test_url in test_urls:
        contacts = scrape_url_safe(test_url, errors_log_path)
        all_contacts.extend(contacts)
        print(f'  → Collected {len(contacts)} contacts from this URL')

    print()
    print(f'✅ Total contacts collected: {len(all_contacts)}')

    if os.path.isfile(errors_log_path):
        print(f'📋 errors_log.csv exists — some URLs had errors')
    else:
        print(f'✅ No errors logged — all URLs worked!')