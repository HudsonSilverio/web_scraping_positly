# scraper.py
# Step 1: Read and clean the URLs from links.xlsx
# Step 2: Open a URL and download its HTML
# Step 3: Find emails inside the HTML
# Step 4: Find names + emails together (contacts)
# Step 5: Handle pagination (multiple pages)

import pandas as pd             # library to read Excel files
import os                       # library to work with file paths
import time                     # library to add waiting time between requests
import requests                 # library to open URLs and download HTML
import re                       # library to find patterns in text
from bs4 import BeautifulSoup   # library to read HTML structure


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
        next_url = re.sub(r'page=\d+', f'page={next_page_num}', current_url)

        # Stop if page has no results
        no_results_signs = [
            'no results', 'no people found',
            'no records', '0 results', 'nothing found'
        ]
        page_text = soup.get_text().lower()
        if any(sign in page_text for sign in no_results_signs):
            return None

        return next_url

    return None


def scrape_all_pages(start_url):
    """
    Scrapes ALL pages of a paginated site starting from start_url.
    Returns a list of all contacts found across all pages.
    start_url = the first page URL
    """

    all_contacts = []   # store all contacts from all pages
    current_url = start_url
    page_number = 1
    max_pages = 20      # safety limit to avoid infinite loops

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


# --- TEST: run this file directly to see if it works ---
if __name__ == '__main__':

    # Find the correct path to links.xlsx
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(BASE_DIR, '..', 'data', 'links.xlsx')

    # Step 1: load the URLs
    urls = load_urls(filepath)
    print(f'✅ Step 1 OK — Total URLs loaded: {len(urls)}')
    print()

    # Step 5 TEST: Pittsburgh has pagination and does not block requests
    test_url = urls[69]  # Pittsburgh
    print(f'📄 Step 5 — Testing pagination with: {test_url}')
    print()

    all_contacts = scrape_all_pages(test_url)

    print()
    print(f'✅ Step 5 OK — Total contacts found across all pages: {len(all_contacts)}')
    print()
    print('First 3 contacts:')
    for c in all_contacts[:3]:
        print(f'   Name:  {c["full_name"]}')
        print(f'   Email: {c["email"]}')
        print(f'   ────────────────────────────')