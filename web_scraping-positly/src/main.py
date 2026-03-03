# main.py
# Job: connects everything and runs the full ETL pipeline
# E = Extract  → scraper.py collects contacts from all URLs
# T = Transform → cleaner.py cleans the raw data
# L = Load      → saves final contacts_output.csv

import os                          # library to work with file paths
from scraper import load_urls      # import our function to read links.xlsx
from scraper import scrape_url_safe  # import our function to scrape each URL
from cleaner import clean_contacts   # import our function to clean the data
from cleaner import save_to_csv      # import our function to save the CSV


def run_pipeline(urls_filepath, output_filepath, errors_log_filepath):
    """
    Runs the full ETL pipeline:
    1. Reads all URLs from links.xlsx
    2. Scrapes each URL collecting contacts
    3. Cleans all collected contacts
    4. Saves final CSV file
    urls_filepath       = path to links.xlsx
    output_filepath     = path to save contacts_output.csv
    errors_log_filepath = path to save errors_log.csv
    """

    print('=' * 60)
    print('  🚀 STARTING WEB SCRAPING PIPELINE')
    print('=' * 60)
    print()

    # ─────────────────────────────────────────
    # STEP E — EXTRACT
    # ─────────────────────────────────────────

    # Load all URLs from links.xlsx
    # urls = load_urls(urls_filepath)
    urls = load_urls(urls_filepath)[:5]
    total_urls = len(urls)
    print(f'📋 Total URLs to scrape: {total_urls}')
    print()

    # List to store ALL contacts from ALL URLs
    all_contacts = []

    # Loop through each URL one by one
    for i, url in enumerate(urls, 1):

        print(f'[{i}/{total_urls}] ', end='')  # show progress like [1/82]

        # Scrape the URL safely (handles all 3 scenarios + errors)
        contacts = scrape_url_safe(url, errors_log_filepath)

        # Add contacts from this URL to our main list
        all_contacts.extend(contacts)

        print(f'  → Total so far: {len(all_contacts)} contacts')
        print()

    print('=' * 60)
    print(f'✅ EXTRACTION DONE — Raw contacts collected: {len(all_contacts)}')
    print('=' * 60)
    print()

    # ─────────────────────────────────────────
    # STEP T — TRANSFORM
    # ─────────────────────────────────────────

    print('🧹 Cleaning the data...')
    print()

    clean = clean_contacts(all_contacts)

    print()
    print('=' * 60)
    print(f'✅ CLEANING DONE — Clean contacts: {len(clean)}')
    print('=' * 60)
    print()

    # ─────────────────────────────────────────
    # STEP L — LOAD
    # ─────────────────────────────────────────

    print('💾 Saving final CSV file...')
    print()

    save_to_csv(clean, output_filepath)

    print()
    print('=' * 60)
    print('  🏆 PIPELINE COMPLETE!')
    print(f'  📊 Total contacts saved: {len(clean)}')
    print(f'  📁 Output file: {output_filepath}')

    # Check if any errors were logged
    if os.path.isfile(errors_log_filepath):
        print(f'  ⚠️  Some URLs failed — check: {errors_log_filepath}')
    else:
        print(f'  ✅ No errors — all URLs scraped successfully!')

    print('=' * 60)


# --- MAIN ENTRY POINT ---
if __name__ == '__main__':

    # Find the correct paths automatically
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Input file
    urls_filepath = os.path.join(BASE_DIR, '..', 'data', 'links.xlsx')

    # Output files
    output_filepath = os.path.join(BASE_DIR, '..', 'data', 'contacts_output.csv')
    errors_log_filepath = os.path.join(BASE_DIR, '..', 'data', 'errors_log.csv')

    # Run the full pipeline
    run_pipeline(urls_filepath, output_filepath, errors_log_filepath)