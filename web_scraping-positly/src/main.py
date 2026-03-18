# main.py
# Job: runs the FULL pipeline in one command
#
# Phase 0 — link_collector: reads universities.csv → finds department URLs → saves links.xlsx
# Phase 1 — scraper:        collects emails from all URLs
# Phase 2 — cleaner:        cleans the raw data
# Phase 3 — save:           saves final contacts_output.csv

import os
from link_collector import get_university_names
from link_collector import search_psychology_department
from link_collector import find_people_links
from link_collector import save_links_to_excel
from scraper import load_urls
from scraper import scrape_url_safe
from cleaner import clean_contacts
from cleaner import save_to_csv


def run_pipeline(universities_filepath, links_filepath, output_filepath, errors_log_filepath):

    print('=' * 60)
    print('  🚀 FULL PIPELINE STARTING')
    print('=' * 60)
    print()

    # ─────────────────────────────────────────
    # PHASE 0 — FIND LINKS AUTOMATICALLY
    # ─────────────────────────────────────────

    print('📍 PHASE 0 — Reading universities and finding department URLs...')
    print()

    # Step 0.1: Read university names from CSV
    names = get_university_names(universities_filepath)
    print(f'✅ Universities loaded: {len(names)}')
    print()

    # Step 0.2 + 0.3: For each university, find department URL and people links
    all_new_links = []

    for name in names[:5]:  # ← mude para names para rodar todas
        print(f'  🏛️  {name}')

        dept_url = search_psychology_department(name)

        if not dept_url:
            print(f'  ❌ Department not found')
            print()
            continue

        print(f'  🔗 {dept_url}')

        people_links = find_people_links(dept_url)
        print(f'  👥 People links: {len(people_links)}')

        all_new_links.extend(people_links)
        print()

    # Step 0.4: Save all links to links.xlsx
    save_links_to_excel(all_new_links, links_filepath)

    print()
    print('=' * 60)
    print(f'✅ PHASE 0 DONE — Links saved to links.xlsx')
    print('=' * 60)
    print()

    # ─────────────────────────────────────────
    # PHASE 1 — EXTRACT
    # ─────────────────────────────────────────

    print('📍 PHASE 1 — Scraping emails from all URLs...')
    print()

    urls = load_urls(links_filepath)
    total_urls = len(urls)
    print(f'📋 Total URLs to scrape: {total_urls}')
    print()

    all_contacts = []

    for i, url in enumerate(urls, 1):

        print(f'[{i}/{total_urls}] ', end='')

        contacts = scrape_url_safe(url, errors_log_filepath)
        all_contacts.extend(contacts)

        print(f'  → Total so far: {len(all_contacts)} contacts')
        print()

    print('=' * 60)
    print(f'✅ PHASE 1 DONE — Raw contacts collected: {len(all_contacts)}')
    print('=' * 60)
    print()

    # ─────────────────────────────────────────
    # PHASE 2 — TRANSFORM
    # ─────────────────────────────────────────

    print('📍 PHASE 2 — Cleaning the data...')
    print()

    clean = clean_contacts(all_contacts)

    print()
    print('=' * 60)
    print(f'✅ PHASE 2 DONE — Clean contacts: {len(clean)}')
    print('=' * 60)
    print()

    # ─────────────────────────────────────────
    # PHASE 3 — LOAD
    # ─────────────────────────────────────────

    print('📍 PHASE 3 — Saving final CSV...')
    print()

    save_to_csv(clean, output_filepath)

    print()
    print('=' * 60)
    print('  🏆 PIPELINE COMPLETE!')
    print(f'  📊 Total contacts saved: {len(clean)}')
    print(f'  📁 File: {output_filepath}')

    if os.path.isfile(errors_log_filepath):
        print(f'  ⚠️  Some URLs failed — check: {errors_log_filepath}')
    else:
        print(f'  ✅ No errors!')

    print('=' * 60)


if __name__ == '__main__':

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    universities_filepath = os.path.join(BASE_DIR, '..', 'data', 'universities.csv')
    links_filepath        = os.path.join(BASE_DIR, '..', 'data', 'links.xlsx')
    output_filepath       = os.path.join(BASE_DIR, '..', 'data', 'contacts_output.csv')
    errors_log_filepath   = os.path.join(BASE_DIR, '..', 'data', 'errors_log.csv')

    run_pipeline(universities_filepath, links_filepath, output_filepath, errors_log_filepath)