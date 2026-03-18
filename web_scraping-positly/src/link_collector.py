# link_collector.py
# Phase 0: Automatically finds psychology department URLs
# and saves them to data/links.xlsx
#
# Step 1: Read university names from data/universities.csv
# Step 2: Look up URL from data/known_urls.json first, then DuckDuckGo fallback
# Step 3: Find People/Staff/Faculty subdivision links inside the page
# Step 4: Add new URLs to links.xlsx

import os
import json
import time
import re
import requests
import pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from ddgs import DDGS
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def load_known_urls():
    """Loads known_urls.json and returns a flat dict {university_name: url}."""
    base_dir  = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, '..', 'data', 'known_urls.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    flat = {}
    for region, entries in data.items():
        if isinstance(entries, dict):
            flat.update(entries)
    return flat

KNOWN_PSYCHOLOGY_URLS = load_known_urls()


def get_selenium_driver():
    """Creates and returns a hidden Chrome browser."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


# =============================================================================
# Step 1: Read university names from data/universities.csv
# =============================================================================
def get_university_names(filepath):
    """
    Reads the universities.csv file and returns a clean list of names.
    filepath = path to universities.csv
    """
    df = pd.read_csv(filepath, encoding='utf-8')
    df.columns = [c.strip() for c in df.columns]
    df = df[['university_name']]
    df['university_name'] = df['university_name'].astype(str).str.strip()
    df = df[df['university_name'] != '']
    df = df.drop_duplicates(subset='university_name')
    df = df.reset_index(drop=True)

    names = df['university_name'].tolist()
    print(f'  ✅ Loaded {len(names)} universities from CSV')
    return names


# =============================================================================
# Step 2: Find the psychology department URL
# =============================================================================
def find_in_dictionary(university_name):
    """
    Looks up the psychology department URL from KNOWN_PSYCHOLOGY_URLS.
    Tries exact match first, then partial keyword match.
    Returns URL string or None.
    """
    if university_name in KNOWN_PSYCHOLOGY_URLS:
        return KNOWN_PSYCHOLOGY_URLS[university_name]

    noise = {'university', 'of', 'the', 'and', 'institute', 'technology',
             'college', 'school', 'national', 'at', 'in', 'for'}

    name_lower = university_name.lower()

    for key, url in KNOWN_PSYCHOLOGY_URLS.items():
        key_words = [w for w in key.lower().split() if w not in noise and len(w) > 3]
        if key_words and all(w in name_lower for w in key_words[:2]):
            return url

    return None


def score_ddg_result(url, title, university_name):
    """
    Score a DuckDuckGo result for how likely it is to be
    the correct psychology department people/faculty page.

    Rules derived from analysis of 415 verified URLs (verde.xlsx):
      40.7% have /people in path
      34.0% have /faculty in path
      17.1% have /staff in path
       9.2% have /graduate in path
       4.3% have /academic in path
      30.6% have psychology. as domain prefix
      42.9% have psych anywhere in domain
      51.3% are .edu domains
      13.0% are .ac.uk domains
       9.2% are .ca domains
       4.8% are .nl domains
    """
    score = 0
    parsed  = urlparse(url)
    netloc  = parsed.netloc.lower()
    path    = parsed.path.lower()
    title_l = title.lower()

    # ─────────────────────────────────────────
    # DOMAIN SIGNALS
    # ─────────────────────────────────────────
    if any(netloc.startswith(p) for p in ['psychology.', 'psych.', 'psy.', 'psychol.']):
        score += 5
    elif any(k in netloc for k in [
        'psychology', 'psychandneuro', 'psychologicalsciences', 'psychologie'
    ]):
        score += 4
    elif 'psych' in netloc or '.psy.' in netloc:
        score += 3

    # ─────────────────────────────────────────
    # ACADEMIC TLD SIGNALS
    # ─────────────────────────────────────────
    academic_tlds = [
        '.edu',       # 51.3%
        '.ac.uk',     # 13.0%
        '.ca',        # 9.2%
        '.nl',        # 4.8%
        '.edu.au',    # 4.3%
        '.es',        # 2.9%
        '.se',        # 2.2%
        '.edu.cn',    # 1.4%
        '.edu.hk',    # 1.0%
        '.ac.nz',     # 1.0%
        '.de',        # 1.0%
        '.edu.sg',    # 0.2%
        '.dk',        # 0.2%
        '.no',        # 0.2%
        '.fi',        # 0.2%
        '.be',        # 0.5%
        '.ch',        # 0.5%
        '.ac.kr',
        '.ac.jp',
    ]
    if any(netloc.endswith(t) for t in academic_tlds):
        score += 1

    # ─────────────────────────────────────────
    # UNIVERSITY NAME IN DOMAIN
    # ─────────────────────────────────────────
    uni_words = [
        w.lower() for w in re.split(r'[\W_]+', university_name)
        if len(w) > 3 and w.lower() not in
        {'university', 'institute', 'technology', 'college', 'school',
         'national', 'sciences', 'science', 'arts'}
    ]
    if any(w in netloc for w in uni_words[:3]):
        score += 2

    # ─────────────────────────────────────────
    # PATH SIGNALS
    # ─────────────────────────────────────────
    if '/people' in path:
        score += 3      # 40.7% of verified URLs
    if '/faculty' in path:
        score += 3      # 34.0% of verified URLs
    if '/staff' in path or '/stafflist' in path:
        score += 2      # 17.1% of verified URLs
    if '/graduate' in path:
        score += 2      # 9.2% of verified URLs
    if '/academic' in path:
        score += 1      # 4.3% of verified URLs
    if '/doctoral' in path:
        score += 1      # 2.7% of verified URLs
    if any(k in path for k in [
        '/directory', '/persons', '/our-people',
        '/researchers', '/profiles', '/team',
        '/mitarbeiterliste', '/postdoc', '/fellows',
        '/emeritus', '/lecturers', '/affiliated',
        '/visiting', '/adjunct', '/instructors',
    ]):
        score += 2

    # ─────────────────────────────────────────
    # TITLE SIGNALS
    # ─────────────────────────────────────────
    if 'psychology' in title_l:
        score += 2
    if any(k in title_l for k in ['faculty', 'staff', 'people', 'department of']):
        score += 1

    # ─────────────────────────────────────────
    # HARD PENALTIES
    # ─────────────────────────────────────────
    bad_patterns = [
        'topuniversities', 'ranking', 'wikipedia', 'linkedin',
        'facebook', 'twitter', 'researchgate', 'academia.edu',
        'glassdoor', 'indeed', 'archived', 'wayback',
        'studyportals', 'mastersportal', 'hotcourses',
        'scholarshipdb', 'collegeboard', 'niche.com',
        'ratemyprofessors', 'unigo', 'cappex',
        'medschool', '/medicine/', '/nursing/', '/pharmacy/',
        '/psychiatry/', 'psychiatry.', 'pediatrics.',
        '/news/', '/blog/', '/press/', '/events/',
    ]
    if any(bad in url.lower() for bad in bad_patterns):
        score -= 15

    return score


def search_psychology_department(university_name):
    """
    Step 1: Check known_urls.json (fast, verified).
    Step 2: DuckDuckGo fallback — score top 5 results and pick best.
    Returns URL or None.
    """
    known_url = find_in_dictionary(university_name)
    if known_url:
        print(f'  [JSON] {known_url}')
        return known_url

    print(f'  [DDG] Searching...')

    search_queries = [
        f'"{university_name}" psychology department people faculty staff',
        f'"{university_name}" psychology department people',
        f'"{university_name}" psychology faculty directory',
    ]

    best_url   = None
    best_score = -99

    for query in search_queries:
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=5))

            for result in results:
                url   = result.get('href', '')
                title = result.get('title', '')
                if not url:
                    continue
                s = score_ddg_result(url, title, university_name)
                print(f'    score={s:+d}  {url[:75]}')
                if s > best_score:
                    best_score = s
                    best_url   = url

            time.sleep(2)

            if best_score >= 7:
                break

        except Exception as e:
            print(f'  Search error: {e}')
            time.sleep(3)

    if best_url and best_score > 0:
        print(f'  [DDG] Best (score={best_score}): {best_url}')
        return best_url

    print(f'  No good result found (best score={best_score})')
    return None


# =============================================================================
# Step 3: Find People/Staff/Faculty sub-links inside the department page
# =============================================================================
def find_people_links(department_url):

    url_keywords = [
        'people', 'staff', 'faculty', 'team', 'directory',
        'members', 'researchers', 'personnel',
        'postdoc', 'fellows', 'administration', 'emeritus',
        'lecturers', 'visitors', 'affiliated',
        'instructors', 'associates', 'historical', 'honorary',
        'our-people', 'our-team', 'find-an-expert', 'persons',
        'teaching-staff', 'research-staff', 'academic-staff',
        'teaching-assistants', 'research-fellows',
        'phd-students', 'doctoral-students', 'postdoctoral',
        'adjunct', 'standing-faculty', 'core-faculty',
        'joint-faculty', 'primary-faculty', 'secondary-faculty',
        'instructional', 'appointed-faculty', 'affiliate',
        'graduate-students', 'administrative-staff',
        'postdoctoral-scholars', 'visiting-scholars',
        'emeriti', 'affiliated-faculty', 'all-faculty',
        'faculty-members', 'teaching-research-staff',
        'postdoctoral-fellows', 'postdoctoral-researchers',
        'mitarbeiterliste', 'stafflist',
    ]

    text_keywords = [
        'faculty', 'staff', 'people', 'team', 'directory',
        'researchers', 'postdoc', 'fellows', 'graduate students',
        'lecturers', 'administration', 'emeritus', 'personnel',
        'our people', 'our team', 'academic staff', 'research staff',
        'teaching staff', 'phd students', 'doctoral students',
        'visiting scholars', 'affiliated', 'adjunct', 'honorary',
        'instructors', 'associates', 'all members',
        'teaching assistants', 'research fellows', 'all faculty',
    ]

    bad_segments = [
        '/news', '/events', '/event/',
        '/apply', '/giving', '/alumni',
        '/intranet', '/login', '/signin', '/signup',
        '/cookie', '/privacy', '/contact-us',
        '/courses', '/undergraduate',
        '/degree-requirements', '/forms', '/diploma',
        '/concern', '/campus-offices', '/minor',
        '/labvacs', '/what-happens', '/what-makes',
        '/epadmissions', '/dphil', '/msc',
        '/part-time-dphil', '/part-time-msc',
        '/funding', '/fees', '/entry',
        '/career-destinations', '/admissions',
        '/study/', '/roleaffiliation/', '/academics/',
        '/community-resources/', '/about-us', '/about/',
        '/research-areas', '/research/', '/seminars', '/workshops',
        '/jobs', '/positions', '/outreach', '/graduate-programs',
        '/admin-units/', '/benefits-leaves/', '/payroll/',
        '/information-technology/', '/operations/', '/centers-programs/',
        '/higher-values/', '/da-updates', '/featured-content/',
        # UCLA noise
        '/prospective-', '/postdocs/contacts', '/postdocs/resources',
        '/postdocs/awards', '/postdocs/equity', '/staff-employment',
        # Toronto noise
        '/current-program-students', '/guidance-undergraduate',
        '/ta-excellence', '/important-dates',
        # Duke / Penn noise
        '/faculty-mentorship', '/faculty-research-labs',
        '/participate-research', '/interdisciplinary-opportunities',
        # MIT noise
        '/give-', '/champions-brain',
        # Berkeley noise
        '/resources/faculty-staff', '/resources/hr-',
        # CMU noise
        '/spur/',
        '?random=', 'format=json', 'occ_id=',
    ]

    listing_segments = [
        'people', 'staff', 'faculty', 'team', 'directory',
        'members', 'researchers', 'personnel', 'postdoc',
        'fellows', 'administration', 'emeritus', 'lecturers',
        'visitors', 'affiliated', 'graduate', 'instructors',
        'associates', 'historical', 'honorary', 'our-people',
        'our-team', 'persons', 'teaching-staff', 'research-staff',
        'academic-staff', 'teaching-assistants', 'research-fellows',
        'phd-students', 'doctoral-students', 'postdoctoral', 'adjunct',
        'standing-faculty', 'core-faculty', 'joint-faculty',
        'primary-faculty', 'secondary-faculty', 'instructional',
        'appointed-faculty', 'affiliate', 'all-faculty',
        'graduate-students', 'administrative-staff',
        'postdoctoral-scholars', 'visiting-scholars', 'emeriti',
        'affiliated-faculty', 'faculty-members', 'teaching-research-staff',
        'postdoctoral-fellows', 'postdoctoral-researchers',
        'all-staff', 'core', 'clinical', 'social',
        'cognitive', 'developmental', 'neuroscience',
        'stafflist', 'mitarbeiterliste',
    ]

    found_links = []
    domain = '/'.join(department_url.split('/')[:3])

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    def is_individual_profile(href):
        if '?tab=' in href or '?type=' in href or '?filter=' in href:
            return False
        if '#' in href:
            return False
        path = href.replace(domain, '').lower().split('?')[0].split('#')[0]
        segments = [s for s in path.rstrip('/').split('/') if s]
        if not segments:
            return False
        last = segments[-1]
        if last in listing_segments:
            return False
        listing_parents = ['people', 'faculty', 'staff', 'team', 'directory']
        for i, seg in enumerate(segments):
            if seg in listing_parents and i + 1 < len(segments):
                child = segments[i + 1]
                if child not in listing_segments and child != '':
                    return True
        return False

    try:
        response = requests.get(department_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            text = tag.get_text(strip=True).lower()

            if href.startswith('/'):
                full_href = domain + href
            elif href.startswith('http'):
                full_href = href
            else:
                continue

            if domain not in full_href:
                continue
            if any(bad in full_href.lower() for bad in bad_segments):
                continue
            if is_individual_profile(full_href):
                continue

            path = full_href.replace(domain, '').lower()
            url_match  = any(kw in path for kw in url_keywords)
            text_match = any(kw in text for kw in text_keywords)

            if url_match or text_match:
                if full_href not in found_links:
                    found_links.append(full_href)

        if not found_links:
            found_links.append(department_url)

    except Exception as e:
        print(f'    Could not open {department_url}: {e}')
        found_links.append(department_url)

    return found_links


# =============================================================================
# Step 4: Save links to links.xlsx
# =============================================================================
def save_links_to_excel(new_links, filepath):
    existing_urls = []

    if os.path.isfile(filepath):
        df_existing = pd.read_excel(filepath, header=None)
        df_existing.columns = ['url']
        existing_urls = df_existing['url'].astype(str).tolist()
        print(f'  Found {len(existing_urls)} existing URLs in links.xlsx')

    all_urls = existing_urls + new_links
    all_urls = list(dict.fromkeys(all_urls))

    df = pd.DataFrame(all_urls, columns=['url'])
    df.to_excel(filepath, index=False, header=False)

    added = len(all_urls) - len(existing_urls)
    print(f'  Added {added} new URLs - Total in file: {len(all_urls)}')


# =============================================================================
# MAIN
# =============================================================================
if __name__ == '__main__':

    print('=' * 60)
    print('  LINK COLLECTOR - Phase 0')
    print('=' * 60)
    print()

    BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
    links_path        = os.path.join(BASE_DIR, '..', 'data', 'links.xlsx')
    universities_path = os.path.join(BASE_DIR, '..', 'data', 'universities.csv')

    names = get_university_names(universities_path)
    print(f'Step 1 OK - Total universities: {len(names)}')
    print()

    print('Step 2+3 - Finding department URLs and people links...')
    print()

    all_new_links = []

    for name in names[:5]:  # ← mude para names para rodar todas
        print(f'  {name}')

        dept_url = search_psychology_department(name)

        if not dept_url:
            print(f'  Department not found')
            print()
            continue

        print(f'  URL: {dept_url}')

        people_links = find_people_links(dept_url)
        print(f'  People links: {len(people_links)}')
        for link in people_links:
            print(f'    -> {link}')

        all_new_links.extend(people_links)
        print()

    print('Step 4 - Saving to links.xlsx...')
    save_links_to_excel(all_new_links, links_path)
    print()
    print('=' * 60)
    print(f'  DONE! Total links collected: {len(all_new_links)}')
    print('=' * 60)