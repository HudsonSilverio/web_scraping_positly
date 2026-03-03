# cleaner.py
# Job: clean the raw contacts collected by scraper.py
# - Remove duplicates
# - Remove contacts with no email
# - Fix dirty names
# - Validate email format
# - Save final contacts_output.csv

import pandas as pd   # library to work with tables of data
import re             # library to find and fix patterns in text
import os             # library to work with file paths


def clean_contacts(contacts):
    """
    Receives a list of raw contacts from scraper.py
    and returns a clean list ready to save to CSV.
    contacts = list of dictionaries with contact info
    """

    # If no contacts were collected, return empty list
    if not contacts:
        print('  ⚠️  No contacts to clean')
        return []

    # Convert the list of dictionaries into a pandas DataFrame (like a table)
    df = pd.DataFrame(contacts)

    print(f'  📊 Contacts before cleaning: {len(df)}')

    # --- Step 1: Remove contacts with no email ---
    # A contact with no email is useless for our project
    df = df[df['email'].notna()]           # remove rows where email is None
    df = df[df['email'].str.strip() != ''] # remove rows where email is empty
    print(f'  ✅ After removing empty emails: {len(df)}')

    # --- Step 2: Validate email format ---
    # Keep only emails that match the pattern: something@something.something
    email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    df = df[df['email'].str.match(email_pattern)]
    print(f'  ✅ After validating email format: {len(df)}')

    # --- Step 3: Remove duplicate emails ---
    # Same email = same person, keep only the first occurrence
    df = df.drop_duplicates(subset='email', keep='first')
    print(f'  ✅ After removing duplicate emails: {len(df)}')

    # --- Step 4: Clean the full_name column ---
    # Remove extra spaces
    df['full_name'] = df['full_name'].str.replace(r'\s+', ' ', regex=True).str.strip()

    # Remove leftover symbols like » , ; |
    df['full_name'] = df['full_name'].str.replace(r'[»,;|]+', '', regex=True).str.strip()

    # Remove academic titles at the start
    df['full_name'] = df['full_name'].str.replace(
        r'^(Professor|Prof\.|Prof|Dr\.|Dr|Mr\.|Mr|Mrs\.|Mrs|Ms\.|Ms|Miss)\s+',
        '', regex=True
    )

    # Remove credentials at the end
    df['full_name'] = df['full_name'].str.replace(
        r'\s*(BSc|MSc|PhD|BA|MA|DSc|FHEA|FBA|CPsychol|FBPsS|MD|MPH).*$',
        '', regex=True
    )

    # Remove extra spaces again after all replacements
    df['full_name'] = df['full_name'].str.replace(r'\s+', ' ', regex=True).str.strip()

    # --- Step 5: Fill missing names with email username ---
    # Example: john.smith@yale.edu → John Smith
    def fix_empty_name(row):
        if not row['full_name'] or row['full_name'].strip() == '':
            username = row['email'].split('@')[0]
            return username.replace('.', ' ').replace('-', ' ').title()
        return row['full_name']

    df['full_name'] = df.apply(fix_empty_name, axis=1)

    # --- Step 6: Clean the email column ---
    # Make all emails lowercase
    df['email'] = df['email'].str.lower().str.strip()

    # --- Step 7: Fill missing values in other columns ---
    df['university_name'] = df['university_name'].fillna('Unknown')
    df['department']      = df['department'].fillna('Psychology')
    df['research_lines']  = df['research_lines'].fillna('')
    df['confidence_score'] = df['confidence_score'].fillna(0.4)

    # --- Step 8: Sort by university name then by full name ---
    df = df.sort_values(
        by=['university_name', 'full_name']
    ).reset_index(drop=True)

    print(f'  ✅ Final clean contacts: {len(df)}')

    # Convert back to list of dictionaries
    return df.to_dict(orient='records')


def save_to_csv(contacts, filepath):
    """
    Saves the clean contacts to a CSV file.
    contacts = list of clean contact dictionaries
    filepath = path where the CSV file will be saved
    """

    if not contacts:
        print('  ⚠️  No contacts to save')
        return

    # Convert to DataFrame
    df = pd.DataFrame(contacts)

    # Make sure columns are in the correct order
    columns = [
        'full_name',
        'university_name',
        'department',
        'department_url',
        'email',
        'research_lines',
        'confidence_score'
    ]
    df = df[columns]

    # Save to CSV file
    # index=False means we don't save the row numbers
    # encoding='utf-8-sig' makes the file open correctly in Excel
    df.to_csv(filepath, index=False, encoding='utf-8-sig')

    print(f'  💾 Saved {len(df)} contacts to: {filepath}')


# --- TEST: run this file directly to see if it works ---
if __name__ == '__main__':

    # Find the correct paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(BASE_DIR, '..', 'data', 'contacts_output.csv')

    # Create some fake dirty contacts to test the cleaner
    test_contacts = [
        {
            'full_name':        'Professor John Smith PhD',
            'university_name':  'Yale',
            'department':       'Psychology',
            'department_url':   'https://psychology.yale.edu',
            'email':            'john.smith@yale.edu',
            'research_lines':   '',
            'confidence_score': 1.0
        },
        {
            'full_name':        'Professor John Smith PhD',  # duplicate
            'university_name':  'Yale',
            'department':       'Psychology',
            'department_url':   'https://psychology.yale.edu',
            'email':            'john.smith@yale.edu',       # same email
            'research_lines':   '',
            'confidence_score': 1.0
        },
        {
            'full_name':        'Sally AdamsBSc (hons), FHEA',
            'university_name':  'Birmingham',
            'department':       'Psychology',
            'department_url':   'https://birmingham.ac.uk',
            'email':            'NOT_AN_EMAIL',              # invalid email
            'research_lines':   '',
            'confidence_score': 0.4
        },
        {
            'full_name':        '',                          # empty name
            'university_name':  'Pittsburgh',
            'department':       'Psychology',
            'department_url':   'https://pitt.edu',
            'email':            'mary.jones@pitt.edu',
            'research_lines':   '',
            'confidence_score': 1.0
        },
        {
            'full_name':        'Dr.   Maria   Gendron »',  # dirty name
            'university_name':  'Yale',
            'department':       'Psychology',
            'department_url':   'https://psychology.yale.edu',
            'email':            'maria.gendron@yale.edu',
            'research_lines':   '',
            'confidence_score': 1.0
        },
    ]

    print('🧹 Testing cleaner.py...')
    print()

    # Clean the test contacts
    clean = clean_contacts(test_contacts)

    print()
    print('📋 Result after cleaning:')
    print()
    for c in clean:
        print(f'   Name:  {c["full_name"]}')
        print(f'   Email: {c["email"]}')
        print(f'   Univ:  {c["university_name"]}')
        print(f'   ────────────────────────────')

    # Save to CSV
    print()
    save_to_csv(clean, output_path)
    print()
    print('✅ cleaner.py is working correctly!')