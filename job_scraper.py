import argparse
import requests
from bs4 import BeautifulSoup
import re
import sqlite3
import logging
from pdfminer.high_level import extract_text
import os
import tempfile
from urllib.parse import urljoin
from datetime import datetime, date
import hashlib

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Disable pdfminer logger
logging.getLogger("pdfminer").setLevel(logging.ERROR)

def adapt_date(date_value):
    return date_value.isoformat()

sqlite3.register_adapter(date, adapt_date)

def convert_date(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d").date()

sqlite3.register_converter("date", convert_date)

def setup_database(db_path):
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS job_ads
                 (id TEXT PRIMARY KEY, title TEXT, full_text TEXT, job_type TEXT, deadline DATE)''')
    conn.commit()
    return conn

def get_content(input_path):
    logging.debug(f"Fetching content from: {input_path}")
    if input_path.startswith(('http:', 'https:')):
        response = requests.get(input_path)
        response.raise_for_status()
        return response.content, input_path
    else:
        with open(input_path, 'r', encoding='utf-8') as file:
            return file.read(), 'https://www.uni-potsdam.de'

def parse_deadline(text):
    patterns = [
        r'Deadline:\s*(\w+ \d{2},? \d{4})',
        r'Bewerbungsschluss:\s*(\d{2}\.\d{2}\.\d{4})'
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            date_str = match.group(1)
            try:
                if '.' in date_str:
                    return datetime.strptime(date_str, '%d.%m.%Y').date()
                else:
                    return datetime.strptime(date_str, '%B %d, %Y').date()
            except ValueError:
                logging.warning(f"Failed to parse date: {date_str}")
    logging.warning(f"No valid deadline found in: {text}")
    return None

def generate_id(title, deadline):
    if deadline:
        return hashlib.md5(f"{title}{deadline}".encode()).hexdigest()
    return hashlib.md5(title.encode()).hexdigest()

def scrape_job_ads(soup, base_url):
    job_ads = []
    job_type = soup.find('h1').text.strip()
    logging.info(f"Job type found: {job_type}")

    # Find the container for job listings
    container = soup.find('div', class_='up-content-link-box')
    if not container:
        logging.warning("Could not find the job listings container")
        return job_ads

    for li in container.find_all('li'):
        link = li.find('a', class_='up-document-link')
        if link:
            title = link.text.strip()
            pdf_url = urljoin(base_url, link['href'])
            
            # Extract all text from the li element
            full_text = li.get_text(strip=True)
            
            deadline = parse_deadline(full_text)
            
            kenn_nr_match = re.search(r'Kenn-Nr\.\s*(\S+)', full_text)
            if kenn_nr_match:
                job_id = kenn_nr_match.group(1)
            else:
                job_id = generate_id(title, deadline)
            
            logging.info(f"Found job ad: ID={job_id}, Title={title}, Deadline={deadline}")
            job_ads.append({
                'id': job_id,
                'title': title,
                'pdf_url': pdf_url,
                'job_type': job_type,
                'deadline': deadline
            })

    logging.info(f"Total job ads found: {len(job_ads)}")
    return job_ads


def process_pdf(pdf_url):
    logging.info(f"Processing PDF: {pdf_url}")
    response = requests.get(pdf_url)
    response.raise_for_status()
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(response.content)
        temp_file_path = temp_file.name

    try:
        text = extract_text(temp_file_path)
        logging.info(f"Extracted {len(text)} characters from PDF")
        return text
    finally:
        os.unlink(temp_file_path)

def process_input(input_path, conn):
    c = conn.cursor()
    try:
        content, base_url = get_content(input_path)
        soup = BeautifulSoup(content, 'html.parser')
        
        job_ads = scrape_job_ads(soup, base_url)
        
        for job in job_ads:
            c.execute("SELECT id FROM job_ads WHERE id=?", (job['id'],))
            if c.fetchone() is None:
                logging.info(f"Processing new job ad: {job['id']} - {job['title']}")
                try:
                    full_text = process_pdf(job['pdf_url'])
                    c.execute("INSERT INTO job_ads (id, title, full_text, job_type, deadline) VALUES (?, ?, ?, ?, ?)",
                              (job['id'], job['title'], full_text, job['job_type'], job['deadline']))
                    logging.info(f"Inserted new job ad: {job['id']}")
                except Exception as e:
                    logging.error(f"Error processing PDF for job {job['id']}: {str(e)}")
            else:
                logging.info(f"Job ad already in database: {job['id']}")

        conn.commit()
    except Exception as e:
        logging.error(f"Error processing input {input_path}: {str(e)}")
        conn.rollback()

def main(input_path, output_path):
    conn = setup_database(output_path)
    
    logging.info(f"Processing input: {input_path}")
    process_input(input_path, conn)
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape job ads from University of Potsdam website or local HTML file.")
    parser.add_argument("input", help="Path to local HTML file or URL of the job listings page")
    parser.add_argument("output", help="Path to the output SQLite database")
    args = parser.parse_args()

    main(args.input, args.output)
