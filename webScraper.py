from bs4 import BeautifulSoup
import requests
import requests.exceptions
import urllib.parse
from collections import deque
import re
import csv
import time
import argparse
from urllib.parse import urljoin

# Function to decode Cloudflare obfuscated email addresses
def cfDecodeEmail(encodedString):
    r = int(encodedString[:2], 16)
    email = ''.join([chr(int(encodedString[i:i+2], 16) ^ r) for i in range(2, len(encodedString), 2)])
    return email

# Globals
crawled_urls = set()  # Set of already crawled URLs
emails_found = set()  # Set to store found emails
start_time = None  # Start time for time-limited scraping

def fetch_url(url):
    """Fetches the content of a URL."""
    try:
        print(f"Fetching URL: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def scrape_emails(soup, page_title, url):
    """Scrapes emails and adds them to the set with associated data."""
    global emails_found
    # Extract Cloudflare obfuscated emails
    obfuscated_emails = set(re.findall(r'data-cfemail="(.*?)"', str(soup)))
    for encoded_email in obfuscated_emails:
        try:
            decoded_email = cfDecodeEmail(encoded_email)
            emails_found.add((decoded_email, url, page_title))
        except Exception as e:
            print(f"[-] Failed to decode email: {encoded_email}. Error: {e}")


def extract_links(soup, base_url):
    """Extracts and returns valid links from a BeautifulSoup object."""
    links = set()
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        absolute_url = urljoin(base_url, href)
        
        # Skip non-HTML files and duplicates
        if absolute_url.endswith('.pdf') or '/pdf/' in absolute_url:
            continue
        if absolute_url not in crawled_urls and absolute_url.startswith(base_url):
            links.add(absolute_url)
    return links

def crawl(start_url, time_limit):
    """Performs a time-limited crawl starting from the given URL."""
    global start_time
    start_time = time.time()
    queue = deque([start_url])  # Queue for BFS
    page_count = 0

    while queue and (time.time() - start_time) < time_limit * 60:
        current_url = queue.popleft()
        if current_url in crawled_urls:
            continue

        response = fetch_url(current_url)
        if not response:
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        page_title = soup.title.string.strip() if soup.title else "No Title"

        # Scrape emails and add links to queue
        scrape_emails(soup, page_title, current_url)
        new_links = extract_links(soup, start_url)
        queue.extend(new_links)

        # Mark the URL as crawled
        crawled_urls.add(current_url)
        page_count += 1

    return page_count

def save_emails_to_csv(output_file):
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Email', 'Source URL', 'Webpage Title'])  # Header
        for email, source_url, page_title in emails_found:
            writer.writerow([email, source_url, page_title])

def save_statistics_to_file(stats_file, page_count):
    """Saves statistics of the web scraping process to a text file."""
    elapsed_time = time.time() - start_time
    with open(stats_file, 'w', encoding='utf-8') as file:
        file.write(f"Total Pages Crawled: {page_count}\n")
        file.write(f"Total Emails Found: {len(emails_found)}\n")
        file.write(f"Time taken: {elapsed_time:.2f} seconds\n")

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Email web scraper with time limit.")
    parser.add_argument("url", help="URL of the website to scrape")
    parser.add_argument("time_limit", type=int, help="Scraping time limit in minutes")
    parser.add_argument("--output_emails", default="emails.csv", help="Output CSV file for emails")
    parser.add_argument("--output_stats", default="stats.txt", help="Output text file for statistics")
    args = parser.parse_args()

    # Start crawling
    page_count = crawl(args.url, args.time_limit)

    # Save results
    save_emails_to_csv(args.output_emails)
    save_statistics_to_file(args.output_stats, page_count)

    print(f"\n=== Results ===")
    print(f"Emails saved to: {args.output_emails}")
    print(f"Statistics saved to: {args.output_stats}")

if __name__ == "__main__":
    main()
