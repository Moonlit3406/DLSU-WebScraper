import requests
import re
import csv
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from collections import deque
import argparse

# Regex for DLSU emails
email_regex = re.compile(r'[A-Za-z0-9._%+-]+@dlsu\.edu\.ph')

# Globals
crawled_urls = set()  # Set of already crawled URLs
emails_found = {}  # Dictionary to store emails and associated details
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

def scrape_emails(soup):
    global emails_found
    emails = soup.find_all(string=email_regex)
    for email in emails:
        email = email.strip()  # Clean up the email string
        parent_tag = soup.find(string=email)
        if parent_tag and parent_tag.parent:  # Ensure parent exists
            parent = parent_tag.parent
            name = ""
            office = ""
            department = ""

            # Extract relevant information from sibling or parent elements
            if parent.name == 'a':  # If the email is part of a link
                name = parent.get_text(strip=True)
            elif parent.find_previous() and parent.find_previous().name in ['p', 'div', 'span']:
                name = parent.find_previous().get_text(strip=True)

            # Store email with associated data
            emails_found.add((email, name, office, department))

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

def crawl(start_url, time_limit, max_nodes):
    """Performs a time-limited crawl starting from the given URL."""
    global start_time
    start_time = time.time()
    queue = deque([(start_url, 0)])  # (url, node count)
    node_count = 0

    while queue and (time.time() - start_time) < time_limit * 60:
        current_url, depth = queue.popleft()
        if node_count >= max_nodes:
            break

        response = fetch_url(current_url)
        if not response:
            continue

        soup = BeautifulSoup(response.content, 'html.parser')

        # Scrape emails and add links to queue
        scrape_emails(soup)
        new_links = extract_links(soup, start_url)
        queue.extend((link, depth + 1) for link in new_links)

        # Mark the URL as crawled
        crawled_urls.add(current_url)
        node_count += 1

def save_emails_to_csv(output_file):
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Email', 'Name', 'Office', 'Department'])  # Header
        for email, name, office, department in emails_found:
            writer.writerow([email, name, office, department])

def save_statistics_to_file(stats_file):
    """Saves statistics of the web scraping process to a text file."""
    elapsed_time = time.time() - start_time
    with open(stats_file, 'w', encoding='utf-8') as file:
        file.write(f"Total URLs crawled: {len(crawled_urls)}\n")
        file.write(f"Total emails found: {len(emails_found)}\n")
        file.write(f"Time taken: {elapsed_time:.2f} seconds\n")

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Email web scraper with time limit.")
    parser.add_argument("url", help="URL of the website to scrape")
    parser.add_argument("time_limit", type=int, help="Scraping time limit in minutes")
    parser.add_argument("max_nodes", type=int, help="Maximum number of nodes to scrape")
    parser.add_argument("--output_emails", default="emails.csv", help="Output CSV file for emails")
    parser.add_argument("--output_stats", default="stats.txt", help="Output text file for statistics")
    args = parser.parse_args()

    # Start crawling
    crawl(args.url, args.time_limit, args.max_nodes)

    # Save results
    save_emails_to_csv(args.output_emails)
    save_statistics_to_file(args.output_stats)

    print(f"\n=== Results ===")
    print(f"Emails saved to: {args.output_emails}")
    print(f"Statistics saved to: {args.output_stats}")

if __name__ == "__main__":
    main()
