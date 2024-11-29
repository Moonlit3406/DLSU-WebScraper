import requests
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Globals
email_id = re.compile(r'[A-Za-z]+\.[A-Za-z]+@dlsu\.edu\.ph')  # Regex for DLSU emails
start_url = 'https://www.dlsu.edu.ph'  # Starting URL
url_list = [start_url]  # List of URLs to crawl
crawled_urls = set()  # Set of already crawled URLs (to avoid duplicates)
emails_found = set()  # Set of unique emails
allowed_extensions = ('html', '')  # Allowed file extensions ('' handles URLs without extensions)

# 1: Pull the requests
def pull_url(func):
    def inner(*args, **kwargs):
        current_url = url_list[-1]
        try:
            print(f"Fetching URL: {current_url}")
            page = requests.get(current_url, timeout=10)  # Add a timeout for robustness
            if page.status_code == 200:
                func(page, *args, **kwargs)
            else:
                print(f"The URL {current_url} returned a status of {page.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error accessing {current_url}: {e}")
    return inner

# 2: Parse the URLs
@pull_url
def get_urls(page):
    global url_list
    soup = BeautifulSoup(page.content, 'html.parser')

    # Find all links on the page
    for link in soup.find_all('a', href=True):
        href = link['href']
        print(href)

        # Convert relative URLs to absolute URLs
        absolute_url = urljoin(start_url, href)

        # Filter out unwanted URLs (PDFs and duplicates)
        if absolute_url.endswith('.pdf') or '/pdf/' in absolute_url:
            continue

        if absolute_url.startswith(start_url) and absolute_url not in crawled_urls and absolute_url.split('.')[-1] in allowed_extensions:
            url_list.append(absolute_url)

    # Scrape emails from the current page
    scrape_emails(soup)

    # Add the current URL to crawled URLs
    crawled_urls.add(url_list[-1])
    url_list.pop()  # Remove the last URL after crawling

# 3: Scrape Emails
def scrape_emails(soup):
    global emails_found
    emails = soup.find_all(string=email_id)
    for email in emails:
        emails_found.add(email.strip())  # Add email to the set (to avoid duplicates)

# 4: Web crawling loop
while url_list:
    get_urls()

# Print results
print("\n=== Crawled URLs ===")
for crawled_url in sorted(crawled_urls):
    print(crawled_url)

if not emails_found:
    print("\nNo emails found.")
else:
    print("\n=== Emails Found ===")
    for email in sorted(emails_found):
        print(email)
