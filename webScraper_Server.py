import Pyro4
from collections import deque
from bs4 import BeautifulSoup
import requests
import re
import csv
import time
from urllib.parse import urljoin

@Pyro4.expose
class ScraperNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.emails_found = set()  # Store emails for this node
        self.page_count = 0
        self.crawled_urls = set()  # Node-specific crawled URLs

    def get_node_id(self):
        """Exposes the node ID."""
        return self.node_id

    def cfDecodeEmail(self, encodedString):
        """Decodes Cloudflare obfuscated email addresses."""
        r = int(encodedString[:2], 16)
        email = ''.join([chr(int(encodedString[i:i+2], 16) ^ r) for i in range(2, len(encodedString), 2)])
        return email

    def fetch_url(self, url):
        """Fetches the content of a URL."""
        try:
            print(f"Fetching URL: {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None

    def scrape_emails(self, soup, page_title, url):
        """Scrapes emails and adds them to the set with associated data."""
        obfuscated_emails = set(re.findall(r'data-cfemail="(.*?)"', str(soup)))
        for encoded_email in obfuscated_emails:
            try:
                decoded_email = self.cfDecodeEmail(encoded_email)
                self.emails_found.add((decoded_email, url, page_title))
            except Exception as e:
                print(f"Failed to decode email: {encoded_email}. Error: {e}")

    def extract_links(self, soup, base_url):
        """Extracts and returns valid links from a BeautifulSoup object."""
        links = set()
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            absolute_url = urljoin(base_url, href)
            if absolute_url.endswith('.pdf') or '/pdf/' in absolute_url:
                continue
            if absolute_url not in self.crawled_urls and absolute_url.startswith(base_url):
                links.add(absolute_url)
        return links

    def crawl(self, start_url, time_limit):
        """Performs a time-limited crawl starting from the given URL."""
        start_time = time.time()
        queue = deque([start_url])  # Queue for BFS
        self.page_count = 0

        while queue and (time.time() - start_time) < time_limit * 60:
            current_url = queue.popleft()
            if current_url in self.crawled_urls:
                continue

            response = self.fetch_url(current_url)
            if not response:
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            page_title = soup.title.string.strip() if soup.title else "No Title"

            self.scrape_emails(soup, page_title, current_url)
            new_links = self.extract_links(soup, start_url)
            queue.extend(new_links)

            self.crawled_urls.add(current_url)
            self.page_count += 1

        return self.page_count

    def get_emails(self):
        """Returns the emails found by this node."""
        return list(self.emails_found)

    def save_emails_to_csv(self, output_file):
        """Saves found emails to a CSV file."""
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Email', 'Source URL', 'Webpage Title'])
            for email, source_url, page_title in self.emails_found:
                writer.writerow([email, source_url, page_title])

    def save_statistics(self, stats_file):
        """Saves scraping statistics to a text file."""
        with open(stats_file, 'w', encoding='utf-8') as file:
            file.write(f"Total Pages Crawled: {self.page_count}\n")
            file.write(f"Total Emails Found: {len(self.emails_found)}\n")

@Pyro4.expose
class ScraperServer:
    def __init__(self):
        self.node_uris = []

    def start_nodes(self, num_nodes):
        """Create and register scraper nodes."""
        daemon = Pyro4.Daemon(host="10.2.202.108")
        ns = Pyro4.locateNS(host="10.2.202.108", port=9090)

        for i in range(num_nodes):
            node = ScraperNode(node_id=i + 1)
            uri = daemon.register(node)
            ns.register(f"scraper.node.{i+1}", uri)
            self.node_uris.append(uri)
            print(f"Node {i + 1} registered with URI: {uri}")

        daemon.requestLoop()

    def get_node_uris(self):
        """Returns the list of registered node URIs."""
        return self.node_uris

def main():
    server = ScraperServer()
    num_nodes = int(input("Enter the number of nodes to start: ").strip())
    server.start_nodes(num_nodes)

if __name__ == "__main__":
    main()
