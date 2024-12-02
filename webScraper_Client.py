import Pyro4
from collections import deque
import csv
import time

def main():
    not_quit = True  # Control the loop for continuous scraping

    # Connect to the Pyro4 name server
    ns_host = input("Enter the Pyro4 name server host (default '10.2.202.108'): ").strip() or "10.2.202.108"
    ns_port = int(input("Enter the Pyro4 name server port (default 9090): ").strip() or "9090")
    ns = Pyro4.locateNS(host=ns_host, port=ns_port)

    # Retrieve the list of scraper node URIs
    node_uris = [uri for name, uri in ns.list(prefix="scraper.node.").items()]
    if not node_uris:
        print("No scraper nodes are registered. Please start the server and nodes.")
        return

    # Connect to a specific scraper node
    print("Available scraper nodes:")
    for i, uri in enumerate(node_uris):
        print(f"  {i + 1}: {uri}")
    node_index = int(input("Select a node by number (default 1): ").strip() or "1") - 1
    scraper_node = Pyro4.Proxy(node_uris[node_index])

    # Get the node ID using the exposed method
    node_id = scraper_node.get_node_id()

    while not_quit:
        # Input parameters
        #start_url = "https://www.dlsu.edu.ph"
        start_url = input("Enter the URL to scrape: ").strip()
        time_limit = int(input("Enter the scraping time limit (minutes): ").strip())

        # Automatically generate output file names based on the node ID
        output_file = f"emails_node_{node_id}.csv"
        stats_file = f"stats_node_{node_id}.txt"

        # BFS Queue for URLs to scrape
        queue = deque([start_url])
        crawled_urls = set()
        all_emails = set()
        start_time = time.time()
        total_pages = 0

        while queue and (time.time() - start_time) < time_limit * 60:
            url = queue.popleft()
            if url in crawled_urls:
                continue

            # Send URL to the scraper node for processing
            print(f"Sending URL to node: {url}")
            try:
                page_count = scraper_node.crawl(url, time_limit)
                emails = scraper_node.get_emails()
                all_emails.update(emails)  # Collect (Email, Source URL, Webpage Title)
                total_pages += page_count
                print(f"Processed {page_count} pages, found {len(all_emails)} emails so far.")
            except Exception as e:
                print(f"Error while processing URL: {url}. Error: {e}")

            crawled_urls.add(url)

        # Calculate scraping time in minutes
        scraping_time_minutes = (time.time() - start_time) / 60

        # Save emails to CSV
        save_emails_to_csv(output_file, all_emails)

        # Save statistics
        save_statistics(stats_file, total_pages, len(all_emails), scraping_time_minutes)
        print(f"Scraping completed. Emails saved to {output_file}.")
        print(f"Statistics saved to {stats_file}.")

        key_in=input("Do you want to quit? [y/n]").strip()
        
        if (key_in=='y' or key_in=='Y'):
            not_quit=False 

def save_emails_to_csv(output_file, emails):
    """Saves emails with additional information to a CSV file."""
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Email", "Source URL", "Webpage Title"])  # Header
        for email, source_url, page_title in emails:
            writer.writerow([email, source_url, page_title])

def save_statistics(stats_file, total_pages, total_emails, scraping_time_minutes):
    """Saves statistics to a text file."""
    with open(stats_file, mode="w", encoding="utf-8") as file:
        file.write(f"Total Pages Crawled: {total_pages}\n")
        file.write(f"Total Emails Found: {total_emails}\n")
        file.write(f"Scraping Time Taken: {scraping_time_minutes:.2f} minutes\n")

if __name__ == "__main__":
    main()
