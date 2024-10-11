import requests
from bs4 import BeautifulSoup
import hashlib
import boto3
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
url = 'https://iirmp.utcluj.ro/orar.html'

dynamodb = boto3.resource('dynamodb', region_name = 'eu-west-1')
table_name = 'Web-Scraping-Table'
table = dynamodb.Table(table_name)

def scrape_pdf_links():
    """
    Scrapes the given URL for all links to PDF files.
    Returns a list of complete PDF links.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        soup = BeautifulSoup(response.content, 'html.parser')
        # Base URL to prepend if a link is relative
        base_url = 'https://iirmp.utcluj.ro'  # Replace with the base URL of your site

        # Find all PDF links
        pdf_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.endswith('.pdf'):
                # Check if the URL is relative
                if not href.startswith('http'):
                    # Prepend the base URL to make it absolute
                    href = base_url + '/' + href.lstrip('/')
                pdf_links.append(href)
        
        logging.info(f"Found {len(pdf_links)} PDF links.")
        return pdf_links
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the webpage: {e}")
        return []


def generate_file_hash(pdf_url):
    """
    Downloads the PDF from the given URL and generates an MD5 hash of its content.
    Returns the MD5 hash.
    """
    try:
        response = requests.get(pdf_url)
        response.raise_for_status()
        hasher = hashlib.md5()  # You can use hashlib.sha256() for a stronger hash
        hasher.update(response.content)
        file_hash = hasher.hexdigest()
        logging.info(f"Generated hash for {pdf_url}: {file_hash}")
        return file_hash
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading PDF {pdf_url}: {e}")
        
        return None

def store_in_dynamodb(link, file_hash):
    """
    Stores the PDF link and its hash in DynamoDB.
    """
    try:
        table.put_item(
            Item={
                'pdf_link': link,
                'file_hash': file_hash,
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        logging.info(f"Stored PDF link {link} with hash {file_hash} in DynamoDB.")
    except Exception as e:
        logging.error(f"Error storing data in DynamoDB: {e}")

def main():
    """
    Main function to scrape PDF links, generate hashes, and store them in DynamoDB.
    """
    pdf_links = scrape_pdf_links()
    for link in pdf_links:
        # Make sure the link is a full URL; if it's relative, prepend the base URL
        if link.startswith('/'):
            link = url + link
        
        file_hash = generate_file_hash(link)
        if file_hash:
            store_in_dynamodb(link, file_hash)

if __name__ == '__main__':
    main()