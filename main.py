import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import logging
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from contextlib import contextmanager
import time
from main_service import send_push_notification

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure logging
def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:  # Prevent duplicate handlers
        logger.addHandler(handler)
    return logger

# Setup different loggers
scraper_logger = setup_logger('scraper', 'logs/scraper.log')
mongo_logger = setup_logger('mongodb', 'logs/mongodb.log')
error_logger = setup_logger('error', 'logs/error.log')

# MongoDB setup
try:
    client = MongoClient("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client["chitram"]
    collection = db["articles"]
    mongo_logger.info("Successfully connected to MongoDB")
except Exception as e:
    error_logger.error(f"MongoDB connection error: {str(e)}")
    raise

def create_session():
    session = requests.Session()
    retries = Retry(total=3, 
                   backoff_factor=1,
                   status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

@contextmanager
def mongodb_connection(uri):
    client = MongoClient(uri)
    try:
        yield client
    finally:
        client.close()

def validate_article(article_doc):
    required_fields = ['url', 'title', 'image_url', 'content']
    return all(article_doc.get(field) for field in required_fields)

def extract_thumbnail(article):
    thumbnail_image_url = "No Image"
    a_tag = article.find('a')
    if a_tag:
        img_tag = a_tag.find('img')
        if img_tag and 'src' in img_tag.attrs and img_tag['src'].startswith('https'):
            thumbnail_image_url = img_tag['src']
        else:
            noscript_tag = a_tag.find('noscript')
            if noscript_tag:
                img_tag = noscript_tag.find('img')
                if img_tag and 'src' in img_tag.attrs and img_tag['src'].startswith('https'):
                    thumbnail_image_url = img_tag['src']
    return thumbnail_image_url

def extract_article_url(article):
    a_tag = article.find('a')
    if a_tag and 'href' in a_tag.attrs:
        return a_tag['href']
    return None

def process_article_content(session, article_url, thumbnail_image_url):
    article_response = session.get(article_url)
    article_response.raise_for_status()
    article_soup = BeautifulSoup(article_response.text, 'html.parser')

    post_inner = article_soup.find('div', class_='post-inner')
    if not post_inner:
        return None

    title_tag = post_inner.find('h1')
    title = title_tag.text.strip() if title_tag else "No Title"

    image_div = post_inner.find('div', class_='single-post-thumb') or post_inner.find('figure', class_='wp-block-image')
    image_tag = image_div.find('img') if image_div else None
    image_url = "No Image"
    if image_tag and 'src' in image_tag.attrs and image_tag['src'].startswith('https'):
        image_url = image_tag['src']
    elif thumbnail_image_url.startswith('https'):
        image_url = thumbnail_image_url

    entry_div = post_inner.find('div', class_='entry')
    paragraphs = entry_div.find_all('p') if entry_div else []
    content = "\n".join(p.text.strip() for p in paragraphs)

    return {
        "url": article_url,
        "title": title,
        "image_url": image_url,
        "content": content,
        "scraped_at": datetime.now()
    }

def scrape_articles():
    session = create_session()
    article_docs = []
    
    try:
        main_url = 'https://www.gulte.com/movienews/'
        scraper_logger.info(f"Starting scraping from {main_url}")

        response = session.get(main_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('div', class_='post-thumbnail')
        scraper_logger.info(f"Found {len(articles)} articles to process")
        
        with mongodb_connection("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0") as client:
            db = client["chitram"]
            collection = db["articles"]
            
            for article in articles[1:]:
                try:
                    # Process thumbnail
                    thumbnail_image_url = extract_thumbnail(article)
                    article_url = extract_article_url(article)
                    
                    if not article_url or collection.find_one({"url": article_url}):
                        continue

                    # Process article content
                    article_doc = process_article_content(session, article_url, thumbnail_image_url)
                    
                    if article_doc and validate_article(article_doc):
                        article_docs.append(article_doc)
                        
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    error_logger.error(f"Error processing article: {str(e)}")
                    continue

            # Bulk insert
            if article_docs:
                collection.insert_many(article_docs)
                mongo_logger.info(f"Successfully stored {len(article_docs)} articles")

                # 🔥 Fetch the latest article directly from MongoDB
                last_article = collection.find_one({}, sort=[("_id", -1)])  # Get the last inserted article
                
                if last_article:
                    send_push_notification(
                        title=last_article.get("title", "New article available!"),
                        message="📰 Latest Chitram News", # Latest article title as body
                        image_url=last_article.get("image_url", None)  # Latest article image as image
                    )
                
        return True

    except Exception as e:
        error_logger.error(f"Unexpected error: {str(e)}")
        return False
    finally:
        session.close()

if __name__ == "__main__":
    scrape_articles()