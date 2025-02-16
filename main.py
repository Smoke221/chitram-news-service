import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import logging
import os

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

def scrape_articles():
    try:
        # URL of the main articles page
        main_url = 'https://www.gulte.com/movienews'
        scraper_logger.info(f"Starting scraping from {main_url}")

        # Fetch the main page
        response = requests.get(main_url)
        response.raise_for_status()  # Raise exception for bad status codes
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all articles
        articles = soup.find_all('div', class_='post-thumbnail')
        scraper_logger.info(f"Found {len(articles)} articles to process")
        
        new_articles_count = 0
        for article in articles[1:]:
            try:
                a_tag = article.find('a')
                if not a_tag or 'href' not in a_tag.attrs:
                    continue

                article_url = a_tag['href']
                
                # Check if article exists in MongoDB
                if collection.find_one({"url": article_url}):
                    scraper_logger.debug(f"Article already exists: {article_url}")
                    continue

                # Fetch the article page
                article_response = requests.get(article_url)
                article_response.raise_for_status()
                article_soup = BeautifulSoup(article_response.text, 'html.parser')

                # Extract data from div with class 'post-inner'
                post_inner = article_soup.find('div', class_='post-inner')

                if post_inner:
                    # Extract title
                    title_tag = post_inner.find('h1')
                    title = title_tag.text.strip() if title_tag else "No Title"

                    # Extract image
                    image_div = post_inner.find('div', class_='single-post-thumb')
                    image_tag = image_div.find('img') if image_div else None
                    image_url = image_tag['src'] if image_tag and 'src' in image_tag.attrs else "No Image"

                    # Extract content
                    entry_div = post_inner.find('div', class_='entry')
                    paragraphs = entry_div.find_all('p') if entry_div else []
                    content = "\n".join(p.text.strip() for p in paragraphs)

                    # Create article document
                    article_doc = {
                        "url": article_url,
                        "title": title,
                        "image_url": image_url,
                        "content": content,
                        "scraped_at": datetime.now()
                    }

                    # Store in MongoDB
                    collection.insert_one(article_doc)
                    new_articles_count += 1
                    mongo_logger.info(f"Successfully stored article: {title}")
                    scraper_logger.info(f"Processed article: {title}")

            except Exception as e:
                error_logger.error(f"Error processing article {article_url if 'article_url' in locals() else 'Unknown'}: {str(e)}")
                continue

        scraper_logger.info(f"Scraping completed. Added {new_articles_count} new articles")
        return True

    except requests.exceptions.RequestException as e:
        error_logger.error(f"Request error: {str(e)}")
    except Exception as e:
        error_logger.error(f"Unexpected error: {str(e)}")
    return False

if __name__ == "__main__":
    scrape_articles()