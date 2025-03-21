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
# Import transformers for text summarization
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

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

# Initialize the summarization model and tokenizer
try:
    summarization_tokenizer = AutoTokenizer.from_pretrained("AventIQ-AI/t5-text-summarizer")
    summarization_model = AutoModelForSeq2SeqLM.from_pretrained("AventIQ-AI/t5-text-summarizer")
    scraper_logger.info("Successfully loaded the summarization model")
except Exception as e:
    error_logger.error(f"Error loading summarization model: {str(e)}")
    # Continue without summarization if model fails to load
    summarization_model = None
    summarization_tokenizer = None

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

def generate_summary(content, min_words_per_paragraph=100):
    """
    Generate a two-paragraph summary of the content using the T5 summarization model.
    Each paragraph will have at least min_words_per_paragraph words.
    """
    if not summarization_model or not summarization_tokenizer or not content:
        return None
    
    try:
        # Prepare the input for the model
        input_text = "summarize: " + content
        
        # Tokenize and generate summary
        inputs = summarization_tokenizer(input_text, return_tensors="pt", max_length=1024, truncation=True)
        
        # Generate a longer summary to ensure we have enough content for two paragraphs
        summary_ids = summarization_model.generate(
            inputs["input_ids"],
            max_length=300,  # Longer output to ensure we have enough for two paragraphs
            min_length=150,  # Minimum length to ensure substantive content
            num_beams=4,
            no_repeat_ngram_size=3,
            early_stopping=True
        )
        
        # Decode the summary
        summary = summarization_tokenizer.decode(summary_ids[0], skip_special_tokens=True)
        
        # Split the summary into sentences
        sentences = summary.split('. ')
        
        # Ensure each sentence ends with a period
        sentences = [s + '.' if not s.endswith('.') else s for s in sentences]
        
        # Calculate approximate midpoint to create two paragraphs
        total_words = len(summary.split())
        target_words_per_paragraph = max(min_words_per_paragraph, total_words // 2)
        
        # Create two paragraphs
        paragraph1 = []
        paragraph2 = []
        word_count = 0
        
        for sentence in sentences:
            sentence_words = len(sentence.split())
            
            if word_count < target_words_per_paragraph:
                paragraph1.append(sentence)
                word_count += sentence_words
            else:
                paragraph2.append(sentence)
        
        # If paragraph2 is empty or too short, redistribute
        if not paragraph2 or len(' '.join(paragraph2).split()) < min_words_per_paragraph:
            # Recalculate distribution
            midpoint = len(sentences) // 2
            paragraph1 = sentences[:midpoint]
            paragraph2 = sentences[midpoint:]
        
        # Join sentences into paragraphs
        para1_text = ' '.join(paragraph1)
        para2_text = ' '.join(paragraph2)
        
        return {
            "paragraph1": para1_text,
            "paragraph2": para2_text,
            "full_summary": summary
        }
        
    except Exception as e:
        error_logger.error(f"Error generating summary: {str(e)}")
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
    
    # Generate summary if content is available
    summary = generate_summary(content) if content else None
    print(summary)
    
    article_doc = {
        "url": article_url,
        "title": title,
        "image_url": image_url,
        "content": content,
        "scraped_at": datetime.now()
    }
    
    # Add summary if available
    if summary:
        article_doc["summary"] = summary
    
    return article_doc

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
                    # Use summary for notification if available
                    message = "📰 Latest Chitram News"
                    if last_article.get("summary") and last_article["summary"].get("paragraph1"):
                        message = last_article["summary"]["paragraph1"][:100] + "..."
                        
                    send_push_notification(
                        title=last_article.get("title", "New article available!"),
                        message=message,
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