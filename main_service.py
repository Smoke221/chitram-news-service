import threading
import schedule
import time
import subprocess
import logging
from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import json_util
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    filename='service.log'
)

# Flask App Setup
app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["chitram"]
collection = db["articles"]

# Scraper Functionality
def run_scraper():
    try:
        logging.info(f"Starting scraper at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        result = subprocess.run(['python3', 'main.py'], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info("Scraper completed successfully")
            logging.info(result.stdout)
        else:
            logging.error("Scraper failed")
            logging.error(result.stderr)
    except Exception as e:
        logging.error(f"Error running scraper: {e}")

# Scheduler Thread Function
def run_scheduler():
    # Schedule the job to run every 1 hour
    schedule.every(1).hour.do(run_scraper)

    # Initial run
    run_scraper()

    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(1)

# Flask Endpoints
@app.route('/articles', methods=['GET'])
def get_articles():
    # Default pagination
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    
    # Optional filtering
    title_filter = request.args.get('title', None)
    
    # Build query
    query = {}
    if title_filter:
        query['title'] = {'$regex': title_filter, '$options': 'i'}
    
    # Calculate skip and limit for pagination
    skip = (page - 1) * per_page
    
    # Fetch total count and articles
    total_count = collection.count_documents(query)
    articles = list(collection.find(query).skip(skip).limit(per_page))
    
    # Convert ObjectId to string for JSON serialization
    articles_json = json.loads(json_util.dumps(articles))
    
    return jsonify({
        'articles': articles_json,
        'page': page,
        'per_page': per_page,
        'total_count': total_count
    })

@app.route('/latest-articles', methods=['GET'])
def get_latest_articles():
    # Fetch the latest articles sorted by timestamp
    latest_articles = list(collection.find().sort('_id', -1))
    
    # Convert ObjectId to string for JSON serialization
    articles_json = json.loads(json_util.dumps(latest_articles))
    
    return jsonify({
        'articles': articles_json
    })

# Main Execution
def main():
    # Create threads
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    
    # Start threads
    scheduler_thread.start()
    
    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=False)

if __name__ == '__main__':
    main()
