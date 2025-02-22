import threading
import schedule
import time
import subprocess
import logging
from flask import Flask, jsonify, request
from pymongo import MongoClient
import json
from bson.json_util import dumps
import os
from paytm import scrape_nowplaying

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    filename='logs/service.log'
)

logger = logging.getLogger('service')

# Flask App Setup
app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["chitram"]
collection = db["articles"]

# Scraper Functionality
def run_scraper():
    try:
        logger.info("Starting scraper")
        result = subprocess.run(['python', 'main.py'], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Scraper completed successfully")
        else:
            logger.error(f"Scraper failed with error: {result.stderr}")
    except Exception as e:
        logger.error(f"Error running scraper: {e}")

# Scheduler Thread Function
def run_scheduler():
    # Schedule the job to run every 20 seconds
    schedule.every(1).hour.do(run_scraper)
    logger.info("Scheduler started - Running scraper every 20 seconds")

    # Initial run
    run_scraper()

    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(1)

# Flask Endpoints
@app.route('/articles', methods=['GET'])
def get_articles():
    try:
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
        articles_json = json.loads(dumps(articles))
        
        return jsonify({
            'articles': articles_json,
            'page': page,
            'per_page': per_page,
            'total_count': total_count
        })
    except Exception as e:
        logger.error(f"Error in /articles endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/latest-articles', methods=['GET'])
def get_latest_articles():
    try:
        # Fetch all articles sorted by _id (timestamp)
        latest_articles = list(collection.find().sort('_id', -1))
        
        # Convert ObjectId to string for JSON serialization
        articles_json = json.loads(dumps(latest_articles))
        
        return jsonify({
            'articles': articles_json
        })
    except Exception as e:
        logger.error(f"Error in /latest-articles endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/city-movies', methods=['POST'])
def get_city_movies():
    """
    Dynamic endpoint to fetch movies for a given city.
    
    Expected JSON payload:
    {
        "city": "City Name"
    }
    
    Returns:
        JSON response with movies playing in the specified city
    """
    try:
        # Get city from request JSON
        data = request.get_json()
        city = data.get('city', 'mumbai').lower()  # Default to Mumbai if no city provided
        
        # Call the scraping function
        movies = scrape_nowplaying(city)
        
        if not movies:
            return jsonify({
                "message": f"No movies found for {city}",
                "city": city
            }), 404
        
        return jsonify({
            "city": city,
            "total_movies": len(movies),
            "movies": movies
        }), 200
    
    except Exception as e:
        return jsonify({
            "error": f"Error fetching movies: {str(e)}",
            "city": city
        }), 500

# Main Execution
def main():
    try:
        # Create and start scheduler thread
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("Started scheduler thread")
        
        # Run Flask app
        logger.info("Starting Flask application")
        app.run(host='0.0.0.0', port=8090, debug=False)
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == '__main__':
    main()
