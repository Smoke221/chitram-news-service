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
from datetime import datetime, timedelta, timezone
import requests

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


@app.route('/latest-articles', methods=['GET'])
def get_latest_articles():
    try:
        # Calculate the datetime threshold (3 days ago)
        three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
        
        # Fetch articles scraped in the last 3 days, sorted by _id (timestamp)
        latest_articles = list(collection.find({"scraped_at": {"$gte": three_days_ago}}).sort('_id', -1))
        
        # Convert ObjectId to string for JSON serialization
        articles_json = json.loads(dumps(latest_articles))
        
        return jsonify({'articles': articles_json})
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

@app.route('/register-token', methods=['POST'])
def register_token():
    """
    Store Expo Push Tokens in MongoDB.
    """
    try:
        data = request.get_json()
        token = data.get("token")

        if not token:
            return jsonify({"error": "No token provided"}), 400

        # Ensure token is not duplicated
        if db["tokens"].count_documents({"token": token}) == 0:
            db["tokens"].insert_one({"token": token})

        return jsonify({"message": "Token registered successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"

def send_push_notification(title, message, image_url):
    """
    Fetch stored Expo push tokens from MongoDB and send notifications with optional image.
    """
    try:
        tokens = db["tokens"].find({}, {"_id": 0, "token": 1})
        tokens = [t["token"] for t in tokens]

        if not tokens:
            return {"error": "No registered push tokens"}

        # Create notification payload
        messages = []
        for token in tokens:
            payload = {
                "to": token,
                "title": title,
                "body": message,
                "data": {"image": image_url} if image_url else {}  # Include image if available
            }
            messages.append(payload)

        # Send push notification
        response = requests.post(EXPO_PUSH_URL, json=messages, headers={"Content-Type": "application/json"})
        logger.info(f"Push notification sent with status code: {response.json()}")
        return response.json()
    except Exception as e:
        return {"error": str(e)}


@app.route('/send-notification', methods=['POST'])
def trigger_notification():
    """
    API to manually trigger a push notification.
    """
    try:
        data = request.get_json()
        title = data.get("title", "New Update!")
        message = data.get("message", "Check out the latest news!")

        response = send_push_notification(title, message)
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
