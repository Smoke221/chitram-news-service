from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import json_util
import json

app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["chitram"]
collection = db["articles"]

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
    # Fetch the latest 10 articles sorted by timestamp
    latest_articles = list(collection.find().sort('_id', -1))
    
    # Convert ObjectId to string for JSON serialization
    articles_json = json.loads(json_util.dumps(latest_articles))
    
    return jsonify({
        'articles': articles_json
    })

if __name__ == '__main__':
    app.run(port=5000, debug=True)
