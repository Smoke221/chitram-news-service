import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pymongo import MongoClient

# MongoDB connection
client = MongoClient("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["chitram"]
collection = db["movies"]  # Single collection for all cities

def scrape_nowplaying(city):
    """Scrape movie data for a given city."""
    # print(f"Fetching fresh data for {city}")
    try:
        main_url = f"https://paytm.com/movies/{city}"
        response = requests.get(main_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        movie_spans = soup.find_all("span", class_=lambda x: x and x.startswith("RunningMovies_moviesList"))

        movies = []
        for span in movie_spans:
            first_div = span.find("div")
            if not first_div:
                continue

            movie_divs = first_div.find_all("div", class_=lambda x: x and x.startswith("DesktopRunningMovie_movieCard"))
            for movie_div in movie_divs:
                script_tag = movie_div.find("script", type="application/ld+json")
                if not script_tag:
                    continue

                movie_data = json.loads(script_tag.string)
                name = movie_data.get("name", "Unknown")
                poster = movie_data.get("image", "Unknown")
                rating = movie_data.get("aggregateRating", {}).get("ratingValue", None)
                languages = movie_data.get("inLanguage", "Unknown")

                if isinstance(languages, str):
                    languages = [lang.strip() for lang in languages.split(",")]
                else:
                    languages = languages if isinstance(languages, list) else [languages]

                movies.append({
                    "name": name,
                    "poster": poster,
                    "rating": rating,
                    "languages": languages,
                    "isActive": True,  # All newly scraped movies are active
                    "lastUpdated": datetime.now()
                })

        return movies
    except requests.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return []

def save_to_mongodb(city, current_movies):
    """Save scraped data to MongoDB with active status tracking."""
    try:
        # Get existing movies for this city
        existing_data = collection.find_one({"city": city})
        
        if existing_data and "movies" in existing_data:
            existing_movies = existing_data["movies"]
            
            # Create a dictionary of current movie names for quick lookup
            current_movie_names = {movie["name"]: True for movie in current_movies}
            
            # For movies that exist in DB but not in current scrape, mark as inactive
            for existing_movie in existing_movies:
                if existing_movie["name"] not in current_movie_names:
                    # This movie is no longer playing - keep it but mark as inactive
                    existing_movie["isActive"] = False
                    existing_movie["lastUpdated"] = datetime.now()
                    current_movies.append(existing_movie)
        
        # Insert or update data for the city
        collection.update_one(
            {"city": city},
            {"$set": {"movies": current_movies, "timestamp": datetime.now()}},
            upsert=True
        )
        # print(f"Data saved to MongoDB for {city}")
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

def scrape_and_store(cities):
    """Scrape data for a list of cities and store it in MongoDB."""
    for city in cities:
        movies = scrape_nowplaying(city)
        if movies:
            save_to_mongodb(city, movies)
        # else:
        #     # print(f"No movies found for {city}. Skipping MongoDB update.")

# List of cities to scrape
cities = [
  "mumbai",
  "delhi",
  "bengaluru",
  "hyderabad",
  "chennai",
  "kolkata",
  "pune",
  "ahmedabad",
  "jaipur",
  "lucknow",
  "chandigarh",
  "kochi",
  "bhopal",
  "indore",
  "nagpur",
  "coimbatore",
  "guwahati",
  "bhubaneswar",
  "patna",
  "surat",
  "vadodara",
  "dehradun",
  "visakhapatnam",
  "thiruvananthapuram",
  "kanpur",
  "agra",
  "varanasi",
  "prayagraj",
  "amritsar",
  "ranchi",
  "ludhiana",
  "mysore",
  "raipur",
  "aurangabad",
  "nashik",
  "rajkot",
  "mangalore",
  "vijayawada",
  "madurai",
  "jalandhar",
  "jodhpur",
  "gwalior",
  "tiruchirappalli",
  "hubli-dharwad",
  "jammu",
  "srinagar",
  "shimla",
  "siliguri",
  "jamshedpur",
  "meerut",
  "thrissur",
  "guntur",
  "nellore",
  "kakinada",
  "panaji",
  "kolhapur",
  "kozhikode",
  "amravati",
  "akola",
  "kurnool",
  "udaipur",
  "ajmer",
  "ujjain",
  "jhansi",
  "nanded",
  "sangli",
  "tirunelveli",
  "salem",
  "erode",
  "warangal",
  "karnal",
  "rohtak",
  "patiala",
  "pathankot",
  "bhilai",
  "bilaspur",
  "cuttack",
  "rourkela",
  "bokaro",
  "dhanbad",
  "gorakhpur",
  "aligarh",
  "bareilly",
  "moradabad",
  "saharanpur",
  "ratlam",
  "bhavnagar",
  "jamnagar",
  "junagadh",
  "gandhinagar",
  "vellore",
  "tirupati",
  "rajahmundry",
  "haridwar",
  "rishikesh",
  "solapur",
  "durgapur",
  "asansol",
  "kota",
  "bikaner"
]

# Run the scraper and store data
scrape_and_store(cities)