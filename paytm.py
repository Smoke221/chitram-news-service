import os
import json
import requests
import random
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

def get_cache_filepath(city):
    return os.path.join(CACHE_DIR, f"{city.lower()}.json")

def is_cache_valid(filepath):
    if not os.path.exists(filepath):
        return False
    modified_time = datetime.fromtimestamp(os.path.getmtime(filepath))
    return datetime.now() - modified_time < timedelta(days=1)

def save_to_cache(city, data):
    filepath = get_cache_filepath(city)
    try:
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump({"timestamp": datetime.now().isoformat(), "movies": data}, file, indent=4)
    except Exception as e:
        print(f"Error saving cache: {e}")

def load_from_cache(city):
    filepath = get_cache_filepath(city)
    if is_cache_valid(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                return json.load(file)["movies"]
        except Exception as e:
            print(f"Error reading cache: {e}")
    return None

def fetch_with_retries(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS)
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                raise e

def scrape_nowplaying(city):
    print(f"Checking cache for {city}")
    cached_data = load_from_cache(city)
    if cached_data:
        print(f"Serving cached data for {city}")
        return cached_data

    print(f"Fetching fresh data for {city}")
    try:
        main_url = f"https://paytm.com/movies/{city}"
        response = fetch_with_retries(main_url)
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
                    "languages": languages
                })

        if movies:
            save_to_cache(city, movies)
        else:
            print("No movies found, skipping cache update.")

        return movies

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []