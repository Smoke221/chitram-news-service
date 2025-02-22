import requests
from bs4 import BeautifulSoup
import json

def scrape_nowplaying(city):
    try:
        main_url = f"https://paytm.com/movies/{city}"
        response = requests.get(main_url)
        response.raise_for_status()  # Raise exception for bad status codes
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find spans where class starts with "RunningMovies_moviesList"
        movie_spans = soup.find_all('span', class_=lambda x: x and x.startswith("RunningMovies_moviesList"))
        
        movies = []
        for span in movie_spans:
            first_div = span.find('div')
            if not first_div:
                continue
            
            movie_divs = first_div.find_all('div', class_=lambda x:x and x.startswith("DesktopRunningMovie_movieCard"))
            for movie_div in movie_divs:
                script_tag = movie_div.find('script', type='application/ld+json')
                if not script_tag:
                    continue
                
                movie_data = json.loads(script_tag.string)
                
                name = movie_data.get("name", "Unknown")
                poster = movie_data.get("image", "Unknown")
                rating = movie_data.get("aggregateRating", {}).get("ratingValue", None)
                languages = movie_data.get("inLanguage", "Unknown")
                
                # Split languages if it's a string, else keep the list
                if isinstance(languages, str):
                    languages = [lang.strip() for lang in languages.split(',')]
                else:
                    languages = languages if isinstance(languages, list) else [languages]
                
                movies.append({
                    "name": name,
                    "poster": poster,
                    "rating": rating,
                    "languages": languages
                })
        
        return movies
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []
