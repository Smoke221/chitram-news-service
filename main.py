import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["chitram"]
collection = db["articles"]

# Function to summarize text
def summarize_text(text, sentence_count=3):
    parser = PlaintextParser.from_string(text, Tokenizer("english"))
    summarizer = LsaSummarizer()
    summary = summarizer(parser.document, sentence_count)
    return " ".join(str(sentence) for sentence in summary)

# URL of the main articles page
main_url = 'https://www.gulte.com/movienews'

# Fetch the main page
response = requests.get(main_url)

soup = BeautifulSoup(response.text, 'html.parser')

# Find all articles
articles = soup.find_all('div', class_='post-thumbnail')

for article in articles[1:]:
    a_tag = article.find('a')
    
    if a_tag and 'href' in a_tag.attrs:
        article_url = a_tag['href']

        # Check if article is already in MongoDB
        if collection.find_one({"url": article_url}):
            print(f"Skipping (Already Scraped): {article_url}")
            continue

        print(f"Scraping: {article_url}")
        
        # Fetch the article page
        article_response = requests.get(article_url)
        article_soup = BeautifulSoup(article_response.text, 'html.parser')

        # Extract data from div with class 'post-inner'
        post_inner = article_soup.find('div', class_='post-inner')

        if post_inner:
            # Title (inside <h1>)
            title_tag = post_inner.find('h1')
            title = title_tag.text.strip() if title_tag else "No Title"

            # Image (inside div class 'single-post-thumb', <img> tag)
            image_div = post_inner.find('div', class_='single-post-thumb')
            image_tag = image_div.find('img') if image_div else None
            image_url = image_tag['src'] if image_tag and 'src' in image_tag.attrs else "No Image"

            # Article Content (inside div class 'entry', all <p> tags)
            entry_div = post_inner.find('div', class_='entry')
            paragraphs = entry_div.find_all('p') if entry_div else []
            content = "\n".join(p.text.strip() for p in paragraphs)

            # Store in MongoDB
            collection.insert_one({
                "url": article_url,
                "title": title,
                "image_url": image_url,
                "content": content
            })
            print(f"Stored: {title}")

print("Scraping completed!")


# 
# import requests
# from bs4 import BeautifulSoup
# from pymongo import MongoClient

# # MongoDB setup
# client = MongoClient("mongodb+srv://smoke:smoke@cluster0.tye86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
# db = client["chitram"]
# collection = db["articles"]

# # URL of the main articles page
# main_url = 'https://www.gulte.com/movienews'

# # Fetch the main page
# response = requests.get(main_url)

# soup = BeautifulSoup(response.text, 'html.parser')

# # Find all articles
# articles = soup.find_all('div', class_='post-thumbnail')[1:]

# for article in articles:
#     a_tag = article.find('a')
#     if not a_tag or 'href' not in a_tag.attrs:
#         continue  # Skip if no valid link

#     article_url = a_tag['href']

#     # Check if article exists in MongoDB
#     existing_article = collection.find_one({"url": article_url})
    
#     if existing_article:
#         print('existing url')
#         # If the article exists, check for missing image
#         if not existing_article.get("image_url"):
#             print('came here...')
#             response = requests.get(article_url)
#             article_soup = BeautifulSoup(response.text, 'html.parser')

#             image_tag = article_soup.find('div', class_='single-post-thumb')
#             image_url = image_tag.find('img')['src'] if image_tag and image_tag.find('img') else None
            
#             if image_url:
#                 collection.update_one({"url": article_url}, {"$set": {"image_url": image_url}})
#         continue  # Skip to next article

#     # Fetch the article page
#     response = requests.get(article_url)
#     article_soup = BeautifulSoup(response.text, 'html.parser')

#     # Extract title
#     title_tag = article_soup.find('div', class_='post-inner').find('h1')
#     title = title_tag.text.strip() if title_tag else "No Title"

#     # Extract image URL
#     image_tag = article_soup.find('div', class_='single-post-thumb')
#     image_url = image_tag.find('img')['src'] if image_tag and image_tag.find('img') else None

#     # Extract body content
#     body_div = article_soup.find('div', class_='entry')
#     paragraphs = body_div.find_all('p') if body_div else []
#     body_text = "\n".join(p.text.strip() for p in paragraphs)

#     # Save to MongoDB
#     article_data = {
#         "url": article_url,
#         "title": title,
#         "image_url": image_url,
#         "body": body_text
#     }
#     collection.insert_one(article_data)

# print("Scraping completed!")

# 