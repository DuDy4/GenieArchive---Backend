import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import json
from loguru import logger

# Load environment variables
load_dotenv()

class NewsWrapper:
    def __init__(self):
        self.api_key = os.getenv('RAPIDAPI_KEY')
        self.api_host = "google-news13.p.rapidapi.com"
        self.url = "https://google-news13.p.rapidapi.com/search"

    def get_news(self, company_name, limit=2):
        querystring = {"keyword": company_name, "lr": "en-US"}
        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.api_host
        }

        try:
            response = requests.get(self.url, headers=headers, params=querystring)
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()

            news_articles = []
            for article in data.get('items', [])[:limit]:  # Process up to the specified limit
                timestamp = article.get('timestamp', "0")
                if isinstance(timestamp, str):
                    timestamp = int(timestamp)

                published_date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                news_articles.append({
                    "date": published_date,
                    "link": article.get('newsUrl', ''),
                    "media": article.get('publisher', ''),
                    "title": article.get('title', ''),
                    "summary": article.get('snippet', '')
                })

            if not news_articles:
                logger.warning(f"No news articles found for {company_name}")

            return {"news": news_articles}

        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to API: {e}")
            return {"error": str(e), "news": []}

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {e}")
            return {"error": "Invalid JSON response", "news": []}

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {"error": "An unexpected error occurred", "news": []}

if __name__ == "__main__":
    wrapper = NewsWrapper()
    company_name = "mabl"  # You can change this to any company name
    result = wrapper.get_news(company_name)  # This will now return up to 2 articles by default

    # Log the formatted JSON output
    logger.info(f"Formatted JSON Output: {json.dumps(result, indent=2)}")

    if not result['news']:
        logger.info("No news articles found. Trying an alternative search...")
        # You could try an alternative search here, e.g., with a different keyword
        alternative_result = wrapper.get_news("technology news")
        logger.info(f"Alternative JSON Output: {json.dumps(alternative_result, indent=2)}")
