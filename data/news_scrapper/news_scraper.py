import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import json
from loguru import logger

from common.utils import env_utils

# Load environment variables
load_dotenv()

RAPID_API_KEY = env_utils.get("RAPID_API_KEY")
RAPID_API_HOST = env_utils.get("RAPID_API_HOST")
RAPID_API_URL = env_utils.get("RAPID_API_URL")


class NewsScrapper:
    def __init__(self):
        self.api_key = RAPID_API_KEY
        self.api_host = RAPID_API_HOST
        self.url = RAPID_API_URL

    async def get_news(self, company_name, limit=10):
        querystring = {"keyword": company_name, "lr": "en-US"}
        headers = {"x-rapidapi-key": self.api_key, "x-rapidapi-host": self.api_host}

        try:
            response = requests.get(self.url, headers=headers, params=querystring)
            logger.debug(f"Response: {response}")
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()
            logger.debug(f"Response: {data}")

            news_articles = []
            for article in data.get("items", [])[:limit]:  # Process up to the specified limit
                timestamp = article.get("timestamp", "0")
                if isinstance(timestamp, str):
                    timestamp = int(timestamp)

                published_date = datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")
                logger.debug(f"Published Date: {published_date}")
                news_articles.append(
                    {
                        "date": published_date,
                        "link": article.get("newsUrl", ""),
                        "media": article.get("publisher", ""),
                        "title": article.get("title", ""),
                        "summary": article.get("snippet", ""),
                    }
                )

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

    async def is_news_outdated(self, news) -> bool:
        # Needs to be implemented
        return False
