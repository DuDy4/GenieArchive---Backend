from common.utils import env_utils
from data.data_common.data_transfer_objects.company_dto import NewsData
import requests
import os
from loguru import logger
from dotenv import load_dotenv
from typing import List
from pydantic import ValidationError
from datetime import datetime


load_dotenv()

RAPID_API_KEY = env_utils.get("RAPID_API_KEY")


class HandleLinkedinScrape:
    def __init__(self):
        self.api_key = RAPID_API_KEY
        self.base_url = "https://fresh-linkedin-profile-data.p.rapidapi.com/get-profile-posts"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "fresh-linkedin-profile-data.p.rapidapi.com",
        }

        # self.result = self.fetch_and_process_posts(linkedin_url, num_posts=3)

    def fetch_and_process_posts(self, linkedin_url: str, num_posts=3) -> List[NewsData]:
        """
        Fetch posts from LinkedIn and process them into NewsData objects, handling multiple image URLs.
        """
        logger.info(f"Fetching the latest {num_posts} posts from: {linkedin_url}")
        querystring = {"linkedin_url": linkedin_url, "type": "posts"}
        if not self.api_key:
            logger.error("API key not found in environment variables")
            return []

        try:
            response = requests.get(self.base_url, headers=self.headers, params=querystring)
            response.raise_for_status()  # Raise exception for HTTP errors
            data = response.json()
            latest_posts = data.get("data", [])[:num_posts]
            logger.success(f"Successfully fetched {len(latest_posts)} posts from {linkedin_url}")

            processed_posts = []
            for post in latest_posts:
                try:

                    images = post.get("images", [])
                    image_urls = [img["url"] for img in images if "url" in img]

                    news_data_dict = {
                        "date": datetime.strptime(post.get("posted"), "%Y-%m-%d %H:%M:%S").date(),
                        "link": post.get("post_url"),
                        "media": "LinkedIn",
                        "title": post.get("text", "")[:100],
                        "summary": post.get("text", None),
                        "image_urls": image_urls,
                    }

                    news_data = NewsData.from_dict(news_data_dict)
                    processed_posts.append(news_data)

                except ValidationError as e:
                    logger.error(f"Validation error for post {post.get('post_url')}: {e}")
                except Exception as e:
                    logger.error(f"Error processing post {post.get('post_url')}: {e}")

            return processed_posts

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"An error occurred: {err}")

        return []
