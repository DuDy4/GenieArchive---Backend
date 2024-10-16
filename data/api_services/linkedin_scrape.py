from common.utils import env_utils
from data.data_common.data_transfer_objects.company_dto import NewsData
import requests
import os
from loguru import logger
from dotenv import load_dotenv
from typing import List, Dict, Any
from pydantic import ValidationError, HttpUrl
from datetime import datetime, timedelta

from data.data_common.data_transfer_objects.news_data_dto import SocialMediaPost

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

    def fetch_and_process_posts(self, linkedin_url: str, num_posts=5) -> List[NewsData]:
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
                logger.info(f"Processing post: {post}")
                try:
                    images = post.get("images", [])
                    image_urls = [img["url"] for img in images if "url" in img]

                    data_dict = {
                        "date": datetime.strptime(post.get("posted"), "%Y-%m-%d %H:%M:%S").date(),
                        "link": post.get("post_url"),
                        "media": "LinkedIn",
                        "title": post.get("text", "")[:100],
                        "text": post.get("text", None),
                        "reshared": post.get("poster_linkedin_url")
                        if post.get("poster_linkedin_url") != linkedin_url
                        else None,
                        "likes": (
                            post.get("num_appreciations", 0)
                            + post.get("num_empathy", 0)
                            + post.get("num_likes", 0)
                            + post.get("num_praises", 0)
                        ),
                        "images": image_urls,
                    }
                    try:
                        news_data = SocialMediaPost.from_dict(data_dict)
                    except ValidationError as e:
                        logger.error(f"Validation error for post {post.get('post_url')}: {e}")
                        try:
                            news_data = NewsData.from_dict(data_dict)
                        except ValidationError as e:
                            logger.error(f"Validation error for post {post.get('post_url')}: {e}")
                            continue
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

    def sort_data_by_preferences(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sort the data by preferences:
        1. Posts that are not reshared, but created by the user, in the last 90 days.
        2. Posts that are reshared, in the last 90 days.
        3. Posts that are not reshared, but created by the user, older than 90 days.
        4. Posts that are reshared, older than 90 days.
        """
        posts = data.get("data", [])
        if not posts:
            return data

        # Define a 90-day window
        ninety_days_ago = datetime.now() - timedelta(days=90)

        # Create different lists for the four categories
        recent_original_posts = []
        recent_reshared_posts = []
        older_original_posts = []
        older_reshared_posts = []

        # Sort posts into respective lists based on reshared status and date
        for post in posts:
            # Parse the 'posted' date from the post
            post_date = datetime.strptime(post.get("posted", ""), "%Y-%m-%d %H:%M:%S")

            # Check if the post is reshared and its recency
            if post.get("reshared"):
                if post_date > ninety_days_ago:
                    recent_reshared_posts.append(post)
                else:
                    older_reshared_posts.append(post)
            else:
                if post_date > ninety_days_ago:
                    recent_original_posts.append(post)
                else:
                    older_original_posts.append(post)

        # Concatenate all sorted categories in order
        sorted_posts = (
            recent_original_posts + recent_reshared_posts + older_original_posts + older_reshared_posts
        )
        data["data"] = sorted_posts

        return data
