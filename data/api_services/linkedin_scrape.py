from pyarrow import scalar

from common.utils import env_utils
import requests
from typing import List, Union
from pydantic import ValidationError
from datetime import datetime, timedelta

from data.data_common.data_transfer_objects.news_data_dto import SocialMediaPost, NewsData
from common.genie_logger import GenieLogger
from data.data_common.dependencies.dependencies import personal_data_repository

logger = GenieLogger()

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

    def fetch_and_process_posts(self, linkedin_url: str, num_posts=50) -> List[NewsData]:
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
            logger.info(f"Successfully fetched {len(latest_posts)} posts from {linkedin_url}")

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
                        "title": post.get("article_title") or post.get("text", "")[:100],
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
                    logger.info(f"Data dict: {data_dict}")
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
                    logger.info(f"Processed post: {news_data}")
                except Exception as e:
                    logger.error(f"Error processing post {post.get('post_url')}: {e}")
            sorted_processed_posts = self.sort_data_by_preferences(processed_posts, linkedin_url)
            return sorted_processed_posts

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"An error occurred: {err}")

        return []

    def sort_posts_by_date(self, posts: List[Union[SocialMediaPost, NewsData]]) -> List[SocialMediaPost]:
        """
        Sort the posts by date, with the most recent posts first.
        """
        sorted_posts = sorted(posts, key=lambda x: x.date, reverse=True)
        return sorted_posts

    def sort_data_by_preferences(self, posts: List[Union["SocialMediaPost", "NewsData"]], linkedin_url: str) -> List["SocialMediaPost"]:
        """
        Sort the data by preferences:
        1. Posts that are not reshared, but created by the user, in the last 90 days.
        2. Posts that are reshared, in the last 90 days.
        3. Posts that are not reshared, but created by the user, older than 90 days.
        4. Posts that are reshared, older than 90 days.
        """
        ninety_days_ago = datetime.now() - timedelta(days=90)

        # Create empty lists for each category
        recent_original_posts = []
        recent_reshared_posts = []
        older_original_posts = []
        older_reshared_posts = []

        for post in posts:
            try:
                # Parse date with fallback for different formats
                post_date = datetime.strptime(str(post.date), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                post_date = datetime.strptime(str(post.date), "%Y-%m-%d")

            # Determine if the post is created by the user
            is_original = linkedin_url in post.reshared
            is_recent = post_date > ninety_days_ago

            # Categorize the post
            if is_original:
                if is_recent:
                    recent_original_posts.append(post)
                else:
                    older_original_posts.append(post)
            else:
                if is_recent:
                    recent_reshared_posts.append(post)
                else:
                    older_reshared_posts.append(post)

        # Debugging each category
        logger.info(f"Recent Original Posts: {[post.date for post in recent_original_posts]}")
        logger.info(f"Recent Reshared Posts: {[post.date for post in recent_reshared_posts]}")
        logger.info(f"Older Original Posts: {[post.date for post in older_original_posts]}")
        logger.info(f"Older Reshared Posts: {[post.date for post in older_reshared_posts]}")

        sorted_recent_original_posts = sorted(recent_original_posts, key=lambda x: x.date, reverse=True)
        sorted_recent_reshared_posts = sorted(recent_reshared_posts, key=lambda x: x.date, reverse=True)
        sorted_older_original_posts = sorted(older_original_posts, key=lambda x: x.date, reverse=True)
        sorted_older_reshared_posts = sorted(older_reshared_posts, key=lambda x: x.date, reverse=True)

        # Combine lists in order of priority
        sorted_posts = sorted_recent_original_posts + sorted_recent_reshared_posts + sorted_older_original_posts + sorted_older_reshared_posts

        return sorted_posts
