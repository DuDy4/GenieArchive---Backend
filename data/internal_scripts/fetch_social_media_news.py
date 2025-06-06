import sys
import os
import requests

from data.data_common.data_transfer_objects.news_data_dto import NewsData
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.personal_data_repository import PersonalDataRepository

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger

from data.api_services.linkedin_scrape import HandleLinkedinScrape
from data.data_common.dependencies.dependencies import personal_data_repository

logger = GenieLogger()
linkedin_scrapper = HandleLinkedinScrape()
personal_data_repository = personal_data_repository()


def get_all_uuids_that_should_try_posts():
    all_personal_data_uuid = personal_data_repository.get_all_uuids_that_should_try_fetch_posts()
    return all_personal_data_uuid

def get_all_future_uuids_that_should_try_posts():
    future_emails = personal_data_repository.get_future_profiles_without_news()
    return future_emails


def fetch_linkedin_posts(uuids: list, scrap_num=5):
    for uuid in uuids[:scrap_num]:
        try:
            linkedin_url = personal_data_repository.get_linkedin_url(uuid)
            if not linkedin_url:
                logger.error(f"Person with uuid {uuid} has no linkedin_url")
                continue
            logger.info(f"Fetching posts for {linkedin_url}")

            # should_fetch = personal_data_repository.should_do_linkedin_posts_lookup(uuid)
            should_not_fetch = personal_data_repository.get_news_data_by_uuid(uuid)
            logger.info(f"Should fetch posts: {should_not_fetch}")
            if should_not_fetch:
                logger.info(f"Skipping fetching posts for {linkedin_url}")
                continue

            scraped_posts = linkedin_scrapper.fetch_and_process_posts(linkedin_url)

            if not scraped_posts:
                logger.error(f"No posts found or an error occurred while scraping {linkedin_url}")
                # before updating the status to TRIED_BUT_FAILED, check if there are any posts in the database

                personal_data_repository.update_news_to_db(
                    uuid, None, PersonalDataRepository.TRIED_BUT_FAILED
                )
                logger.error(f"Updated status to TRIED_BUT_FAILED for {linkedin_url}")

            logger.info(f"Successfully scraped {len(scraped_posts)} posts from LinkedIn URL: {linkedin_url}")
            news_data_objects = []
            news_in_database = personal_data_repository.get_news_data_by_uuid(uuid)

            for post in scraped_posts:
                if not post:
                    logger.error(f"Post is empty: {post}")
                    continue
                if news_in_database and post in news_in_database:
                    logger.info(f"Post already in database: {post}")
                    continue
                news_data_objects.append(post)
            final_news_data_list = list(set(news_data_objects + news_in_database))
            if final_news_data_list:
                personal_data_repository.update_news_list_to_db(uuid, final_news_data_list, PersonalDataRepository.FETCHED)
            # if news_data_objects:
            #     event = GenieEvent(Topic.NEW_PERSONAL_DATA, {"person_uuid": uuid, "force": True}, "public")
            #     event.send()
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            news_in_db = personal_data_repository.get_news_data_by_uuid(uuid)
            if not news_in_db:
                personal_data_repository.update_news_to_db(
                    uuid, None, PersonalDataRepository.TRIED_BUT_FAILED
                )
        except Exception as e:
            logger.error(f"Error sending event for {uuid}: {e}")
            continue

def create_summary_to_existing_posts(uuids: list):
    for uuid in uuids:
        news_data_objects = personal_data_repository.get_news_data_by_uuid(uuid)
        if not news_data_objects:
            logger.error(f"No news data found for uuid: {uuid}")
            continue
        news_data_objects = [news.to_dict() if not news.summary else None for news in news_data_objects]
        news_data_objects = [news for news in news_data_objects if news]
        event = GenieEvent(
            Topic.NEW_PERSONAL_DATA,
            data={"uuid": uuid, "news_data": news_data_objects},
        )
        event.send()
        logger.info(f"Successfully sent event for {uuid}")


# if __name__ == "__main__":
    # all_uuids = get_all_uuids_that_should_try_posts()
    # uuids = get_all_future_uuids_that_should_try_posts()
    # logger.info(f"Fetching posts for {len(uuids)} profiles")
    # posts = fetch_linkedin_posts(uuids, 13)
    # create_summary_to_existing_posts(uuids)
