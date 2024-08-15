from apify_client import ApifyClient
import os
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
logger = GenieLogger()
from azure.monitor.opentelemetry import configure_azure_monitor
configure_azure_monitor()
from common.utils import env_utils

load_dotenv()

API_KEY = env_utils.get("FACEBOOK_WRAP_API_TOKEN")
ACTOR_ID = env_utils.get("FACEBOOK_WRAP_ACTOR_ID")


class FacebookWrapper:
    def __init__(self):
        self.api_token = API_KEY
        self.actor_id = ACTOR_ID
        self.client = ApifyClient(self.api_token)

    def start_actor(self, run_input):
        logger.info(f"Starting actor with input: {run_input}")
        actor_run = self.client.actor(self.actor_id).call(run_input=run_input)
        logger.info(f"Actor run response: {actor_run}")
        return actor_run

    def fetch_dataset_items(self, dataset_id):
        dataset_items = self.client.dataset(dataset_id).list_items().items
        if dataset_items:
            logger.info(f"Fetched {dataset_items} dataset items")
            if dataset_items[0].get("error"):
                logger.warning("Failed to fetch dataset items")
            return dataset_items[0]
        return None

    def run_actor_and_fetch_results(self, facebook_url):
        run_input = {"startUrls": [{"url": facebook_url}]}
        logger.info(f"Starting actor with input: {run_input}")
        actor_run = self.start_actor(run_input)
        logger.info(f"Actor run response: {actor_run}")
        dataset_id = actor_run["defaultDatasetId"]
        return self.fetch_dataset_items(dataset_id)
