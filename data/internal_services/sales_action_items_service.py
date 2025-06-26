import json
import base64
import random
from itertools import cycle
from data.data_common.utils.persons_utils import get_default_individual_sales_criteria
from googleapiclient.discovery import build
from google.oauth2 import service_account

from common.utils import env_utils
from common.genie_logger import GenieLogger
from data.data_common.data_transfer_objects.profile_category_dto import ProfileCategory, SalesCriteria, SalesCriteriaType
from data.data_common.data_transfer_objects.sales_action_item_dto import SalesActionItem, SalesActionItemCategory
from data.data_common.dependencies.dependencies import tenant_profiles_repository

logger = GenieLogger()

class SalesActionItemsService:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    # Hardcoded Spreadsheet ID and Sheet Name
    SPREADSHEET_ID = "1x8IIiMyW7592yqYnthQ5YXN1rL2C3UN_XsM5_7MmsQg"
    SHEET_NAME = "Sales criteria"
    SALES_CRITERIA_COLUMN = "Sales Criteria"
    ACTION_ITEM_COLUMN = "Action Item"
    DETAILED_ACTION_ITEM_COLUMN = "Detailed Action Item"
    ACTION_ITEM_CATEGORY_COLUMN = "Action Item Category"

    def __init__(self):
        """
        Initialize the service, setting up credentials and the Sheets API service.
        """
        # Load Google Service Account credentials
        encoded_creds = env_utils.get("GOOGLE_SERVICE_JSON")
        if not encoded_creds:
            raise Exception("Environment variable 'GOOGLE_SERVICE_JSON' is not set or is empty.")

        self.google_creds = json.loads(base64.b64decode(encoded_creds).decode("utf-8"))
        if not self.google_creds:
            raise Exception("Failed to decode Google service account credentials.")

        self.credentials = None
        self.service = None
        self.refresh_credentials()
        self.tenant_profiles_repository = tenant_profiles_repository()
        self.data_rows = None
        self.criteria_index = None
        self.action_item_index = None
        self.detailed_item_index = None
        self.category_index = None
        self._initailze_sheet()

    def refresh_credentials(self):
        """
        Reinitialize the credentials to ensure they are always fresh.
        """
        pass

    def _initailze_sheet(self):
        pass

    def get_action_items(self, sales_criteria: list[SalesCriteria]) -> list[SalesActionItem]:
        """
        Get action items for a given sales criteria.
        :param sales_criteria: The sales criteria to filter by.
        :return: A tuple (Action Item, Detailed Action Item) or None if no suggestions are found.
        """
        try:
            # Attempt to fetch action items
            return self._fetch_x_action_items(sales_criteria)
        except Exception as e:
            if "401" in str(e):  # Example: Handle token expiration
                print("Refreshing credentials due to 401 error...")
                self.refresh_credentials()
                return self._fetch_x_action_items(sales_criteria)
            else:
                raise e
            
    def _fetch_x_action_items(self, sales_criteria: list[SalesCriteria], num_items = 5) -> list[SalesActionItem]:
        pass

    def _fetch_action_items(self, sales_criteria: list[SalesCriteria]) -> list[SalesActionItem]:
        pass
    
    # def get_or_create_action_items(self, uuid, tenant_id):
    #     existing_action_items = self.tenant_profiles_repository.get_sales_action_items(uuid, tenant_id)
    #     if not existing_action_items:
    #         action_items = []
    #         for sales_criteria in sales_criterias:
    #             try:
    #                 action_item_text, detailed_action_item_text = self.get_action_items(sales_criteria)
    #             except Exception as e:
    #                 logger.error(f"Error getting action items for {sales_criteria}: {e}")
    #                 continue
    #             if action_item_text:
    #                 action_item = SalesActionItem(
    #                     criteria=sales_criteria.criteria.value,
    #                     action_item=action_item_text,
    #                     detailed_action_item=detailed_action_item_text,
    #                     score=int(sales_criteria.target_score * 0.25) # Placeholder - 25% of the target score
    #                 )
    #                 action_items.append(action_item)
    #         if action_items:
    #             self.tenant_profiles_repository.update_sales_action_items(uuid, tenant_id, action_items)
    #


if __name__ == "__main__":
    # Initialize the service
    sales_service = SalesActionItemsService()

    # Hardcoded sales criteria for testing
    sales_criterias = get_default_individual_sales_criteria(ProfileCategory(category="The Innovator", scores={"Business Fit": 100}, description="Business Fit Description"))
    # Get action items
    try:
        suggestion = sales_service.get_action_items(sales_criterias)
        if suggestion:
            print(f"- Action Item: {suggestion[1].action_item}")
            
            print(f"- Category: {suggestion[1].category}")
        else:
            print(f"No suggestions found.")
    except Exception as e:
        print(f"Error: {e}")
