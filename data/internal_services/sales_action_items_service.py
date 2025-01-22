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
        self.credentials = service_account.Credentials.from_service_account_info(
            self.google_creds, scopes=self.SCOPES
        )
        # Reinitialize the Sheets API service
        self.service = build("sheets", "v4", credentials=self.credentials)

    def _initailze_sheet(self):
        range_name = f"{self.SHEET_NAME}!A:E"
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.SPREADSHEET_ID, range=range_name).execute()
        rows = result.get("values", [])

        # Extract headers and data
        if len(rows) < 2:
            raise Exception("Sheet is empty or does not have enough data.")

        headers = rows[0]
        self.data_rows = rows[1:]

        if self.SALES_CRITERIA_COLUMN not in headers or self.ACTION_ITEM_COLUMN not in headers or self.DETAILED_ACTION_ITEM_COLUMN not in headers:
            raise Exception("Required columns are missing.")

        # Get column indices
        self.criteria_index = headers.index(self.SALES_CRITERIA_COLUMN)
        self.action_item_index = headers.index(self.ACTION_ITEM_COLUMN)
        self.detailed_item_index = headers.index(self.DETAILED_ACTION_ITEM_COLUMN)
        self.category_index = headers.index(self.ACTION_ITEM_CATEGORY_COLUMN)

        # Fill down the Sales Criteria for merged cells
        last_criteria = None
        for row in self.data_rows:
            if len(row) > self.criteria_index and row[self.criteria_index].strip():
                last_criteria = row[self.criteria_index].strip()  # Update the last non-empty value
            elif last_criteria:
                # Fill the empty cell with the last non-empty value
                if len(row) <= self.criteria_index:
                    row.extend([""] * (self.criteria_index - len(row) + 1))  # Extend the row if it's too short
                row[self.criteria_index] = last_criteria

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
        action_items = []
        descending_sales_criteria = sorted(sales_criteria, key=lambda x: x.target_score, reverse=True)
        criteria_cycle = cycle(descending_sales_criteria) 
        used_suggestions = set()
        for i in range(num_items):
            sale_criteria = next(criteria_cycle)
            normalized_criteria = sale_criteria.criteria.value.strip().lower().replace("_", " ")

            # Filter rows matching the criteria
            suggestions = [
                (row[self.action_item_index], row[self.detailed_item_index], row[self.category_index] if len(row) > self.category_index else None)
                for row in self.data_rows
                if len(row) > self.criteria_index
                and row[self.criteria_index].strip().lower().replace("_", " ") == normalized_criteria
                and len(row) > self.action_item_index
                and row[self.action_item_index].strip()  # Exclude rows with empty Action Item
                and (row[self.action_item_index], row[self.detailed_item_index], row[self.category_index] if len(row) > self.category_index else None) not in used_suggestions
            ]

            if suggestions:
                action_item_tuple = random.choice(suggestions) 
                used_suggestions.add(action_item_tuple)  
                action_item, detailed_item, category = action_item_tuple
                if category:
                    category = category.strip().upper().replace(" ", "_")
                    action_item_category = SalesActionItemCategory(category)
                action_items.append(SalesActionItem(
                    criteria=sale_criteria.criteria,
                    action_item=action_item,
                    detailed_action_item=detailed_item,
                    score=int(sale_criteria.target_score * 0.25),  # Placeholder - 25% of the target score
                    category=action_item_category if category else SalesActionItemCategory.GENERIC
                ))

        return action_items

    def _fetch_action_items(self, sales_criteria: list[SalesCriteria]) -> list[SalesActionItem]:
        """
        Internal method to fetch action items from the Google Sheet.
        """
        action_items = []
        for sale_criteria in sales_criteria:
            normalized_criteria = sale_criteria.criteria.value.strip().lower().replace("_", " ")

            # Filter rows matching the criteria
            suggestions = [
                (row[self.action_item_index], row[self.detailed_item_index], row[self.category_index] if len(row) > self.category_index else None)
                for row in self.data_rows
                if len(row) > self.criteria_index
                and row[self.criteria_index].strip().lower().replace("_", " ") == normalized_criteria
                and len(row) > self.action_item_index
                and row[self.action_item_index].strip()  # Exclude rows with empty Action Item
            ]

            if suggestions:
                action_item, detailed_item, category = random.choice(suggestions)
                if category:
                    category = category.strip().upper().replace(" ", "_")
                    action_item_category = SalesActionItemCategory(category)
                action_items.append(SalesActionItem(
                    criteria=sale_criteria.criteria,
                    action_item=action_item,
                    detailed_action_item=detailed_item,
                    score=int(sale_criteria.target_score * 0.25),  # Placeholder - 25% of the target score
                    category=action_item_category if category else SalesActionItemCategory.GENERIC
                ))

        return action_items
    
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
