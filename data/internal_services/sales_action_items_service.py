import os
import json
import base64
import random
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Load environment variables from .env file
load_dotenv()

from data.data_common.dependencies.dependencies import tenant_profiles_repository

class SalesActionItemsService:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    # Hardcoded Spreadsheet ID and Sheet Name
    SPREADSHEET_ID = "1x8IIiMyW7592yqYnthQ5YXN1rL2C3UN_XsM5_7MmsQg"
    SHEET_NAME = "Sales criteria"
    SALES_CRITERIA_COLUMN = "Sales Criteria"
    ACTION_ITEM_COLUMN = "Action Item"
    DETAILED_ACTION_ITEM_COLUMN = "Detailed Action Item"

    def __init__(self):
        """
        Initialize the service, setting up credentials and the Sheets API service.
        """
        # Load Google Service Account credentials
        encoded_creds = os.getenv("GOOGLE_SERVICE_JSON")
        if not encoded_creds:
            raise Exception("Environment variable 'GOOGLE_SERVICE_JSON' is not set or is empty.")

        self.google_creds = json.loads(base64.b64decode(encoded_creds).decode("utf-8"))
        if not self.google_creds:
            raise Exception("Failed to decode Google service account credentials.")

        self.credentials = None
        self.service = None
        self.refresh_credentials()
        self.tenant_profiles_repository = tenant_profiles_repository()

    def refresh_credentials(self):
        """
        Reinitialize the credentials to ensure they are always fresh.
        """
        self.credentials = service_account.Credentials.from_service_account_info(
            self.google_creds, scopes=self.SCOPES
        )
        # Reinitialize the Sheets API service
        self.service = build("sheets", "v4", credentials=self.credentials)

    def get_action_items(self, sales_criteria):
        """
        Get action items for a given sales criteria.
        :param sales_criteria: The sales criteria to filter by.
        :return: A tuple (Action Item, Detailed Action Item) or None if no suggestions are found.
        """
        try:
            # Attempt to fetch action items
            return self._fetch_action_items(sales_criteria)
        except Exception as e:
            if "401" in str(e):  # Example: Handle token expiration
                print("Refreshing credentials due to 401 error...")
                self.refresh_credentials()
                return self._fetch_action_items(sales_criteria)
            else:
                raise e

    def _fetch_action_items(self, sales_criteria):
        """
        Internal method to fetch action items from the Google Sheet.
        """
        range_name = f"{self.SHEET_NAME}!A:D"
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.SPREADSHEET_ID, range=range_name).execute()
        rows = result.get("values", [])

        # Extract headers and data
        if len(rows) < 2:
            raise Exception("Sheet is empty or does not have enough data.")

        headers = rows[0]
        data_rows = rows[1:]

        if self.SALES_CRITERIA_COLUMN not in headers or self.ACTION_ITEM_COLUMN not in headers or self.DETAILED_ACTION_ITEM_COLUMN not in headers:
            raise Exception("Required columns are missing.")

        # Get column indices
        criteria_index = headers.index(self.SALES_CRITERIA_COLUMN)
        action_item_index = headers.index(self.ACTION_ITEM_COLUMN)
        detailed_item_index = headers.index(self.DETAILED_ACTION_ITEM_COLUMN)

        # Fill down the Sales Criteria for merged cells
        last_criteria = None
        for row in data_rows:
            if len(row) > criteria_index and row[criteria_index].strip():
                last_criteria = row[criteria_index].strip()  # Update the last non-empty value
            elif last_criteria:
                # Fill the empty cell with the last non-empty value
                if len(row) <= criteria_index:
                    row.extend([""] * (criteria_index - len(row) + 1))  # Extend the row if it's too short
                row[criteria_index] = last_criteria

        # Normalize the sales criteria
        if isinstance(sales_criteria, str):
            criteria = sales_criteria
        else:
            criteria = sales_criteria.criteria.value
        normalized_criteria = criteria.strip().lower().replace("_", " ")

        # Filter rows matching the criteria
        suggestions = [
            (row[action_item_index], row[detailed_item_index] if len(row) > detailed_item_index else None)
            for row in data_rows
            if len(row) > criteria_index
            and row[criteria_index].strip().lower().replace("_", " ") == normalized_criteria
            and len(row) > action_item_index
            and row[action_item_index].strip()  # Exclude rows with empty Action Item
        ]

        return random.choice(suggestions) if suggestions else None
    
    # def get_or_create_action_items(uuid, tenant_id):
    #     existing_action_items = self.tenant_profiles_repository.get_sales_action_items(person['uuid'], seller_tenant_id)
    #     if not existing_action_items:
    #         action_items = []
    #         for sales_criteria in sales_criterias:
    #             try:
    #                 action_item_text, detailed_action_item_text = self.sales_action_items_service.get_action_items(sales_criteria)
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
    #             self.tenant_profiles_repository.update_sales_action_items(person['uuid'], seller_tenant_id, action_items)
    #


if __name__ == "__main__":
    # Initialize the service
    sales_service = SalesActionItemsService()

    # Hardcoded sales criteria for testing
    SALES_CRITERIA = "Budget"  # Test case with underscores and merged cells

    # Get action items
    try:
        suggestion = sales_service.get_action_items(SALES_CRITERIA)
        if suggestion:
            action_item, detailed_item = suggestion
            print(f"Suggestion for '{SALES_CRITERIA}':")
            print(f"- Action Item: {action_item}")
            print(f"- Detailed Action Item: {detailed_item}")
        else:
            print(f"No suggestions found for '{SALES_CRITERIA}'.")
    except Exception as e:
        print(f"Error: {e}")
