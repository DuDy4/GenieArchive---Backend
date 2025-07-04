import asyncio
import base64
import datetime
import json
from common.utils import env_utils
from common.genie_logger import GenieLogger
from ai.langsmith.langsmith_loader import Langsmith
from data.api_services.linkedin_scrape import HandleLinkedinScrape
from common.utils.str_utils import fix_linkedin_url
from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
)

logger = GenieLogger()


class ParamsApiService:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    # Hardcoded Spreadsheet ID and Sheet Name
    SPREADSHEET_ID = "10Q0E0pByVGpAsN4XWkmF2SETH9zxam4ax1_rNiIJgFE"
    SHEET_NAME = "Formatted Parameters"
    ID_COLUMN = "ID"
    MIN_RANGE_COLUMN = "Min Range"
    MAX_RANGE_COLUMN = "Max Range"
    MIN_VALUE_COLUMN = "Min Value"
    PARAM_NAME_COLUMN = "Parameter"
    DEFINITION_COLUMN = "Definition"
    PARAM_EXPLANATION_COLUMN = "Range explanation"
    CLUES_COLUMN = "Clues"

    def __init__(self):
        """
        Initialize the service, setting up credentials and the Sheets API service.
        """
        # Load Google Service Account credentials
        self.langsmith = Langsmith()
        encoded_creds = env_utils.get("GOOGLE_SERVICE_JSON")
        if not encoded_creds:
            raise Exception("Environment variable 'GOOGLE_SERVICE_JSON' is not set or is empty.")

        self.google_creds = json.loads(base64.b64decode(encoded_creds).decode("utf-8"))
        if not self.google_creds:
            raise Exception("Failed to decode Google service account credentials.")

        self.credentials = None
        self.service = None
        self.refresh_credentials()
        self.data_rows = None
        self.id_column_index = None
        self.min_range_column_index = None
        self.max_range_column_index = None
        self.min_value_column_index = None
        self.param_name_column_index = None
        self.definition_column_index = None
        self.param_explanation_column_index = None
        self.clues_column_index = None
        # asyncio.run(self._initailze_sheet())
        self.linkedin_scrapper = HandleLinkedinScrape()
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()

    def refresh_credentials(self):
        """
        Reinitialize the credentials to ensure they are always fresh.
        """
        pass

    async def _initialize_sheet(self):
        range_name = f"{self.SHEET_NAME}!A:I"
        sheet = self.service.spreadsheets()
        # values = sheet.values()
        # raw_sheet = values.get(spreadsheetId=self.SPREADSHEET_ID, range=range_name)
        raw_sheet = sheet.values().get(
            spreadsheetId=self.SPREADSHEET_ID,
            range=range_name,
            majorDimension="ROWS",
            valueRenderOption="FORMATTED_VALUE"
        )
        result = raw_sheet.execute()
        # result = sheet.values().get(spreadsheetId=self.SPREADSHEET_ID, range=range_name).execute()
        rows = result.get("values", [])

        num_columns = 9
        logger.info(f"Number of columns: {num_columns}")
        # Check if the sheet has enough columns
        rows = [row + [""] * (num_columns - len(row)) for row in rows]

        # Extract headers and data
        if len(rows) < 2:
            raise Exception("Sheet is empty or does not have enough data.")

        headers = rows[4]
        self.data_rows = rows[5:]

        self.id_column_index = headers.index(self.ID_COLUMN)
        self.min_range_column_index = headers.index(self.MIN_RANGE_COLUMN)
        self.max_range_column_index = headers.index(self.MAX_RANGE_COLUMN)
        self.min_value_column_index = headers.index(self.MIN_VALUE_COLUMN)
        self.param_name_column_index = headers.index(self.PARAM_NAME_COLUMN)
        self.definition_column_index = headers.index(self.DEFINITION_COLUMN)
        self.param_explanation_column_index = headers.index(self.PARAM_EXPLANATION_COLUMN)
        self.clues_column_index = headers.index(self.CLUES_COLUMN)


        # Fill down the Sales Criteria for merged cells
        # last_param = None
        # for row in self.data_rows:
        #     if len(row) > self.criteria_index and row[self.criteria_index].strip():
        #         last_param = row[self.criteria_index].strip()  # Update the last non-empty value
        #     elif last_param:
        #         # Fill the empty cell with the last non-empty value
        #         if len(row) <= self.criteria_index:
        #             row.extend([""] * (self.criteria_index - len(row) + 1))  # Extend the row if it's too short
        #         row[self.criteria_index] = last_param

    async def evaluate_param(self, post, name, position, company, param_id):
        person = {
            'name': name,
            'position': position,
            'company': company,
        }
        for row in self.data_rows:
            if row[self.id_column_index] == param_id:
                logger.info(f"Found parameter in sheet. Row: {row}")
                param_name = row[self.param_name_column_index]
                min_range = row[self.min_range_column_index]
                max_range = row[self.max_range_column_index]
                param_explanation = row[self.param_explanation_column_index]
                clues_list = row[self.clues_column_index].split(";")
                break
        param_data = {
            'param_name': param_name,
            'min_range': min_range,
            'max_range': max_range,
            'explanation': param_explanation,
            'clues': clues_list,
        }
        response = await self.langsmith.get_param_evaluation(person, param_data, post)
        if response:
            response_dict = { 'param': param_name, 'param_id': param_id }
            response['param'] = param_name
            for i, response_clue in enumerate(response['clues']):
                response_clue['clue'] = clues_list[i]
            response_dict.update(response)
            return response_dict
        return {}
    
    async def evaluate_posts(self, linkedin_url, num_posts, name, selected_params):
        await self._initialize_sheet()
        posts = await self.fetch_linkedin_posts(linkedin_url, int(num_posts), name)

        # Create a list to store all evaluation tasks
        evaluation_tasks = []
        
        # Generate tasks for each post and parameter combination
        for post in posts:
            post_tasks = [
                self.evaluate_param(post.text, name, "", "", param_id) 
                for param_id in selected_params
            ]
            evaluation_tasks.extend(post_tasks)
        
        # Run all evaluation tasks simultaneously
        responses_raw = asyncio.as_completed(*evaluation_tasks)
        
        # Filter out None responses and construct post data
        responses = []
        for i, response in enumerate(responses_raw):
            if response:
                post_index = i // len(selected_params)
                param_index = i % len(selected_params)
                
                post_data = {
                    "Full Name": name,
                    'post': posts[post_index].to_dict(),
                    'params': response,
                }
                responses.append(post_data)
        
        return responses
        # for post in posts:
        #     for param_id in selected_params:
        #         response = await self.evaluate_param(post.text, name, "", "", param_id)
        #         if response:
        #             post_data = {
        #                 "Full Name": name,
        #                 'post': post.to_dict(),
        #                 'params': response,
        #             }
        #             responses.append(post_data)
        # return responses
    
    async def fetch_linkedin_posts(self, linkedin_url, num_posts, name):
        linkedin_url = fix_linkedin_url(linkedin_url)
        uuid = "123-" + linkedin_url
        posts = []
        if self.personal_data_repository.exists_linkedin_url(linkedin_url):
            posts = self.personal_data_repository.get_news_data_by_linkedin(linkedin_url)
        else:
            posts = self.linkedin_scrapper.fetch_and_process_posts(linkedin_url, num_posts)
            self.personal_data_repository.insert(uuid=uuid, name=name, linkedin_url=linkedin_url)
            self.personal_data_repository.update_news_list_to_db(uuid, posts)

        return posts