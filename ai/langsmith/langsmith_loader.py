import os

from ..models import Models
from langchain import hub
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

load_dotenv()


class Langsmith:
    def __init__(self):
        self.api_key = os.environ.get("LANGSMITH_API_KEY")
        self.base_url = "https://api.langsmith.com/v1"
        self.model = ChatOpenAI(model=Models.GPT_4O)

    def run_prompt_profile_person(self, person_data):
        prompt = hub.pull("profile_person")
        try:
            runnable = prompt | self.model
            response = runnable.invoke({person_data})
        except Exception as e:
            response = f"Error: {e}"
        return response

    def run_prompt_linkedin_url(self, email_address, company_data=None):
        prompt = hub.pull("linkedin_from_email_and_company")
        try:
            runnable = prompt | self.model
            response = runnable.invoke(
                {"email_address": email_address, "company_data": company_data}
            )
        except Exception as e:
            response = f"Error: {e}"
        return response

    def ask_chatgpt(self, prompt):
        try:
            runnable = self.model
            response = runnable.invoke(prompt)
        except Exception as e:
            response = f"Error: {e}"
        return response
