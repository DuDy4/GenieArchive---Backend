import asyncio
import os

from loguru import logger

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

    async def get_profile(self, person_data):
        # Run the two prompts concurrently
        logger.info("Running Langsmith prompts")
        logger.debug(f"Person data: {person_data.keys()}")
        strengths = await self.run_prompt_strength(person_data)
        person_data["strengths"] = (
            strengths.get("strengths") if strengths.get("strengths") else strengths
        )
        get_to_know = await self.run_prompt_get_to_know(person_data)
        person_data["get_to_know"] = get_to_know
        return person_data

    def run_prompt_profile_person(self, person_data):
        prompt = hub.pull("profile_person")
        try:
            runnable = prompt | self.model
            response = runnable.invoke(person_data)
        except Exception as e:
            response = f"Error: {e}"
        return response

    async def run_prompt_strength(self, person_data):
        prompt = hub.pull("get_strengths")
        try:
            runnable = prompt | self.model
            response = runnable.invoke(person_data)
        except Exception as e:
            response = f"Error: {e}"
        logger.debug(f"Got strengths from Langsmith")
        return response

    async def run_prompt_get_to_know(self, person_data):
        prompt = hub.pull("dos-and-donts")
        try:
            runnable = prompt | self.model
            arguments = {
                "personal_data": person_data.get("personal_data")
                if person_data.get("personal_data")
                else "not found",
                "person_background": person_data.get("background")
                if person_data.get("background")
                else "not found",
                "strengths": person_data.get("strengths")
                if person_data.get("strengths")
                else "not found",
                "hobbies": person_data.get("hobbies")
                if person_data.get("hobbies")
                else "not found",
                "news": person_data.get("news")
                if person_data.get("news")
                else "not found",
                "product_data": person_data.get("product_data")
                if person_data.get("product_data")
                else "not found",
                "company_summary": person_data.get("company_summary")
                if person_data.get("company_summary")
                else "not found",
            }
            # logger.debug(f"Arguments for get-to-know: {arguments}")
            response = runnable.invoke(arguments)
            logger.info("Got get-to-know from Langsmith")
            # if response and isinstance(response, dict) and response.get("best_practices") == []:
            #     return await self.run_prompt_get_to_know(person_data)
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
