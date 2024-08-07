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

    async def get_profile(self, person_data, company_data=None):
        # Run the two prompts concurrently
        logger.info("Running Langsmith prompts")
        logger.debug(f"Person data: {person_data.keys()}")
        strengths = await self.run_prompt_strength(person_data)
        # news = self.run_prompt_news(person_data)
        # strengths, news = await asyncio.gather(strengths, news)
        person_data["strengths"] = (
            strengths.get("strengths") if strengths.get("strengths") else strengths
        )
        get_to_know = await self.run_prompt_get_to_know(person_data, company_data)
        person_data["get_to_know"] = get_to_know
        return person_data

    def run_prompt_profile_person(self, person_data):
        prompt = hub.pull("profile_person")
        try:
            runnable = prompt | self.model
            response = runnable.invoke(person_data)
        except Exception as e:
            response = f"Error: {e}"
        if response.get("news"):
            response = response.get("news")
        return response

    async def run_prompt_strength(self, person_data):
        prompt = hub.pull("get_strengths")
        try:
            runnable = prompt | self.model
            response = runnable.invoke(person_data)
            if response and isinstance(response, list) and len(response) == 0:
                response = runnable.invoke(person_data)
        except Exception as e:
            response = f"Error: {e}"
        logger.debug(f"Got strengths from Langsmith")
        return response

    # async def run_prompt_news(self, person_data):
    #     prompt = hub.pull("get_news")
    #     try:
    #         runnable = prompt | self.model
    #         response = runnable.invoke(person_data)
    #         if response.get("news"):
    #             response = response.get("news")
    #             if response and isinstance(response, list) and len(response) == 0:
    #                 response = runnable.invoke(person_data)
    #                 if response.get("news"):
    #                     response = response.get("news")
    #     except Exception as e:
    #         response = f"Error: {e}"
    #     logger.debug(f"Got news from Langsmith: {response}")
    #     return response

    async def run_prompt_get_to_know(self, person_data, company_data=None):
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
                "news": company_data.get("news")
                if company_data and company_data.get("news")
                else "not found",
                "product_data": person_data.get("product_data")
                if person_data.get("product_data")
                else "not found",
                "company_data": company_data if company_data else "not found",
            }
            # logger.debug(f"Arguments for get-to-know: {arguments}")
            response = runnable.invoke(arguments)
            logger.debug(f"Response from get-to-know: {response}")
            if (
                response
                and isinstance(response, dict)
                and (
                    response.get("best_practices") == []
                    or isinstance(response.get("best_practices"), dict)
                    or response.get("avoid") == []
                    or response.get("phrases_to_use") == []
                )
            ):
                logger.warning("Got wrong get-to-know from Langsmith - trying again")
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

    def run_prompt_company_overview_challenges(self, company_data):
        logger.info("Running Langsmith prompt for company overview and challenges")
        logger.debug(f"Company data: {company_data}")

        prompt = hub.pull("get_company_overview")
        try:
            runnable = prompt | self.model
            response = runnable.invoke(company_data)
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
