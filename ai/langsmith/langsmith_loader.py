import asyncio
import json
import os


from common.utils import env_utils

# from ..models import Models
from langchain import hub
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
from data.api_services.embeddings import GenieEmbeddingsClient

logger = GenieLogger()
load_dotenv()


class Langsmith:
    def __init__(self):
        self.api_key = env_utils.get("LANGSMITH_API_KEY")
        self.base_url = "https://api.langsmith.com/v1"
        self.model = ChatOpenAI(model="gpt-4o")
        self.embeddings_client = GenieEmbeddingsClient()

    async def get_profile(self, person_data, company_data=None):
        # Run the two prompts concurrently
        logger.info("Running Langsmith prompts")
        logger.debug(f"Person data: {person_data.keys()}")
        strengths = await self.run_prompt_strength(person_data)
        # news = self.run_prompt_news(person_data)
        # strengths, news = await asyncio.gather(strengths, news)
        person_data["strengths"] = strengths.get("strengths") if strengths.get("strengths") else strengths
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
                "strengths": person_data.get("strengths") if person_data.get("strengths") else "not found",
                "hobbies": person_data.get("hobbies") if person_data.get("hobbies") else "not found",
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
            response = runnable.invoke({"email_address": email_address, "company_data": company_data})
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

    def run_prompt_get_meeting_goals(self, personal_data, my_company_data, seller_context, call_info={}):
        if seller_context:
            prompt = hub.pull("get_meeting_goals_w_context")
        else:
            prompt = hub.pull("get_meeting_goals")
        arguments = {
            "personal_data": personal_data,
            "my_company_data": my_company_data,
            "info": call_info,
            "seller_context" : seller_context
        }
        response = None
        try:
            runnable = prompt | self.model
            response = runnable.invoke(arguments)
        except Exception as e:
            response = f"Error: {e}"
        finally:
            logger.debug(f"Got meeting goals from Langsmith: {response}")
            while True:
                if isinstance(response, dict) and response.get("goals"):
                    response = response.get("goals")
                if isinstance(response, str):
                    response = json.loads(response)
                if isinstance(response, list):
                    break
                if not response:
                    logger.error("Meeting goals response returned None")
                    response = []
            return response

    def run_prompt_get_meeting_guidelines(self, customer_strengths, meeting_details, meeting_goals, seller_context, case={}):
        if seller_context:
            prompt = hub.pull("get_meeting_guidelines_w_context")
        else:
            prompt = hub.pull("get_meeting_guidelines")
        arguments = {
            "customer_strengths": customer_strengths,
            "meeting_details": meeting_details,
            "meeting_goals": meeting_goals,
            "case": case,
            "seller_context" : seller_context
        }

        try:
            runnable = prompt | self.model
            response = runnable.invoke(arguments)
        except Exception as e:
            response = f"Error: {e}"
        finally:
            logger.debug(f"Got meeting guidelines from Langsmith: {response}")
            while True:
                if isinstance(response, dict) and response.get("guidelines"):
                    response = response.get("guidelines") or response.get("data")
                if isinstance(response, str):
                    response = json.loads(response)
                if isinstance(response, list):
                    break
            return response

    def ask_chatgpt(self, prompt):
        try:
            runnable = self.model
            response = runnable.invoke(prompt)
        except Exception as e:
            response = f"Error: {e}"
        return response
