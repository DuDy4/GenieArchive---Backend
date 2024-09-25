import asyncio
import json
import os
import random

from common.utils import env_utils

# from ..models import Models
from langchain import hub
from langchain_openai import ChatOpenAI
from langsmith.utils import LangSmithConnectionError
from dotenv import load_dotenv
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()


class Langsmith:
    def __init__(self):
        self.api_key = env_utils.get("LANGSMITH_API_KEY")
        self.base_url = "https://api.langsmith.com/v1"
        self.model = ChatOpenAI(model="gpt-4o")

    async def get_profile(self, person_data, company_data=None):
        # Run the two prompts concurrently
        logger.info("Running Langsmith prompts")
        logger.debug(f"Person data: {person_data.keys()}")
        strengths = await self.run_prompt_strength(person_data)
        logger.info(f"Strengths from Langsmith: {strengths}")
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
        runnable = prompt | self.model
        arguments = person_data

        try:
            response = await self._run_prompt_with_retry(runnable, arguments)
        except Exception as e:
            response = f"Error: {e}"

        logger.debug(f"Got strengths from Langsmith: {response}")
        return response

    async def run_prompt_get_to_know(self, person_data, company_data=None):
        prompt = hub.pull("dos-and-donts")
        runnable = prompt | self.model
        arguments = {
            "personal_data": person_data.get("personal_data", "not found"),
            "person_background": person_data.get("background", "not found"),
            "strengths": person_data.get("strengths", "not found"),
            "hobbies": person_data.get("hobbies", "not found"),
            "news": company_data.get("news", "not found") if company_data else "not found",
            "product_data": person_data.get("product_data", "not found"),
            "company_data": company_data if company_data else "not found",
        }
        try:
            response = await self._run_prompt_with_retry(runnable, arguments)
            logger.debug(f"Response from get-to-know: {response}")
            for i in range(5):
                if (
                    response
                    and isinstance(response, dict)
                    and (
                        response == {}
                        or (
                            response.get("best_practices") == []
                            or not response.get("best_practices")
                            or isinstance(response.get("best_practices"), dict)
                            or response.get("avoid") == []
                            or not response.get("avoid")
                            or isinstance(response.get("avoid"), dict)
                            or response.get("phrases_to_use") == []
                            or not response.get("phrases_to_use")
                            or isinstance(response.get("phrases_to_use"), dict)
                        )
                    )
                ):
                    logger.warning("Got wrong get-to-know from Langsmith - trying again")
                    response = runnable.invoke(arguments)
                else:
                    break
            logger.info("Got get-to-know from Langsmith: " + str(response))
        except Exception as e:
            response = f"Error: {e}"

        logger.info(f"Got get-to-know from Langsmith: {response}")
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

    async def run_prompt_get_meeting_goals(self, personal_data, my_company_data, call_info={}):
        prompt = hub.pull("get_meeting_goals")
        arguments = {
            "personal_data": personal_data,
            "my_company_data": my_company_data,
            "info": call_info,
        }
        response = None
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)
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

    async def run_prompt_get_meeting_guidelines(
        self, customer_strengths, meeting_details, meeting_goals, case={}
    ):
        prompt = hub.pull("get_meeting_guidelines")
        arguments = {
            "customer_strengths": customer_strengths,
            "meeting_details": meeting_details,
            "meeting_goals": meeting_goals,
            "case": case,
        }
        response = None
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)
        except Exception as e:
            response = f"Error: {e}"
        finally:
            if not response:
                logger.error("Meeting guidelines response returned None")
                response = self.run_prompt_get_meeting_guidelines(
                    customer_strengths, meeting_details, meeting_goals, case
                )
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

    async def _run_prompt_with_retry(self, runnable, arguments, max_retries=5, base_wait=2):
        for attempt in range(max_retries):
            try:
                response = runnable.invoke(arguments)
                if response:  # If successful, return the response
                    return response
            except LangSmithConnectionError as e:  # Handling specific connection error from LangSmith
                logger.error(f"LangSmithConnectionError encountered on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:  # Only wait if we have retries left
                    wait_time = base_wait * (2**attempt) + random.uniform(
                        0, 1
                    )  # Exponential backoff with jitter
                    logger.info(f"Retrying in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    raise e  # Raise exception if retries are exhausted
            except Exception as e:
                logger.error(f"General error encountered on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    wait_time = base_wait * (2**attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    raise e  # Raise exception if retries are exhausted
        raise Exception("Max retries exceeded")
