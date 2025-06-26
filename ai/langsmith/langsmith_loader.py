import re
import asyncio
import json
import logging
import random

from common.utils import env_utils

from langchain import hub
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langsmith.utils import LangSmithConnectionError
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
# from data.api_services.embeddings import GenieEmbeddingsClient
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

load_dotenv()

logger = GenieLogger()

OPENAI_API_VERSION = env_utils.get("OPENAI_API_VERSION", "2024-08-01-preview")

class LoggerEventHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = self.format(record)
            # Send log_entry to log aggregator
        except Exception:
            self.handleError(record)

    def handleError(self, record):
        logger.error("Error in logging handler", exc_info=True)
        event = GenieEvent(
            topic=Topic.AI_TOKEN_ERROR,
            data={"error": "Error in logging handler", "record": record},
        )
        event.send()



class Langsmith:
    def __init__(self):
        self.api_key = env_utils.get("LANGSMITH_API_KEY")
        self.base_url = env_utils.get("LANGCHAIN_ENDPOINT", "https://api.smith.langchain.com")
        # self.model = ChatOpenAI(api_key=self.api_key, base_url=self.base_url)
        self.azure_model = AzureChatOpenAI(
            deployment_name="gpt-4o",
            model="gpt-4o",
            openai_api_version=OPENAI_API_VERSION,
        )
        self.model = self.azure_model
        # self.embeddings_client = GenieEmbeddingsClient()
        self.setup_custom_logging()
        

    async def get_profile(self, person_data,  news_data=None):
        # Run the two prompts concurrently
        logger.info("Running Langsmith prompts")
        # strengths = await self.run_prompt_strength(person_data, news_data)
        strengths_task = asyncio.create_task(self.run_prompt_strength(person_data, news_data))
        work_history_summary_task = asyncio.create_task(self.get_work_history_summary(person_data, person_data.get("work_history", [])))
        strengths = await strengths_task
        logger.info(f"Strengths from Langsmith: {strengths}")

        person_data["strengths"] = strengths.get("strengths") if isinstance(strengths, dict) and strengths.get("strengths") else strengths
        # get_to_know_task = asyncio.create_task(self.run_prompt_get_to_know(person_data, company_data, news_data, seller_context))

        work_history = await work_history_summary_task
        logger.info(f"Work history from Langsmith: {work_history}")
        person_data["work_history_summary"] = work_history
        #
        # get_to_know = await get_to_know_task
        # logger.info(f"Get to know from Langsmith: {get_to_know}")
        # person_data["get_to_know"] = get_to_know

        # news = self.run_prompt_news(person_data)
        # strengths, news = await asyncio.gather(strengths, news)
        logger.info(f"Profile from Langsmith: {person_data}")
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

    async def run_prompt_strength(self, person_data, news_data=None):
        logger.info("Running Langsmith prompt for strengths")
        prompt = hub.pull("get_strengths_with_social_media") if news_data else hub.pull("get_strengths")
        runnable = prompt | self.model
        arguments = {"personal_data": person_data, "social_media_posts": news_data if news_data else None}

        try:
            response = await self._run_prompt_with_retry(runnable, arguments)
            for i in range(5):
                try:
                    if response and isinstance(response, dict):
                        response = response.get("strengths")
                    if response and isinstance(response, list) and len(response) > 0:
                        if isinstance(response[0], str):
                            logger.error(f"Strengths from Langsmith got wrong: {response}")
                            response = await self._run_prompt_with_retry(runnable, arguments)
                except Exception as e:
                    logger.error(f"Error parsing strengths from Langsmith: {e}")
        except Exception as e:
            logger.error(f"Error running strengths prompt: {e}")
        return response

    async def run_prompt_action_items(self, person_data, action_item, action_item_criteria, company_data=None, seller_context=None):
        prompt = hub.pull("specific-action-item") 
        runnable = prompt | self.model
        logger.info(f"Running Langsmith prompt for specific action item: {action_item}, criteria: {action_item_criteria}, seller_context: {seller_context}")
        arguments = {
            "sales_action_item": action_item, 
            "action_item_criteria": action_item_criteria,
            "prospect_company_data": company_data if company_data else None, 
            "prospect_data": person_data, 
            "seller_company_data": seller_context
        }

        try:
            response = await self._run_prompt_with_retry(runnable, arguments)
        except Exception as e:
            response = f"Error: {e}"
        return response

    async def run_prompt_send_file_action_items(
            self, 
            action_item,
            action_item_criteria, 
            file_name, 
            person_data, 
            prospect_company_data=None,
            chunk_text = None):
        prompt = hub.pull("send-file-action-item") 
        runnable = prompt | self.azure_model
        logger.info(f"Running Langsmith prompt for specific send file action item: {action_item}, criteria: {action_item_criteria}, file: {file_name}")
        arguments = {
            "sales_action_item": action_item, 
            "action_item_criteria": action_item_criteria,
            "file_name": file_name,
            "prospect_data": person_data, 
            "prospect_company_data": prospect_company_data if prospect_company_data else None, 
            "chunk_text": chunk_text
        }

        try:
            response = await self._run_prompt_with_retry(runnable, arguments)
        except Exception as e:
            response = f"Error: {e}"
        return response

    async def run_prompt_get_to_know(self, person_data, company_data=None, news_data=None, seller_context=None):
        prompt = hub.pull("dos_and_donts_w_context_and_posts") if news_data else hub.pull("dos_and_donts_w_context")
        runnable = prompt | self.model
        arguments = {
            "personal_data": person_data.get("personal_data", "not found"),
            "person_background": person_data.get("background", "not found"),
            "strengths": person_data.get("strengths", "not found"),
            "hobbies": person_data.get("hobbies", "not found"),
            "news": person_data.get("news") if person_data.get("news") else (company_data.get("news", "not found") if company_data else "not found"),
            "product_data": person_data.get("product_data", "not found"),
            "company_data": company_data if company_data else "not found",
            "personal_social_media_posts": news_data if news_data else "not found",
            "seller_context": seller_context if seller_context else "not found",
        }
        try:
            response = await self._run_prompt_with_retry(runnable, arguments)
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
        return response

    async def run_prompt_doc_categories(self, doc_content):
        prompt = hub.pull("classify-file-category")
        try:
            runnable = prompt | self.model
            response = runnable.invoke({"file_content": doc_content})
        except Exception as e:
            response = f"Error: {e}"
        if isinstance(response, dict) and response.get("doc_categories"):
            return response.get("doc_categories")
        else:
            return []

    def run_prompt_company_overview_challenges(self, company_data):
        logger.info("Running Langsmith prompt for company overview and challenges")

        prompt = hub.pull("get_company_overview")
        try:
            runnable = prompt | self.model
            response = runnable.invoke(company_data)
        except Exception as e:
            response = f"Error: {e}"
        return response

    async def run_prompt_get_meeting_goals(
        self, personal_data, my_company_data, seller_context, call_info={}
    ):
        if seller_context:
            prompt = hub.pull("get_meeting_goals_w_context")
        else:
            prompt = hub.pull("get_meeting_goals")
        arguments = {
            "personal_data": personal_data,
            "my_company_data": my_company_data,
            "info": call_info,
            "seller_context": seller_context,
        }
        response = None
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)
        except Exception as e:
            response = f"Error: {e}"
        finally:
            logger.info(f"Got meeting goals from Langsmith: {response}")
            for i in range(5):
                logger.info(f"TRY #{i}: Trying to parse meeting goals from Langsmith: {response}")
                if isinstance(response, dict) and (response.get("goals") or response.get("")):
                    response = response.get("goals") or response.get("")
                if isinstance(response, str):
                    response = json.loads(response)
                if isinstance(response, list):
                    break
                if not response:
                    logger.error("Meeting goals response returned None")
                    response = []
            return response

    async def run_prompt_get_meeting_guidelines(
        self, customer_strengths, meeting_details, meeting_goals, seller_context, case={}
    ):
        if seller_context:
            prompt = hub.pull("get_meeting_guidelines_w_context")
        else:
            prompt = hub.pull("get_meeting_guidelines")
        arguments = {
            "customer_strengths": customer_strengths,
            "meeting_details": meeting_details,
            "meeting_goals": meeting_goals,
            "case": case,
            "seller_context": seller_context,
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
                response = await self.run_prompt_get_meeting_guidelines(
                    customer_strengths, meeting_details, meeting_goals, case
                )
            while True:
                if isinstance(response, dict) and response.get("guidelines"):
                    response = response.get("guidelines") or response.get("data")
                if isinstance(response, str):
                    response = json.loads(response)
                if isinstance(response, list):
                    break
            return response

    async def preprocess_uploaded_file_content(self, text):
        try:
            prompt = hub.pull("file-upload-preprocessing")
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, text)
        except Exception as e:
            logger.error(f"Error running file upload preprocessing: {e}")
            response = text
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

    async def _run_prompt_with_retry_artifacts(self, runnable, arguments, max_retries=5, base_wait=2):
        """Retries LangSmith prompt execution with async support."""
        for attempt in range(max_retries):
            try:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(None, runnable.invoke, arguments)
                if response:  # ✅ If successful, return the response
                    return response
            except LangSmithConnectionError as e:
                logger.error(f"LangSmithConnectionError encountered on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    wait_time = base_wait * (2**attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)  # ✅ Keep it async
                else:
                    raise e
            except Exception as e:
                logger.error(f"General error encountered on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    wait_time = base_wait * (2**attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)  # ✅ Keep it async
                else:
                    raise e
        raise Exception("Max retries exceeded")

    async def get_news(self, news_data: dict):
        prompt = hub.pull("post_summary")
        runnable = prompt | self.model
        arguments = [news_data]

        try:
            response = await self._run_prompt_with_retry(runnable, arguments)
        except Exception as e:
            response = f"Error: {e}"

        logger.info(f"Got news summary from Langsmith: {response}")
        return response

    async def get_company_challenges_with_news(self, company_dto):
        prompt = hub.pull("get_company_challenges_with_news")
        arguments = {"company_data": company_dto.to_dict(), "company_news": company_dto.news}
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)
            if response and isinstance(response, dict):
                response = response.get("challenges")
        except Exception as e:
            response = f"Error: {e}"
        return response

    async def get_meeting_summary(self, meeting_data, seller_context, profiles, company_data):
        logger.info("Running Langsmith prompt for meeting summary")
        prompt = hub.pull("get_meeting_summary_to_email")
        arguments = {
            "meeting_data": meeting_data,
            "profiles": profiles,
            "company_data": company_data,
            "seller_context": seller_context,
        }
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)
        except Exception as e:
            response = f"Error: {e}"
        return response
    

    async def get_work_history_summary(self, person, work_history):
        logger.info("Running Langsmith prompt for work history summary")
        prompt = hub.pull("work-history-summary")
        arguments = {
            "person_data": person,
            "work_history": work_history
        }
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)            
            if response and response.content and isinstance(response.content, str):
                response = response.content
        except Exception as e:          
            response = f"Error: {e}"
        return response
    

    async def get_profile_param_reasoning(self, person_name, text, param_name, reasoning, profile):
        logger.info("Running Langsmith prompt for profile-param-reasoning")
        prompt = hub.pull("profile-param-reasoning")
        arguments = {
            "name": person_name,
            "text": text,
            "param": param_name,
            "reasoning": reasoning,
            "profile": profile
        }
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)            
            if response and response.content and isinstance(response.content, str):
                response = response.content
        except Exception as e:          
            response = f"Error: {e}"
        return response

    async def get_work_history_post(self, work_history_artifact: dict):
        logger.info("Running Langsmith prompt for work history post")
        prompt = hub.pull("work_history_post_generator")
        arguments = {
            "work_history": work_history_artifact
        }
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)
            if response and response.content and isinstance(response.content, str):
                response = response.content
        except Exception as e:
            response = f"Error: {e}"
        return response

    async def get_param_evaluation(self, person, param_data, person_artifact):
        logger.info("Running Langsmith prompt for evaluating param")
        prompt = hub.pull("param-scoring-v2")
        arguments = {
            "personal_info": person,
            "param_data": param_data,
            "post": person_artifact
        }
        try:
            runnable = prompt | self.azure_model
            response = await self._run_prompt_with_retry_artifacts(runnable, arguments)
            if response and response.content and isinstance(response.content, str):
                response = response.content
                if response.startswith("```"):
                    response = response.replace("```json", "").strip("`").strip()
                    response = response.replace("\\n", "\n").replace("\n", " ").replace('\\"', '"')
                    response = response.split("```")[0].strip() if "```" in response else response
                    response = json.loads(response, strict=False)
                else:
                    response = self.extract_json(response)
            else:
                logger.error(f"Error parsing param evaluation from Langsmith: {response}")
        except Exception as e:
            logger.error(f"Error running param evaluation: {e}")
            try:
                logger.info(f"The response we got is: {response}")
            except Exception as e:
                logger.error(f"Did not even get a response: {e}")
            return {}
        return response

    async def get_summary(self, data, max_words=50):
        logger.info("Running Langsmith prompt for text summary")
        prompt = hub.pull("whiteforest/chain-of-density-prompt")
        arguments = {
            "content": data,
            "max_words": max_words,
            "content_category" : "Overview",
            "entity_range": "1-3",
            "iterations" : "1"
        }
        try:
            runnable = prompt | self.model
            response = await self._run_prompt_with_retry(runnable, arguments)
            if response and response.content and isinstance(response.content, str):
                summary_array = json.loads(response.content)
                if summary_array and isinstance(summary_array, list) and len(summary_array) > 0:
                    try:
                        summary = summary_array[0].get("denser_summary")
                        return summary
                    except Exception:
                        return None
                else:
                    return None
            else:
                return None
        except Exception as e:
            response = f"Error: {e}"
        return response

    async def get_get_to_know(self, person_data, company_data=None, news_data=None, seller_context=None):
        get_to_know = await self.run_prompt_get_to_know(person_data, company_data, news_data, seller_context)
        person_data["get_to_know"] = get_to_know
        return person_data

    def setup_custom_logging(self):
        logger = logging.getLogger("openai._base_client")
        logger.setLevel(logging.INFO)  # Set level if not already set

        # Create and attach the Slack notification handler
        slack_handler = LoggerEventHandler()
        slack_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        slack_handler.setFormatter(formatter)

        logger.addHandler(slack_handler)



    def extract_json(self, text):
        match = re.search(r'```json\n(.*?)\n```', text, re.DOTALL)
        if match:
            json_str = match.group(1)
        else:
            json_str = text
        try:
            json_obj = json.loads(json_str)
            return json_obj
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
        return None