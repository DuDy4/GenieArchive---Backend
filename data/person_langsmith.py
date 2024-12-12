import json
import sys
import os
import asyncio

from dotenv import load_dotenv

from common.utils.news_utils import filter_not_reshared_social_media_news
from data.data_common.data_transfer_objects.profile_dto import Phrase

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.utils import env_utils
from data.data_common.utils.persons_utils import determine_profile_category, get_default_individual_sales_criteria
from ai.langsmith.langsmith_loader import Langsmith
from data.api_services.embeddings import GenieEmbeddingsClient
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.data_transfer_objects.sales_action_item_dto import SalesActionItem
from data.internal_services.sales_action_items_service import SalesActionItemsService
from data.data_common.dependencies.dependencies import (
    companies_repository,
    profiles_repository,
    personal_data_repository,
    tenants_repository,
    tenant_profiles_repository, 
    persons_repository,
    deals_repository,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()

PERSON_PORT = env_utils.get("PERSON_PORT", 8005)

CONSUMER_GROUP_LANGSMITH = "langsmithconsumergroup"


class LangsmithConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.NEW_PERSONAL_NEWS,
                    Topic.FAILED_TO_GET_PERSONAL_NEWS,
                    Topic.NEW_BASE_PROFILE,
                    Topic.NEW_PERSON_CONTEXT,
                    Topic.PERSONAL_NEWS_ARE_UP_TO_DATE],
            consumer_group=CONSUMER_GROUP_LANGSMITH,
        )

        self.langsmith = Langsmith()
        self.company_repository = companies_repository()
        self.profiles_repository = profiles_repository()
        self.tenants_repository = tenants_repository()
        self.personal_data_repository = personal_data_repository()
        self.tenant_profiles_repository = tenant_profiles_repository()
        self.embeddings_client = GenieEmbeddingsClient()
        self.persons_repository = persons_repository()
        self.deals_repository = deals_repository()
        self.sales_action_items_service = SalesActionItemsService()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        logger.info(f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}")
        topic = event.properties.get(b"topic").decode("utf-8")

        match topic:
            case Topic.NEW_PERSONAL_NEWS:
                logger.info("Handling new personal data to process")
                await self.handle_personal_data_to_process(event)
            case Topic.FAILED_TO_GET_PERSONAL_NEWS:
                logger.info("Failed to get personal news")
                await self.handle_personal_data_to_process(event)
            case Topic.PERSONAL_NEWS_ARE_UP_TO_DATE:
                logger.info("Personal news are up to date")
                await self.handle_personal_data_to_process(event)
            case Topic.NEW_BASE_PROFILE:
                logger.info("Handling new base profile")
                await self.handle_new_base_profile(event)
            case Topic.NEW_PERSON_CONTEXT:
                logger.info("Person Langsmith - Handling new person context")
                await self.handle_personal_data_to_process(event)
            case _:
                logger.info("No matching topic")

    async def handle_personal_data_to_process(self, event):
        """
        The right flow is:
        1. Check if there already is a profile in profiles table
            1.1 If there is, check if there is tenant_profile for this tenant (check if there is both sales_criteria and sales_action_items)
                1.1.1. If there is, do nothing
                1.1.2. If there isn't, create an event NEW_BASE_PROFILE
            1.2 If there isn't, create an event NEW_BASE_PROFILE
        2. Get person data and personal_data and personal_news from personal_data table
        3. Get company data from company table
        4. Get seller_context from embeddings
        5. Call langsmith.get_profile
            5.1 strength and get_to_know
            5.2 work_history_summary
        6. create events:
            6.1 NEW_BASE_PROFILE
            6.2 NEW_PROCESSED_PROFILE
        """
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_uuid = event_body.get("person_uuid") if event_body.get("person_uuid") else event_body.get("person_id")
        if not person_uuid:
            logger.error(f"No person data found for person {person_uuid}")
            return
        profile = self.profiles_repository.get_profile_data(person_uuid)

        # Check if needs to proceed with the event
        if profile and not event_body.get("force"):
            logger.info(f"Profile already exists: {profile}")
            tenant_id = logger.get_tenant_id()
            if not tenant_id:
                logger.error(f"No tenant id found")
                return
            tenant_sales_criteria, tenant_sales_action_items = self.tenant_profiles_repository.get_sales_criteria_and_action_items(person_uuid, tenant_id)
            if not tenant_sales_criteria or not tenant_sales_action_items:
                logger.info(f"Creating event NEW_BASE_PROFILE for person {person_uuid}")
                person = self.persons_repository.get_person(person_uuid)
                profile_to_send = {
                    "strengths": [strength.to_dict() for strength in profile.strengths],
                    "get_to_know": { key: [phrase.to_dict() for phrase in phrases] for key, phrases in profile.get_to_know.items()},
                    "work_history_summary": profile.work_history_summary,
                }
                data_to_send = {"person": person.to_dict(), "profile": profile_to_send, "email": person.email}
                event = GenieEvent(Topic.NEW_BASE_PROFILE, data_to_send, "public")
                event.send()
            logger.info(f"Profile {person_uuid} has tenant sales criteria and action items under tenant {tenant_id}")
            return {"status": "success"}

        # Gather person, personal_data, news_data, company_data and seller_context
        personal_data = self.personal_data_repository.get_pdl_personal_data(person_uuid)
        if not personal_data:
            personal_data = self.personal_data_repository.get_apollo_personal_data(person_uuid)
        person = self.persons_repository.get_person(person_uuid)
        if not person and not personal_data:
            logger.error(f"No person data found for person {person_uuid}")
            return
            # else:
            #     person = self.persons_repository.get_person(person_uuid)
            #     person = person.to_dict()
            #     personal_data = self.personal_data_repository.get_pdl_personal_data(person_uuid)
            #     if not personal_data:
            #         personal_data = self.personal_data_repository.get_apollo_personal_data(person_uuid)
            #     if not person and not personal_data:
            #         logger.error(f"No person data found for person {person_uuid}")
            #         return
        news_data = self.personal_data_repository.get_news_data_by_uuid(person_uuid)
        if not news_data:
            logger.error(f"No news data found for person {person_uuid}")
            news_data = []
        news_data = filter_not_reshared_social_media_news(news=news_data, linkedin_url=person.linkedin)
        logger.info(f"Personal News data: {str(news_data)[:300]}")

        email_address = person.email

        logger.info(f"Person from NEW_PERSONAL_DATA event: {email_address}")
        company_data = None
        company_dict = {}

        if email_address and isinstance(email_address, str) and "@" in email_address:
            company_data = self.company_repository.get_company_from_domain(email_address.split("@")[1])
        if company_data:
            company_dict = company_data.to_dict()
            company_dict.pop("uuid")
            company_dict.pop("domain")
            company_dict.pop("employees")
            company_dict.pop("logo")

        seller_context = None
        seller_tenant_id = logger.get_tenant_id()
        if seller_tenant_id:
            seller_email = self.tenants_repository.get_tenant_email(seller_tenant_id)
            if seller_email:
                seller_context = self.embeddings_client.search_materials_by_prospect_data(seller_email, personal_data)

        # Start cooking the profile - inside has strengths, get_to_know and work_history_summary
        response = await self.langsmith.get_profile(personal_data, company_dict, news_data, seller_context)
        logger.info(f"Response: {response.keys() if isinstance(response, dict) else response}")

        profile_strengths_get_to_know_work_history_summary = {
            "strengths": response.get("strengths"),
            "get_to_know": response.get("get_to_know"),
            "work_history_summary": response.get("work_history_summary"),
        }

        data_to_send = {"person": person.to_dict(), "profile": profile_strengths_get_to_know_work_history_summary, "force_refresh": event_body.get("force")}

        logger.info(f"About to send event's data: {data_to_send}")

        batch_manager = EventHubBatchManager()
        await batch_manager.start_batch()
        batch_manager.queue_event(GenieEvent(Topic.NEW_BASE_PROFILE, data_to_send, "public"))
        batch_manager.queue_event(GenieEvent(Topic.NEW_PROCESSED_PROFILE, data_to_send, "public"))
        await batch_manager.send_batch()

        return {"status": "success"}
    
    async def handle_new_base_profile(self, event):
        """
        The right flow is:
        1. Get person data and profile_strengths_get_to_know_work_history_summary from event.
        2. Get company data from database
        """
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("profile")
        strengths = personal_data.get("strengths")
        original_get_to_know = personal_data.get("get_to_know")
        work_history_summary = personal_data.get("work_history_summary")
        person = event_body.get("person")
        email_address = person.get("email")
        forced_refresh = event_body.get("force_refresh")
        seller_tenant_id = logger.get_tenant_id()

        company_data = None

        existing_sales_criteria, existing_action_items = self.tenant_profiles_repository.get_sales_criteria_and_action_items(person['uuid'], seller_tenant_id)
        if existing_action_items and existing_sales_criteria and not forced_refresh:
            logger.info(f"Sales criteria and action items already exist for person {person['uuid']} and tenant {seller_tenant_id}")
            return {"status": "success"}

        if email_address and isinstance(email_address, str) and "@" in email_address:
            company_data = self.company_repository.get_company_from_domain(email_address.split("@")[1])
            if company_data:
                company_dict = company_data.to_dict()
                company_dict.pop("uuid")
                company_dict.pop("domain")
                company_dict.pop("employees")
                company_dict.pop("logo")

        profile_strength_get_to_know_work_history_summary = {
            "strengths": strengths,
            "get_to_know": original_get_to_know,
            "work_history_summary": work_history_summary,
        }

        seller_context = None
        # seller_tenant_id = logger.get_tenant_id()
        if seller_tenant_id:
            seller_email = self.tenants_repository.get_tenant_email(seller_tenant_id)
            if seller_email:
                seller_context = self.embeddings_client.search_materials_by_prospect_data(seller_email, person)

        personal_news = self.personal_data_repository.get_news_data_by_uuid(person['uuid'])
        if not personal_news:
            personal_news = []
        personal_news = filter_not_reshared_social_media_news(news=personal_news, linkedin_url=person.get('linkedin'))
        person['news'] = personal_news

        # Get/create sales criteria
        # existing_sales_criteria = self.tenant_profiles_repository.get_sales_criteria(person['uuid'], seller_tenant_id)
        profile_category = determine_profile_category(strengths)
        if not existing_sales_criteria or forced_refresh:
            sales_criterias = get_default_individual_sales_criteria(profile_category)
            self.tenant_profiles_repository.update_sales_criteria(person['uuid'],  seller_tenant_id, sales_criterias)
        else:
            sales_criterias = existing_sales_criteria

        # Get/create sales action items
        # existing_action_items = self.tenant_profiles_repository.get_sales_action_items(person['uuid'], seller_tenant_id)
        if not existing_action_items or forced_refresh:
            action_items = self.sales_action_items_service.get_action_items(sales_criterias)
            if action_items:
                tasks = [
                    self.process_action_item(person, action_item, company_data, seller_context)
                    for action_item in action_items
                ]

                # Gather all results concurrently
                results = await asyncio.gather(*tasks)

                # Filter out None values and collect specific action items
                specific_action_items = [result for result in results if result is not None]
                logger.info(f"Specific action items for {person['uuid']} and tenant {seller_tenant_id}: {specific_action_items}")
                if specific_action_items and len(specific_action_items) == len(action_items):
                    action_items = specific_action_items
                else:  
                    logger.warning(f"Failed to get specific action items for all action items for prospect: {person['name']}")
                self.tenant_profiles_repository.update_sales_action_items(person['uuid'], seller_tenant_id, action_items)
        
        profile_uuid = person.get("uuid")

        event = GenieEvent(Topic.NEW_TENANT_PROFILE, {"profile_uuid": profile_uuid}, "public")
        event.send()
        return {"status": "success"}

    async def process_action_item(self, person, action_item, company_data, seller_context):
        logger.info(f"Action item: {action_item.to_dict()}")
        response = await self.langsmith.run_prompt_action_items(
            person,
            action_item.action_item,
            action_item.criteria.value,
            company_data,
            seller_context
        )
        if response and response.content:
            output_action_item = response.content
            if output_action_item:
                action_item.action_item = output_action_item
                logger.info(f"Updated action item: {action_item.to_dict()}")
                return action_item  # Return the updated action item
        logger.warning(f"Failed to get specific action item for prospect: {person['name']}, action item: {action_item.to_dict()}")
        return None  # Return None if nothing was updated


if __name__ == "__main__":
    langsmith_consumer = LangsmithConsumer()
    try:
        asyncio.run(langsmith_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
