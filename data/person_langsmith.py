import json
import sys
import os
import asyncio

from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.utils import env_utils

from ai.langsmith.langsmith_loader import Langsmith
from data.api_services.embeddings import GenieEmbeddingsClient
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.utils.persons_utils import create_person_from_pdl_personal_data, create_person_from_apollo_personal_data
from data.data_common.dependencies.dependencies import (
    companies_repository,
    profiles_repository,
    personal_data_repository,
    tenants_repository,
    tenant_profiles_repository, persons_repository,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()

PERSON_PORT = env_utils.get("PERSON_PORT", 8005)

CONSUMER_GROUP_LANGSMITH = "langsmithconsumergroup"


class LangsmithConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.NEW_PERSONAL_DATA, Topic.NEW_NEWS_DATA, Topic.NEW_BASE_PROFILE, Topic.NEW_PERSON_CONTEXT],
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

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        logger.info(f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}")
        topic = event.properties.get(b"topic").decode("utf-8")

        match topic:
            case Topic.NEW_PERSONAL_DATA:
                logger.info("Handling new personal data to process")
                await self.handle_new_personal_data(event)
            case Topic.NEW_NEWS_DATA:
                logger.info("Handling new news data")
                await self.handle_new_news_data(event)
            case Topic.NEW_BASE_PROFILE:
                logger.info("Handling new base profile")
                await self.handle_new_base_profile(event)
            case Topic.NEW_PERSON_CONTEXT:
                logger.info("Person Langsmith - Handling new person context")
                await self.handle_new_person_context(event)
            case _:
                logger.info("No matching topic")

    async def handle_new_person_context(self, event):
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        tenant_id = event_body.get("tenant_id")
        if not tenant_id:
            tenant_id = logger.get_tenant_id()
        logger.info(f"Tenant ID: {tenant_id}")
        user_email = self.tenants_repository.get_tenant_email(tenant_id)
        if not user_email:
            logger.error(f"No user email found for tenant {tenant_id}")
            return
        person_id = event_body.get("person_id")
        company_uuid = event_body.get("company_uuid")
        company_data = self.company_repository.get_company(company_uuid)
        person_data = self.profiles_repository.get_profile_data(person_id)
        if not person_data:
            logger.error(f"No person data found for person {person_id}")
            return
        pdl_personal_data = self.personal_data_repository.get_pdl_personal_data(person_id)
        apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person_id)
        fetched_personal_data = None
        if pdl_personal_data:
            fetched_personal_data = pdl_personal_data
        elif apollo_personal_data:
            fetched_personal_data = apollo_personal_data
        person_data_dict = person_data.to_dict()
        person_data_dict['personal_data'] = fetched_personal_data
        seller_context = None
        if tenant_id:
            seller_email = self.tenants_repository.get_tenant_email(tenant_id)
            if seller_email:
                seller_context = self.embeddings_client.search_materials_by_prospect_data(seller_email, person_data)
        if seller_context:
            response = await self.langsmith.get_get_to_know(person_data_dict, company_data.to_dict(), seller_context)
        if response:
            get_to_know = response.get("get_to_know")
            if get_to_know:
                logger.info(f"Updating get to know for person {person_id}")
                self.tenant_profiles_repository.update_get_to_know(person_id, get_to_know, tenant_id)

    async def handle_new_personal_data(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("personal_data")
        person_data = {"personal_data": personal_data}
        person = event_body.get("person")
        if not person and not personal_data:
            person_uuid = event_body.get("person_uuid")
            if not person_uuid:
                logger.error(f"No person data found for person {person_uuid}")
                return
            else:
                person = self.persons_repository.get_person(person_uuid)
                personal_data = self.personal_data_repository.get_pdl_personal_data(person_uuid)
                if not personal_data:
                    personal_data = self.personal_data_repository.get_apollo_personal_data(person_uuid)
                person_data = {"personal_data": personal_data}
        email_address = person.get("email")
        profile = self.profiles_repository.get_profile_data_by_email(email_address)
        if profile and not event_body.get("force"):
            logger.info(f"Profile already exists: {profile}")
            return {"status": "success"}
        profile = self.profiles_repository.get_profile_data(person.get("uuid"))
        if profile:
            logger.info(f"Profile already exists: {profile}")
            return {"status": "success"}
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

        # seller_context = None
        # seller_tenant_id = logger.get_tenant_id()
        # if seller_tenant_id:
        #     seller_email = self.tenants_repository.get_tenant_email(seller_tenant_id)
        #     if seller_email:
        #         seller_context = self.embeddings_client.search_materials_by_prospect_data(seller_email, person_data)

        response = await self.langsmith.get_profile(person_data, company_dict)
        logger.info(f"Response: {response.keys() if isinstance(response, dict) else response}")

        profile_strength_and_get_to_know = {
            "strengths": response.get("strengths"),
            "get_to_know": response.get("get_to_know"),
        }

        data_to_send = {"person": person, "profile": profile_strength_and_get_to_know}

        logger.info(f"About to send event's data: {data_to_send}")

        event = GenieEvent(Topic.NEW_BASE_PROFILE, data_to_send, "public")
        event.send()
        return {"status": "success"}
    
    async def handle_new_base_profile(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("profile")
        strengths = personal_data.get("strengths")
        original_get_to_know = personal_data.get("get_to_know")
        person = event_body.get("person")
        email_address = person.get("email")
        
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

        profile_strength_and_get_to_know = {
            "strengths": strengths,
            "get_to_know": original_get_to_know,
        }

        seller_context = None
        seller_tenant_id = logger.get_tenant_id()
        if seller_tenant_id:
            seller_email = self.tenants_repository.get_tenant_email(seller_tenant_id)
            if seller_email:
                seller_context = self.embeddings_client.search_materials_by_prospect_data(seller_email, person)

        if seller_context:
            response = await self.langsmith.get_get_to_know(person, company_dict, seller_context)
            logger.info(f"Response: {response.keys() if isinstance(response, dict) else response}")
            profile_strength_and_get_to_know["tenant_get_to_know"] = response.get("get_to_know")


        data_to_send = {"person": person, "profile": profile_strength_and_get_to_know}

        logger.info(f"About to send event's data: {data_to_send}")

        event = GenieEvent(Topic.NEW_PROCESSED_PROFILE, data_to_send, "public")
        event.send()
        return {"status": "success"}

    async def handle_new_news_data(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        news_data = event_body.get("news_data")
        if isinstance(news_data, str):
            news_data = json.loads(news_data)
        logger.info(f"News data: {news_data}, type: {type(news_data)}")
        uuid = event_body.get("uuid")
        for news_item in news_data:
            if isinstance(news_item, str):
                news_item = json.loads(news_item)
            logger.info(f"News item: {news_item}")
            response = await self.langsmith.get_news(news_item)
            logger.info(f"Response: {response}")

            if response:
                # Ensure response is handled correctly
                if hasattr(response, "content"):
                    summary = response.content
                elif isinstance(response, dict):
                    summary = response.get("content", "")
                else:
                    summary = str(response)

                # Clean the summary if it's a string
                if isinstance(summary, str):
                    summary = summary.strip('"')

                logger.info(f"Summary: {summary}")

                # Update the news item with the summary
                news_item["summary"] = summary
                logger.info(f"News item with summary: {news_item}")

                # Save to the database
                self.personal_data_repository.update_news_to_db(uuid, news_item)

                event = GenieEvent(Topic.NEW_PERSONAL_DATA, {"person_uuid": uuid, "force": True}, "public")
                event.send()
        return {"status": "success"}


if __name__ == "__main__":
    langsmith_consumer = LangsmithConsumer()
    try:
        asyncio.run(langsmith_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
