import asyncio
import json
from errno import EHOSTDOWN

from common.utils import email_utils
from data.data_common.data_transfer_objects.meeting_dto import (
    MeetingDTO,
    evaluate_meeting_classification,
    AgendaItem,
    MeetingClassification,
)
from data.api_services.embeddings import GenieEmbeddingsClient
from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.data_transfer_objects.company_dto import CompanyDTO
from data.data_common.data_transfer_objects.user_dto import UserDTO
from data.data_common.repositories.user_profiles_repository import UserProfilesRepository
from data.data_common.repositories.users_repository import UsersRepository
from data.internal_scripts.fetch_social_media_news import (
    fetch_linkedin_posts,
    get_all_uuids_that_should_try_posts,
)

from data.internal_scripts.create_action_items import sync_action_items

from data.data_common.dependencies.dependencies import (
    tenants_repository,
    google_creds_repository,
    persons_repository,
    ownerships_repository,
    meetings_repository,
    profiles_repository,
    companies_repository,
    personal_data_repository,
)
from common.genie_logger import GenieLogger
import uuid
from fastapi import HTTPException

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.utils.postgres_connector import check_db_connection

logger = GenieLogger()


class AdminApiService:
    def __init__(self):
        self.users_repository = UsersRepository()
        self.tenants_repository = tenants_repository()
        self.google_creds_repo = google_creds_repository()
        self.persons_repository = persons_repository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()
        self.profiles_repository = profiles_repository()
        self.companies_repository = companies_repository()
        self.user_profiles_repository = UserProfilesRepository()
        self.user_profiles_repository = UserProfilesRepository()
        self.personal_data_repository = personal_data_repository()
        self.embeddings_client = GenieEmbeddingsClient()
        self.langsmith = Langsmith()

    def update_profiles(self, profiles):
        for profile_data in profiles:
            profile = profile_data.get("profile")
            person = profile_data.get("person")
            company = profile_data.get("company")

            profile_dto = ProfileDTO.from_dict(profile)
            person_dto = PersonDTO.from_dict(person)
            company_dto = CompanyDTO.from_dict(company)

            logger.info(f"Updating Profile: {profile_dto.name} - {person_dto.email} - {company_dto.name}")

            self.persons_repository.save_person(person_dto)
            self.companies_repository.save_company_without_news(company_dto)
            self.profiles_repository.save_profile(profile_dto)
        return {"status": "success"}

    def get_latest_profiles(self, limit, search_term=None):
        profile_uuids = self.profiles_repository.get_latest_profile_ids(limit, search_term)
        profiles = []
        for profile_uuid in profile_uuids:
            profile = self.profiles_repository.get_profile_data(profile_uuid)
            if not profile:
                logger.error(f"Profile not found for: {profile_uuid}")
                continue
            profile.uuid = str(profile_uuid)
            profile.picture_url = str(profile.picture_url)
            if not profile:
                logger.error(f"Profile not found: {profile_uuid}")
                continue
            # specific_get_to_know = self.user_profiles_repository.get_get_to_know(profile_uuid)
            # if specific_get_to_know:
            #     profile.get_to_know = specific_get_to_know

            person = self.persons_repository.get_person(profile_uuid)
            company = self.companies_repository.get_company_from_domain(person.email.split("@")[1])
            profiles.append(
                {
                    "profile": profile.to_dict(),
                    "person": person.to_dict(),
                    "company": company.to_dict() if company else None,
                }
            )


        return profiles

    def sync_profile(self, person_uuid):
        self.validate_uuid(person_uuid)
        person = self.persons_repository.get_person(person_uuid)
        if not person:
            logger.error(f"Person not found: {person_uuid}")
            return {"error": "Person not found"}
        logger.info(f"Got person: {person}")
        if person.linkedin:
            # all_tenants = self.ownerships_repository.get_tenants_for_person(person_uuid)
            all_users = self.ownerships_repository.get_users_for_person(person_uuid)
            # logger.info(f"Got tenants: {all_tenants}, users: {all_users}")
            # if not all_tenants or len(all_tenants) == 0:
            #     logger.error(f"Person does not have any tenants: {person_uuid}")
            #     return {"error": "Person does not have any tenants"}
            for user in all_users:
                data_to_send ={
                    "user_id": user.get("user_id"),
                    "tenant_id": user.get("tenant_id"),
                    "person": person.to_dict(),
                }
                event = GenieEvent(Topic.NEW_PERSON, data_to_send, "public")
                event.send()

            # for tenant in all_tenants:
            #     data_to_send ={
            #         "tenant_id": tenant,
            #         "person": person.to_dict(),
            #     }
            #     event = GenieEvent(Topic.NEW_PERSON, data_to_send, "public")
            #     event.send()
        else:
            logger.error(f"Person does not have a LinkedIn URL")
            return {"error": "Person does not have a LinkedIn URL"}
        return {"message": "Profile sync initiated for " + person.email}

    def sync_email(self, person_uuid):
        self.validate_uuid(person_uuid)
        person = self.persons_repository.get_person(person_uuid)
        if not person:
            logger.error(f"Person not found: {person_uuid}")
            return {"error": "Person not found"}
        logger.info(f"Got person: {person}")
        # tenants = self.ownerships_repository.get_tenants_for_person(person_uuid)
        users = self.ownerships_repository.get_users_for_person(person_uuid)
        # needs to change here
        # if not tenants or len(tenants) == 0:
        #     logger.error(f"Person does not have any tenants: {person_uuid}")
        #     return {"error": "Person does not have any tenants"}
        for user in users:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"tenant_id": user.get("tenant_id"), "user_id": user.get("user_id"), "email": person.email},
            )
            event.send()
        return {"message": "Email sync initiated for " + person.email}

    def sync_params(self, person_uuid):
        self.validate_uuid(person_uuid)
        person = self.persons_repository.get_person(person_uuid)
        if not person:
            logger.error(f"Person not found: {person_uuid}")
            return {"error": "Person not found"}
        logger.info(f"Got person: {person}")
        # tenants = self.ownerships_repository.get_tenants_for_person(person_uuid)
        users = self.ownerships_repository.get_users_for_person(person_uuid)
        # needs to change here
        # if not tenants or len(tenants) == 0:
        #     logger.error(f"Person does not have any tenants: {person_uuid}")
        #     return {"error": "Person does not have any tenants"}
        personal_data = self.personal_data_repository.get_pdl_personal_data(person_uuid)
        if not personal_data:
            personal_data = self.personal_data_repository.get_apollo_personal_data(person_uuid)
        if not personal_data:
            logger.error(f"Person does not have any personal data: {person_uuid}")
            return {"error": "Person does not have any personal data"}
        for user in users:
            # event = GenieEvent(
            #     topic=Topic.NEW_PERSONAL_DATA,
            #     data={"tenant_id": user.get("tenant_id"), "user_id": user.get("user_id"), "person": person.to_dict(), "personal_data": personal_data},
            # )
            # event.send()
            event = GenieEvent(
                topic=Topic.NEW_PERSONAL_NEWS,
                data={"tenant_id": user.get("tenant_id"), "user_id": user.get("user_id"), "person_uuid": person_uuid}
            )
            event.send()
        return {"message": "Email sync initiated for " + person.email}

    def validate_uuid(self, uuid_string):
        try:
            val = uuid.UUID(uuid_string, version=4)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid UUID format")
        return str(val)

    def fetch_all_tenants(self):
        all_tenants = self.tenants_repository.get_all_tenants()
        response = {"admin": True, "tenants": [tenant.to_dict() for tenant in all_tenants]}
        return response

    def fetch_all_users(self):
        all_users = self.users_repository.get_all_users()
        response = {"admin": True, "users": [user.to_dict() for user in all_users]}
        return response

    def process_meeting_from_scratch(self, meeting: MeetingDTO):
        participant_emails = meeting.participants_emails
        try:
            self_email = [email for email in participant_emails if email.get("self")][0].get("email")
        except IndexError:
            logger.error(f"Could not find self email in {participant_emails}")
            return
        self_domain = self_email.split("@")[1] if "@" in self_email else None
        if self_domain:
            additional_domains = self.companies_repository.get_additional_domains(self_email.split("@")[1])
            emails_to_process = email_utils.filter_emails_with_additional_domains(self_email, participant_emails, additional_domains)
        else:
            emails_to_process = email_utils.filter_emails(self_email, participant_emails)
        logger.info(f"Emails to process: {emails_to_process}")
        for email in emails_to_process:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"tenant_id": meeting.tenant_id, "email": email},
            )
            event.send()
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN, data={"tenant_id": meeting.tenant_id, "email": email}
            )
            event.send()
        event = GenieEvent(
            topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
            data={"tenant_id": meeting.tenant_id, "email": self_email},
        )
        event.send()
        logger.info("processed meeting from scratch - finished")
        return {"status": "success"}

    def process_agenda_to_all_meetings(self, number_of_meetings=10):
        all_meetings = self.meetings_repository.get_all_external_meetings_without_agenda()
        logger.info(f"Found {len(all_meetings)} meetings without agenda")
        all_meetings = all_meetings[:number_of_meetings]
        logger.info(f"Processing {len(all_meetings)} meetings")
        for meeting in all_meetings:
            if meeting.agenda:
                logger.info("Meeting has agenda")
            else:
                meeting_goals = self.meetings_repository.get_meeting_goals(meeting.uuid)
                if meeting_goals:
                    event = GenieEvent(topic=Topic.NEW_MEETING_GOALS, data={"meeting_uuid": meeting.uuid})
                    event.send()
                else:
                    self.process_meeting_from_scratch(meeting)
        logger.info("Finished processing all meetings")
        all_meetings = self.meetings_repository.get_all_external_meetings_without_agenda()
        logger.info(f"After processing, found {len(all_meetings)} meetings without agenda")
        return {"status": "success"}

    def process_classification_to_all_meetings(self):
        meetings = self.meetings_repository.get_all_meetings_without_classification()
        for meeting in meetings:
            classification = evaluate_meeting_classification(meeting.participants_emails)
            meeting.classification = classification
            logger.info(f"Updated meeting {meeting.uuid} with classification {meeting.classification}")
            self.meetings_repository.save_meeting(meeting)
            logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")
        return {"status": f"success. evaluated classification for {len(meetings)} meetings"}

    def process_new_classification_to_all_meetings(self):
        meetings = self.meetings_repository._get_all_meetings()
        for meeting in meetings:
            classification = evaluate_meeting_classification(meeting.participants_emails)
            meeting.classification = classification
            self.meetings_repository.save_meeting(meeting)
            logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")

    def process_missing_profiles_from_existing_personal_data(self):
        profiles_uuid = self.profiles_repository.get_missing_profiles()
        logger.info(f"Found {len(profiles_uuid)} profiles without profile")
        for profile_uuid in profiles_uuid:
            logger.info(f"Syncing profile for {profile_uuid}")
            result = self.sync_profile(profile_uuid)
            if result.get("error"):
                logger.error(f"Error syncing profile: {result}")
                email = self.persons_repository.get_person(profile_uuid).email
                result = self.sync_email(profile_uuid)
                if result.get("error"):
                    logger.error(f"Error syncing email: {result}")
                else:
                    logger.info(f"Email synced for {email}")

    def process_single_meeting_agenda(self, meeting_uuid: str):
        meeting = self.meetings_repository.get_meeting_data(meeting_uuid)
        if not meeting:
            logger.error(f"Meeting not found: {meeting_uuid}")
            return {"error": "Meeting not found"}
        if meeting.agenda:
            logger.error(f"Meeting already has an agenda: {meeting_uuid}")
            return {"error": "Meeting already has an agenda"}
        if meeting.classification.value != MeetingClassification.EXTERNAL.value:
            logger.error(f"Meeting is not external: {meeting_uuid}")
            return {"error": "Meeting is not external"}
        participant_emails = meeting.participants_emails
        if not participant_emails:
            logger.error(f"No participant emails found for meeting: {meeting.uuid}")
            return {"error": "No participant emails found"}
        # host_email = self.tenants_repository.get_tenant_email(meeting.tenant_id)
        host_email = self.users_repository.get_email_by_user_id(meeting.user_id)
        if not host_email:
            logger.error(f"No host email found for user: {meeting.user_id}")
            return {"error": "No host email found"}
        self_email = [email for email in participant_emails if email.get("self")][0].get("email")
        if not self_email or self_email != host_email:
            logger.error(f"Host email not found in participant emails: {participant_emails}")
            return {"error": "Host email not found"}
        goals = self.meetings_repository.get_meeting_goals(meeting_uuid)
        if goals:
            return self.handle_meeting_with_goals(meeting, self_email, goals)
        else:
            return self.handle_meeting_without_goals(meeting, self_email)

    def sync_action_items(self, num_sync=5, forced_refresh=False):
        result = sync_action_items(num_sync, forced_refresh)
        return result

    def handle_meeting_with_goals(self, meeting: MeetingDTO, self_email: str, goals: list):
        participant_emails = meeting.participants_emails
        self_domain = self_email.split("@")[1] if "@" in self_email else None
        if self_domain:
            additional_domains = self.companies_repository.get_additional_domains(self_email.split("@")[1])
            emails = email_utils.filter_emails_with_additional_domains(self_email, participant_emails, additional_domains)
        else:
            emails = email_utils.filter_emails(self_email, participant_emails)
        profiles = self.profiles_repository.get_profiles_by_email_list(emails)
        if not profiles:
            logger.error(f"No strengths found for any participant for meeting: {meeting.uuid}")
            return {"error": f"No strengths found for any participant for meeting: {meeting.uuid}"}
        profile = profiles[0]
        seller_context = None
        if self_email:
            seller_context = self.embeddings_client.search_materials_by_prospect_data(self_email, profile)
            seller_context = " || ".join(seller_context) if seller_context else None
        logger.info(f"About to run ask langsmith for meeting guidelines for meeting {meeting.uuid}")
        meeting_guidelines = asyncio.run(
            self.langsmith.run_prompt_get_meeting_guidelines(profile, meeting, goals, seller_context)
        )
        if not meeting_guidelines:
            logger.error(f"Failed to create meeting guidelines for meeting {meeting.uuid}")
            return {"error": f"Failed to create meeting guidelines for meeting {meeting.uuid}"}
        logger.info(f"Meeting guidelines: {meeting_guidelines}")
        try:
            agendas = [AgendaItem.from_dict(agenda) for agenda in meeting_guidelines]
        except AttributeError as e:
            logger.error(f"Error converting agenda to AgendaItem: {e}")
            return {"error": f"Error converting agenda to AgendaItem: {e}"}
        meeting.agenda = agendas
        self.meetings_repository.save_meeting(meeting)
        event = GenieEvent(
            topic=Topic.UPDATED_AGENDA_FOR_MEETING,
            data={"meeting": meeting.to_dict()},
        )
        event.send()
        return {"status": f"Created Agenda for meeting {meeting.uuid}"}

    def handle_meeting_without_goals(self, meeting: MeetingDTO, self_email: str):
        self_company = self.companies_repository.get_company_from_domain(email_utils.get_domain(self_email))
        if not self_company:
            logger.error(f"Company not found for email: {self_email}")
            return {"error": "Company not found"}
        target_personal_data = None
        for email_obj in meeting.participants_emails:
            email = email_obj.get("email")
            if not email:
                logger.error(f"No email found in participant email object: {email_obj}")
                continue
            if email == self_email:
                continue
            personal_data = self.personal_data_repository.get_any_personal_data_by_email(email)
            if personal_data:
                target_personal_data = personal_data
                break
        if not target_personal_data:
            logger.error(f"No target personal data for any participant for meeting: {meeting.uuid}")
            return {"error": "No target personal data found"}
        # Else, we have a target personal data and company data
        seller_context = None
        if self_email:
            seller_context = self.embeddings_client.search_materials_by_prospect_data(
                self_email, target_personal_data
            )
            seller_context = " || ".join(seller_context) if seller_context else None
        logger.info(f"About to run ask langsmith for meeting goals for meeting {meeting.uuid}")
        meetings_goals = asyncio.run(
            self.langsmith.run_prompt_get_meeting_goals(
                personal_data=target_personal_data,
                my_company_data=self_company,
                seller_context=seller_context,
            )
        )
        if not meetings_goals:
            logger.error(f"No meeting goals found for {meeting.uuid}")
            return {"error": "No meeting goals found"}
        logger.info(f"Meetings goals: {meetings_goals}")
        self.meetings_repository.save_meeting_goals(meeting.uuid, meetings_goals)
        event = GenieEvent(
            topic=Topic.NEW_MEETING_GOALS,
            data={"meeting_uuid": meeting.uuid, "seller_context": seller_context},
        )
        event.send()
        return {"status": f"Created Meeting goals. Proceeding to Create agenda"}

    def check_database_connection(self):
        try:
            connection: bool = check_db_connection()
            return connection
        except Exception as e:
            logger.error(f"Error checking database connection: {e}")
            return False

    def sync_personal_news(self, scrap_num=5):
        all_uuids = get_all_uuids_that_should_try_posts()
        logger.info(f"Found {len(all_uuids)} uuids to fetch posts")
        fetch_linkedin_posts(all_uuids, scrap_num)
        return {"status": "success"}

    def process_missing_apollo_personal_data(self):
        all_personal_data_uuid = self.personal_data_repository.get_all_uuids_without_apollo()
        logger.info(f"Persons without apollo_data: {len(all_personal_data_uuid)}")
        self.fetch_apollo_data(all_personal_data_uuid)
        return {"status": "success"}

    def get_all_uuids_that_did_not_try_apollo(self):
        all_personal_data_uuid = self.personal_data_repository.get_all_uuids_without_apollo()
        return all_personal_data_uuid

    def fetch_apollo_data(self, uuids: list):
        for uuid in uuids:
            try:
                person = self.persons_repository.get_person(uuid)
                if not person:
                    logger.error(f"Person with uuid {uuid} not found")
                    continue
                if not person.email:
                    logger.error(f"Person with uuid {uuid} has no email")
                    continue
                event = GenieEvent(topic=Topic.APOLLO_NEW_PERSON_TO_ENRICH, data={"person": person.to_dict()})
                event.send()
                logger.info(f"Sent event for {person.email}")
            except Exception as e:
                logger.error(f"Error sending event for {uuid}: {e}")
                break

    def update_action_item(self, user_id, uuid, criteria, description):
        result = self.user_profiles_repository.update_sales_action_item_description(user_id, uuid, criteria, description)
        return {"status": "success"} if result else {"error": str(result)}


