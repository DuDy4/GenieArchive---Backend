from common.utils import email_utils
from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.company_dto import NewsData
from data.data_common.data_transfer_objects.meeting_dto import MeetingClassification, MeetingDTO
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.dependencies.dependencies import (
    meetings_repository,
    persons_repository,
    companies_repository,
    profiles_repository,
)
from data.api.base_models import (
    MiniMeeting,
    PrivateMeetingOverviewResponse,
    MiniPersonResponse,
    InternalMeetingOverviewResponse,
    ParticipantEmail,
    MidMeetingCompany,
    MiniProfileResponse,
    MiniMeetingOverviewResponse,
)
from pydantic import HttpUrl
from fastapi import HTTPException
from common.genie_logger import GenieLogger
from data.data_common.utils.str_utils import titleize_values

logger = GenieLogger()


class MeetingsApiService:
    def __init__(self):
        self.meetings_repository = meetings_repository()
        self.persons_repository = persons_repository()
        self.profiles_repository = profiles_repository()
        self.companies_repository = companies_repository()

    def get_all_meetings(self, tenant_id):
        if not tenant_id:
            logger.error("Tenant ID not provided")
            raise HTTPException(status_code=400, detail="Tenant ID not provided")

        meetings = self.meetings_repository.get_all_meetings_by_tenant_id(tenant_id)
        dict_meetings = [meeting.to_dict() for meeting in meetings]
        # sort by meeting.start_time
        dict_meetings.sort(key=lambda x: x["start_time"])
        logger.info(f"About to sent to {tenant_id} meetings: {len(dict_meetings)}")
        return dict_meetings

    def get_meeting_overview(self, tenant_id, meeting_uuid):
        meeting = self.meetings_repository.get_meeting_data(meeting_uuid)
        if not meeting:
            logger.error(f"Meeting not found for meeting_uuid: {meeting_uuid}")
            raise HTTPException(status_code=404, detail="Meeting not found")
        if meeting.tenant_id != tenant_id:
            logger.error(f"Tenant mismatch for tenant_id: {tenant_id}, meeting_uuid: {meeting_uuid}")
            raise HTTPException(status_code=400, detail="Tenant mismatch")

        if meeting.classification.value == MeetingClassification.PRIVATE.value:
            private_meeting = MiniMeeting.from_meeting_dto(meeting)
            logger.info(f"Private meeting: {private_meeting}")
            return PrivateMeetingOverviewResponse(meeting=private_meeting)

        if meeting.classification.value == MeetingClassification.INTERNAL.value:
            return self.handle_internal_meeting_overview(meeting)

        if meeting.classification.value == MeetingClassification.EXTERNAL.value:
            return self.handle_external_meeting_overview(meeting)

    def handle_internal_meeting_overview(self, meeting):
        mini_meeting = MiniMeeting.from_meeting_dto(meeting)
        logger.info(f"Mini meeting: {mini_meeting}")
        participants_emails = meeting.participants_emails
        participants = []
        for email_object in participants_emails:
            email = email_object.get("email")
            if not email:
                logger.warning(f"Email not found in: {email_object}")
                continue
            person = self.persons_repository.find_person_by_email(email)
            if person:
                mini_person = MiniPersonResponse.from_dict(person.to_dict())
                logger.debug(f"Person: {mini_person}")
                participants.append(mini_person)
            else:
                mini_person = MiniPersonResponse.from_dict({"uuid": get_uuid4(), "email": email})
                logger.debug(f"Person: {mini_person}")
                participants.append(mini_person)
        internal_meeting_overview = InternalMeetingOverviewResponse(
            meeting=mini_meeting,
            participants=participants,
        )
        logger.info(f"Internal meeting overview: {internal_meeting_overview}")
        return internal_meeting_overview

    def handle_external_meeting_overview(self, meeting):
        try:
            mini_meeting = MiniMeeting.from_meeting_dto(meeting)
        except Exception as e:
            logger.error(f"Error creating mini meeting: {e}")
            raise HTTPException(status_code=500, detail="Could not process meeting")

        logger.info(f"Mini meeting: {mini_meeting}")

        participants = [ParticipantEmail.from_dict(email) for email in meeting.participants_emails]
        host_email_list = [email.email_address for email in participants if email.self]
        host_email = host_email_list[0] if host_email_list else None
        logger.debug(f"Host email: {host_email}")
        filtered_participants_emails = email_utils.filter_emails(host_email, participants)
        logger.info(f"Filtered participants: {filtered_participants_emails}")

        domain_emails = [email.split("@")[1] for email in filtered_participants_emails]
        domain_emails = list(set(domain_emails))
        logger.info(f"Domain emails: {domain_emails}")

        companies = []

        for domain in domain_emails:
            company = self.companies_repository.get_company_from_domain(domain)
            logger.info(f"Company: {str(company)[:300]}")
            if company:
                companies.append(company)

        if not companies:
            logger.error("No companies found in this meeting")
            raise HTTPException(
                status_code=404,
                detail="No companies found in this meeting. Might be that we are still processing the data.",
            )

        company = companies[0] if companies else None
        logger.info(f"Company: {str(company)[:300]}")
        if company:
            news = []
            domain = company.domain
            try:
                for new in company.news:
                    link = HttpUrl(new.get("link") if new and isinstance(new, dict) else str(new.link))
                    if isinstance(new, dict):
                        new["link"] = link
                    elif isinstance(new, NewsData):
                        new.link = link
                    if domain not in str(link):
                        news.append(new)
                company.news = news[:3]
                logger.debug(f"Company news: {str(company.news)[:300]}")
            except Exception as e:
                logger.error(f"Error processing company news: {e}")
                company.news = []
            mid_company = titleize_values(MidMeetingCompany.from_company_dto(company))

        logger.info(f"Company: {str(mid_company)[:300]}")

        mini_profiles = []
        mini_persons = []

        for participant in filtered_participants_emails:
            profile = self.profiles_repository.get_profile_data_by_email(participant)
            if profile:
                person = PersonDTO.from_dict({"email": participant})
                person.uuid = profile.uuid
                logger.info(f"Person: {person}")
                profile_response = MiniProfileResponse.from_profile_dto(profile, person)
                logger.info(f"Profile: {profile_response}")
                if profile_response:
                    mini_profiles.append(profile_response)
            else:
                person = self.persons_repository.find_person_by_email(participant)
                logger.info(f"Person: {person}")
                if person:
                    person_response = MiniPersonResponse.from_person_dto(person)
                    logger.info(f"Person response: {person_response}")
                else:
                    person = PersonDTO.from_dict({"email": participant})
                    person_response = MiniPersonResponse.from_dict(person.to_dict())
                logger.debug(f"Person: {person_response}")
                mini_persons.append(person_response)

        if not mini_profiles and not mini_persons:
            logger.error("No profiles found in this meeting")
            raise HTTPException(
                status_code=404,
                detail="No profiles found in this meeting. Might be that we are still processing the data.",
            )

        mini_participants = {
            "profiles": mini_profiles,
            "persons": mini_persons,
        }

        logger.info(f"Meeting participants: {mini_participants}")

        try:
            mini_overview = MiniMeetingOverviewResponse(
                meeting=mini_meeting,
                company=mid_company,
                participants=mini_participants,
            )
        except Exception as e:
            logger.error(f"Error creating mini overview: {e}")
            raise HTTPException(status_code=500, detail="Error creating mini overview")

        logger.info(f"Mini overview: {str(mini_overview)[:300]}")

        return mini_overview
