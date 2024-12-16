from datetime import datetime, timedelta
import hashlib

from common.utils import email_utils
from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.news_data_dto import NewsData, SocialMediaPost
from data.data_common.data_transfer_objects.meeting_dto import MeetingClassification, MeetingDTO
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.dependencies.dependencies import (
    meetings_repository,
    persons_repository,
    companies_repository,
    profiles_repository,
    tenants_repository,
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
    InternalMiniPersonResponse,
)
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
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
        self.tenants_repository = tenants_repository()

    def get_all_meetings(self, tenant_id):
        if not tenant_id:
            logger.error("Tenant ID not provided")
            raise HTTPException(status_code=400, detail="Tenant ID not provided")
        logger.info(f"About to get all meetings for tenant_id: {tenant_id}")

        meetings = self.meetings_repository.get_all_meetings_by_tenant_id(tenant_id)
        dict_meetings = [meeting.to_dict() for meeting in meetings]
        # sort by meeting.start_time
        dict_meetings.sort(key=lambda x: x["start_time"])
        logger.info(f"About to sent to {tenant_id} meetings: {len(dict_meetings)}")
        return dict_meetings

    def get_all_meetings_with_selected_date(self, tenant_id, selected_datetime: datetime = datetime.now()):
        if not tenant_id:
            logger.error("Tenant ID not provided")
            raise HTTPException(status_code=400, detail="Tenant ID not provided")
        logger.info(f"About to get all meetings for tenant_id: {tenant_id}")

        meetings = self.meetings_repository.get_all_meetings_by_tenant_id_in_datetime(tenant_id, selected_datetime)
        dict_meetings = [meeting.to_dict() for meeting in meetings]
        # sort by meeting.start_time
        dict_meetings.sort(key=lambda x: x["start_time"])
        logger.info(f"About to sent to {tenant_id} meetings: {len(dict_meetings)}")
        return dict_meetings

    def delete_meeting(self, tenant_id, meeting_uuid):
        meeting = self.meetings_repository.get_meeting_data(meeting_uuid)
        if not meeting:
            logger.error(f"Meeting not found for meeting_uuid: {meeting_uuid}")
            raise HTTPException(status_code=404, detail="Meeting not found")
        if meeting.tenant_id != tenant_id:
            logger.error(f"Tenant mismatch for tenant_id: {tenant_id}, meeting_uuid: {meeting_uuid}")
            raise HTTPException(status_code=400, detail="Tenant mismatch")
        self.meetings_repository.delete(meeting_uuid)
        return {"status": "success", "message": "Meeting deleted"}

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
            logger.info(f"Person: {person}")
            if person:
                profile_picture = self.profiles_repository.get_profile_picture(person.uuid)
                mini_person = InternalMiniPersonResponse.from_person_dto(person, profile_picture)
                participants.append(mini_person)
            else:
                mini_person = InternalMiniPersonResponse.from_dict({"uuid": get_uuid4(), "email": email})
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

        mini_participants, domain_emails = self.handle_participants_overview(meeting.participants_emails)

        logger.info(f"Meeting participants: {mini_participants}")

        mid_company = self.handle_company_overview(domain_emails)

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

    def handle_company_overview(self, domain_emails):
        companies = []

        for domain in domain_emails:
            company = self.companies_repository.get_company_from_domain(domain)
            logger.info(f"Company: {str(company)[:300]}")
            if company:
                companies.append(company)

        if not companies:
            logger.error("No companies found in this meeting")
            return MidMeetingCompany(
                name="Unknown",
            )

        company = companies[0] if companies else None
        logger.info(f"Company: {str(company)[:300]}")
        mid_company = None
        if company:
            if company.news:
                if len(company.news) > 3:
                    news = []
                    domain = company.domain
                    try:
                        for new in company.news:
                            link = HttpUrl(
                                new.get("link") if new and isinstance(new, dict) else str(new.link)
                            )
                            if isinstance(new, dict):
                                new["link"] = link
                            elif isinstance(new, NewsData):
                                new.link = link
                            if domain not in str(link):
                                news.append(new)
                        company.news = news[:3]
                    except Exception as e:
                        logger.error(f"Error processing company news: {e}")
                        company.news = []
            else:
                company.news = []
            mid_company = titleize_values(MidMeetingCompany.from_company_dto(company))

            logger.info(f"Company: {str(mid_company)[:300]}")
        return mid_company

    def handle_participants_overview(self, participants_emails):
        participants = [ParticipantEmail.from_dict(email) for email in participants_emails]
        host_email_list = [email.email_address for email in participants if email.self]
        self_email = host_email_list[0] if host_email_list else None
        self_domain = self_email.split("@")[1] if "@" in self_email else None
        if self_domain:
            additional_domains = self.companies_repository.get_additional_domains(self_email.split("@")[1])
            filtered_participants_emails = email_utils.filter_emails_with_additional_domains(self_email, participants_emails, additional_domains)
        else:
            filtered_participants_emails = email_utils.filter_emails(self_email, participants_emails)
        logger.info(f"Filtered participants: {filtered_participants_emails}")

        domain_emails = [email.split("@")[1] for email in filtered_participants_emails]
        domain_emails = list(set(domain_emails))
        logger.info(f"Domain emails: {domain_emails}")

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
        return mini_participants, domain_emails
    
    
    def create_fake_meeting(self, tenant_id: str, emails: list[str]):
        # template_meeting = {'kind': 'calendar#event', 'id': '4mbrg4t0ri1e20dd09339446rp_20241202T070000Z', 'status': 'confirmed', 'created': '2024-11-25T12:52:39.000Z', 'updated': '2024-12-02T09:15:58.731Z', 'summary': 'My Genie Meeting', 'creator': {'email': 'asaf@genieai.ai'}, 'organizer': {'email': 'asaf@genieai.ai'}, 'start': {'dateTime': '2024-12-02T09:00:00+02:00', 'timeZone': 'Asia/Jerusalem'}, 'end': {'dateTime': '2024-12-02T10:00:00+02:00', 'timeZone': 'Asia/Jerusalem'}, 'recurringEventId': '4mbrg4t0ri1e20dd09339446rp', 'originalStartTime': {'dateTime': '2024-12-02T09:00:00+02:00', 'timeZone': 'Asia/Jerusalem'}, 'hangoutLink': 'https://dino-chrome.com/', 'eventType': 'default'}
        attendee_template = "{'email': '%s', 'responseStatus': 'accepted'}"
        self_attendee_template = "{'email': '%s', 'responseStatus': 'accepted', 'self': True}"
        self_email = self.tenants_repository.get_tenant_email(tenant_id)
        combined = tenant_id + "|" + "|".join(sorted(emails))
        hash = hashlib.sha256(combined.encode()).hexdigest()
        attendees = [eval(attendee_template % email) for email in emails]
        attendees.append(eval(self_attendee_template % self_email))
        current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

        meeting = MeetingDTO(
            uuid=get_uuid4(),
            google_calendar_id=hash,
            tenant_id=tenant_id,
            link="https://dino-chrome.com",
            location="https://dino-chrome.com",
            subject="My Genie Meeting",
            participants_emails=attendees,
            participants_hash=None,
            start_time=(current_time + timedelta(hours=24)).isoformat(),
            end_time=(current_time + timedelta(hours=25)).isoformat(),
            classification=MeetingClassification.EXTERNAL,
            fake=True,
        )
        self.meetings_repository.save_meeting(meeting)
        for email in emails:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
                data={"tenant_id": tenant_id, "email": email},
            )
            event.send()
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"tenant_id": tenant_id, "email": email},
            )
            event.send()
