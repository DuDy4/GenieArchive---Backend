from common.utils import email_utils
from data.api.base_models import *
from data.data_common.dependencies.dependencies import (
    profiles_repository,
    ownerships_repository,
    meetings_repository,
    tenants_repository,
    persons_repository,
    personal_data_repository,
)
from fastapi import HTTPException
from common.genie_logger import GenieLogger

logger = GenieLogger()


class ProfilesApiService:
    def __init__(self):
        self.profiles_repository = profiles_repository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()
        self.tenants_repository = tenants_repository()
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()

    def get_profiles_for_meeting(self, tenant_id, meeting_id):
        meeting = self.meetings_repository.get_meeting_data(meeting_id)
        if not meeting:
            return {"error": "Meeting not found"}
        if meeting.tenant_id != tenant_id:
            return {"error": "Tenant mismatch"}
        tenant_email = self.tenants_repository.get_tenant_email(tenant_id)
        logger.info(f"Tenant email: {tenant_email}")
        participants_emails = meeting.participants_emails
        logger.debug(f"Participants emails: {participants_emails}")
        filtered_participants_emails = email_utils.filter_emails(
            host_email=tenant_email, participants_emails=participants_emails
        )
        logger.info(f"Filtered participants emails: {filtered_participants_emails}")
        filtered_emails = filtered_participants_emails
        logger.info(f"Filtered emails: {filtered_emails}")
        persons = []
        for email in filtered_emails:
            person = self.persons_repository.find_person_by_email(email)
            if person:
                persons.append(person)
        logger.info(f"Got persons for the meeting: {[persons.uuid for persons in persons]}")
        profiles = []
        for person in persons:
            profile = self.profiles_repository.get_profile_data(person.uuid)
            logger.info(f"Got profile: {str(profile)[:300]}")
            if profile:
                profiles.append(profile)
        logger.info(f"Sending profiles: {[profile.uuid for profile in profiles]}")
        return [MiniProfileResponse.from_profile_dto(profiles[i], persons[i]) for i in range(len(profiles))]

    def get_profile_info(self, tenant_id, uuid):
        if not self.ownerships_repository.check_ownership(tenant_id, uuid):
            raise HTTPException(status_code=403, detail="No access to this profile")

        profile = self.profiles_repository.get_profile_data(uuid)
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")

        return profile

    def get_profile_attendee_info(self, tenant_id, uuid):
        if not self.ownerships_repository.check_ownership(tenant_id, uuid):
            return {"error": "Profile not found under this tenant"}
        profile = self.profiles_repository.get_profile_data(uuid)
        if not profile:
            return {"error": "Could not find profile"}

        # This will Upper Camel Case and Titleize the values in the profile
        profile = ProfileDTO.from_dict(profile.to_dict())

        picture = profile.picture_url
        name = titleize_name(profile.name)
        company = profile.company
        position = profile.position
        links = self.personal_data_repository.get_social_media_links(uuid)
        logger.info(f"Got links: {links}, type: {type(links)}")
        profile = {
            "picture": picture,
            "name": name,
            "company": company,
            "position": position,
            "social_media_links": SocialMediaLinksList.from_list(links).to_list() if links else [],
        }
        logger.info(f"Attendee info: {profile}")
        return AttendeeInfo(**profile)
