from common.utils import email_utils
from common.utils.job_utils import fix_and_sort_experience_from_pdl, fix_and_sort_experience_from_apollo
from data.api.base_models import *
from data.data_common.dependencies.dependencies import (
    profiles_repository,
    ownerships_repository,
    meetings_repository,
    tenants_repository,
    persons_repository,
    personal_data_repository,
    companies_repository,
    hobbies_repository,
)
from fastapi import HTTPException

from common.genie_logger import GenieLogger
from data.data_common.utils.str_utils import to_custom_title_case

logger = GenieLogger()


class ProfilesApiService:
    def __init__(self):
        self.profiles_repository = profiles_repository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()
        self.tenants_repository = tenants_repository()
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()
        self.companies_repository = companies_repository()
        self.hobbies_repository = hobbies_repository()

    def get_profiles_for_meeting(self, tenant_id, meeting_id):
        meeting = self.meetings_repository.get_meeting_data(meeting_id)
        if not meeting:
            logger.error(f"Meeting not found for meeting_id: {meeting_id}")
            raise HTTPException(status_code=404, detail="Meeting not found")
        if meeting.tenant_id != tenant_id:
            logger.error(f"Tenant mismatch for meeting_id: {meeting_id}, tenant_id: {tenant_id}")
            raise HTTPException(status_code=403, detail="Tenant mismatch")

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
            logger.error(f"No access to profile with uuid: {uuid} for tenant_id: {tenant_id}")
            raise HTTPException(status_code=403, detail="No access to this profile")

        profile = self.profiles_repository.get_profile_data(uuid)
        if not profile:
            logger.error(f"Profile not found with uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Profile not found")

        return profile

    def get_profile_attendee_info(self, tenant_id, uuid):
        if not self.ownerships_repository.check_ownership(tenant_id, uuid):
            logger.error(f"Profile not found under tenant_id: {tenant_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Profile not found under this tenant")

        profile = self.profiles_repository.get_profile_data(uuid)
        if not profile:
            logger.error(f"Profile not found with uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Could not find profile")

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

    def get_profile_strengths(self, tenant_id, uuid):
        if not self.ownerships_repository.check_ownership(tenant_id, uuid):
            logger.error(f"Profile not found under tenant_id: {tenant_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Profile not found under this tenant")

        profile = self.profiles_repository.get_profile_data(uuid)
        if profile:
            strengths_formatted = "".join([f"\n{strength}\n" for strength in profile.strengths])
            logger.info(f"strengths: {strengths_formatted}")
            return StrengthsListResponse(strengths=profile.strengths)

        logger.error(f"Could not find profile with uuid: {uuid}")
        raise HTTPException(status_code=404, detail="Could not find profile")

    def get_profile_get_to_know(self, tenant_id, uuid):
        if not self.ownerships_repository.check_ownership(tenant_id, uuid):
            logger.error(f"Profile not found under tenant_id: {tenant_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Profile not found under this tenant")

        profile = self.profiles_repository.get_profile_data(uuid)
        logger.info(f"Got profile: {str(profile)[:300]}")
        if profile:
            formated_get_to_know = "".join(
                [(f"\n{key}: {value}\n") for key, value in profile.get_to_know.items()]
            )
            logger.info(f"Get to know: {formated_get_to_know}")
            return GetToKnowResponse(**profile.get_to_know)

        logger.error(f"Could not find profile with uuid: {uuid}")
        raise HTTPException(status_code=404, detail="Could not find profile")

    def get_profile_good_to_know(self, tenant_id, uuid):
        if not self.ownerships_repository.check_ownership(tenant_id, uuid):
            logger.error(f"Profile not found under tenant_id: {tenant_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail={"error": "Could not find profile"})

        profile = self.profiles_repository.get_profile_data(uuid)
        if profile:
            profile_email = self.persons_repository.get_person_email(uuid)
            logger.info(f"Got profile email: {profile_email}")
            personal_news = self.personal_data_repository.get_news_data_by_email(profile_email)
            if not personal_news:
                logger.info(f"No personal news found for {uuid}, getting company news")
                news = self.companies_repository.get_news_data_by_email(profile_email)
            else:
                logger.info(f"Got personal news for {uuid}")
                news = personal_news
            logger.info(f"Got news: {news}")

            hobbies_uuid = profile.hobbies
            logger.info(f"Got hobbies: {hobbies_uuid}")
            hobbies = [self.hobbies_repository.get_hobby(str(hobby_uuid)) for hobby_uuid in hobbies_uuid]
            logger.info(f"Got hobbies: {hobbies}")

            connections = profile.connections

            good_to_know = {
                "news": news,
                "hobbies": hobbies,
                "connections": connections,
            }
            formatted_good_to_know = "".join([f"\n{key}: {value}\n" for key, value in good_to_know.items()])
            logger.info(f"Good to know: {formatted_good_to_know}")
            return GoodToKnowResponse(
                news=news if news else [],
                hobbies=hobbies if hobbies else [],
                connections=connections if connections else [],
            )

        logger.error(f"Could not find profile with uuid: {uuid}")
        raise HTTPException(status_code=404, detail={"error": "Could not find profile"})

    def get_work_experience(self, tenant_id, uuid):
        if not self.ownerships_repository.check_ownership(tenant_id, uuid):
            logger.error(f"Profile {uuid} was not found under tenant {tenant_id}")
            raise HTTPException(
                status_code=404, detail={"error": f"Profile {uuid} was not found under tenant {tenant_id}"}
            )

        personal_data = self.personal_data_repository.get_pdl_personal_data(uuid)
        if personal_data:
            experience = personal_data["experience"]
            fixed_experience = fix_and_sort_experience_from_pdl(experience)
        else:
            personal_data = self.personal_data_repository.get_apollo_personal_data(uuid)
            if not personal_data:
                logger.error(f"Could not find personal data for profile {uuid}")
                raise HTTPException(
                    status_code=404, detail={"error": f"Could not find personal data for profile {uuid}"}
                )
            fixed_experience = fix_and_sort_experience_from_apollo(personal_data)
        logger.info(f"Fixed experience: {fixed_experience}")

        if fixed_experience:
            # Sort experiences by end_date (if end_date is None, treat as current job)
            fixed_experience.sort(key=lambda x: x["end_date"] or "9999-12", reverse=True)

            # Get the last 3 employers with different positions

            final_experience = []
            employers = set()
            for experience in fixed_experience:
                if len(employers) >= 3:
                    break
                company_obj = experience["company"]
                if company_obj:
                    company = company_obj["name"]
                    if company:
                        final_experience.append(experience)
                        employers.add(company)

            # Convert to custom title case for consistency
            return to_custom_title_case(final_experience)

        logger.error(f"Profile {uuid} was not found under tenant {tenant_id}")
        raise HTTPException(
            status_code=404, detail={"error": f"Profile {uuid} was not found under tenant {tenant_id}"}
        )
