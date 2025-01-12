from common.utils import email_utils, env_utils
from common.utils.email_utils import filter_emails_with_additional_domains
from common.utils.job_utils import fix_and_sort_experience_from_pdl, fix_and_sort_experience_from_apollo
from data.api.base_models import *
from data.data_common.utils.persons_utils import determine_profile_category, get_default_individual_sales_criteria, profiles_description
from data.data_common.dependencies.dependencies import (
    profiles_repository,
    ownerships_repository,
    meetings_repository,
    persons_repository,
    personal_data_repository,
    companies_repository,
    hobbies_repository,
)
from data.data_common.repositories.users_repository import UsersRepository
from data.data_common.repositories.user_profiles_repository import UserProfilesRepository
from fastapi import HTTPException

from common.genie_logger import GenieLogger
from data.data_common.utils.str_utils import to_custom_title_case

logger = GenieLogger()


class ProfilesApiService:
    def __init__(self):
        self.profiles_repository = profiles_repository()
        self.user_profiles_repository = UserProfilesRepository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()
        self.users_repository = UsersRepository()
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()
        self.companies_repository = companies_repository()
        self.hobbies_repository = hobbies_repository()

    def get_profiles_and_persons_for_meeting(self, user_id, meeting_id):
        meeting = self.meetings_repository.get_meeting_data(meeting_id)
        if not meeting:
            logger.error(f"Meeting not found for meeting_id: {meeting_id}")
            raise HTTPException(status_code=404, detail="Meeting not found")
        if meeting.user_id != user_id:
            logger.error(f"Tenant mismatch for meeting_id: {meeting_id}, user_id: {user_id}")
            raise HTTPException(status_code=403, detail="Tenant mismatch")
        participants_emails = meeting.participants_emails
        participants = [ParticipantEmail.from_dict(email) for email in participants_emails]
        host_email_list = [email.email_address for email in participants if email.self]
        self_email = host_email_list[0] if host_email_list else None
        self_domain = (self_email.split("@")[1] if "@" in self_email else None) if self_email else None
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

        if not mini_profiles and not mini_persons and meeting.classification.value == "external":
            logger.error("No profiles found in this meeting")
            raise HTTPException(
                status_code=404,
                detail="No profiles found in this meeting. Might be that we are still processing the data.",
            )

        mini_participants = {
            "profiles": mini_profiles,
            "persons": mini_persons,
        }
        return mini_participants

    def get_profile_info(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            logger.error(f"No access to profile with uuid: {uuid} for user_id: {user_id}")
            raise HTTPException(status_code=403, detail="No access to this profile")

        profile = self.profiles_repository.get_profile_data(uuid)
        if not profile:
            logger.error(f"Profile not found with uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Profile not found")

        return profile

    def get_profile_attendee_info(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            logger.error(f"Profile not found under user_id: {user_id} for uuid: {uuid}")
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
            "work_history_summary": profile.work_history_summary,
        }
        logger.info(f"Attendee info: {profile}")
        return AttendeeInfo(**profile)

    def get_profile_strengths(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            email = self.users_repository.get_email_by_user_id(user_id)
            if email and email_utils.is_genie_admin(email):
                logger.info(f"Genie admin has access to profile with uuid: {uuid}")
            else:
                logger.error(f"Profile not found under user_id: {user_id} for uuid: {uuid}")
                raise HTTPException(status_code=404, detail="Profile not found under this user")

        profile = self.profiles_repository.get_profile_data(uuid)
        if profile:
            strengths_formatted = "".join([f"\n{strength}\n" for strength in profile.strengths])
            logger.info(f"strengths: {strengths_formatted}")
            category = determine_profile_category(profile.strengths)
            sales_criteria = self.user_profiles_repository.get_sales_criteria(uuid, user_id) or profile.sales_criteria
            return StrengthsListResponse(strengths=profile.strengths, profile_category=category, sales_criteria=sales_criteria)

        logger.error(f"Could not find profile with uuid: {uuid}")
        raise HTTPException(status_code=404, detail="Could not find profile")

    def get_profile_get_to_know(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            logger.error(f"Profile not found under user_id: {user_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Profile not found under this user")

        profile = self.profiles_repository.get_profile_data(uuid)
        user_specific_get_to_know = self.user_profiles_repository.get_get_to_know(uuid, user_id)
        if user_specific_get_to_know:
            logger.info(f"Got user specific get to know for : {profile.name}")
            profile.get_to_know = user_specific_get_to_know
        logger.info(f"Got profile: {str(profile)[:300]}")
        if profile:
            for category, phrases in profile.get_to_know.items():
                if category == 'avoid':
                    profile.get_to_know[category] = phrases[:1]
                else:
                    profile.get_to_know[category] = phrases[:2]
            formated_get_to_know = "".join(
                [(f"\n{key}: {value}\n") for key, value in profile.get_to_know.items()]
            )
            logger.info(f"Get to know: {formated_get_to_know}")
            return GetToKnowResponse(**profile.get_to_know)

        logger.error(f"Could not find profile with uuid: {uuid}")
        raise HTTPException(status_code=404, detail="Could not find profile")

    def get_profile_action_items(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            logger.error(f"Profile not found under user_id: {user_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail="Profile not found under this user")

        # profile = self.profiles_repository.get_profile_data(uuid)
        action_items = self.user_profiles_repository.get_sales_action_items(uuid, user_id)
        if action_items:
            logger.info(f"Got tenant specific action items: {action_items}")
            return ActionItemsResponse.from_action_items_list(action_items=action_items)
        # else:
        #     logger.info(f"No tenant specific action items found for {uuid}, getting default action items")
        #     sales_criteria = self.tenant_profiles_repository.get_sales_criteria(uuid, tenant_id)
        #     if sales_criteria:
        #         action_items = self.
        #         logger.info(f"Got default action items: {action_items}")
        #         return ActionItemsResponse(action_items=action_items)

        logger.error(f"Could not find profile action items with uuid: {uuid}")
        raise HTTPException(status_code=404, detail="Could not find profile action items")

    def get_profile_good_to_know(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            logger.error(f"Profile not found under user_id: {user_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail={"error": "Could not find profile"})

        profile = self.profiles_repository.get_profile_data(uuid)
        if profile:
            profile_email = self.persons_repository.get_person_email(uuid)
            logger.info(f"Got profile email: {profile_email}")
            news = self.personal_data_repository.get_news_data_by_email(profile_email)
            if not news:
                logger.info(f"No personal news found for {uuid}, getting company news")
                news = []

            hobbies_uuid = profile.hobbies
            logger.info(f"Got hobbies: {hobbies_uuid}")
            hobbies = [self.hobbies_repository.get_hobby(str(hobby_uuid)) for hobby_uuid in hobbies_uuid]
            if not hobbies:
                logger.info(f"No hobbies found for {uuid}")
                hobbies_names = self.personal_data_repository.get_hobbies_by_email(profile_email)
                if hobbies_names:
                    hobbies = [
                        self.hobbies_repository.get_hobby_by_name(hobby_name)
                        for hobby_name in hobbies_names
                        if hobby_name
                    ]
                    hobbies = [hobby for hobby in hobbies if (hobby and hobby.get("icon_url"))]
            logger.info(f"Got hobbies: {hobbies}")

            connections = profile.connections

            good_to_know = {
                "news": news,
                "hobbies": hobbies,
                "connections": connections,
            }
            formatted_good_to_know = "".join([f"\n{key}: {str(value)[:300]}\n" for key, value in good_to_know.items()])
            logger.info(f"Good to know: {formatted_good_to_know}")
            return GoodToKnowResponse(
                news=news if news else [],
                hobbies=hobbies if hobbies else [],
                connections=connections if connections else [],
            )

        logger.error(f"Could not find profile with uuid: {uuid}")
        raise HTTPException(status_code=404, detail={"error": "Could not find profile"})

    def get_sales_criteria(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            logger.error(f"Profile not found under user_id: {user_id} for uuid: {uuid}")
            raise HTTPException(status_code=404, detail={"error": "Could not find profile"})

        sales_criteria = self.user_profiles_repository.get_sales_criteria(uuid, user_id)
        if not sales_criteria:
            logger.info(f"No tenant's sales criteria found for {uuid}, getting profile's default sales criteria")
            profile_dto = self.profiles_repository.get_profile_data(uuid)
            sales_criteria = profile_dto.sales_criteria
            if sales_criteria:
                self.profiles_repository.save_profile(profile_dto)
            else:
                logger.error(f"ERROR: No sales criteria found for {uuid}")
        logger.info(f"Got sales criteria: {sales_criteria}")
        if not sales_criteria:
            return SalesCriteriaResponse.from_list([])
        return SalesCriteriaResponse.from_list(sales_criteria)

    def get_work_experience(self, user_id, uuid):
        if not self.ownerships_repository.check_ownership(user_id, uuid):
            logger.error(f"Profile {uuid} was not found under user: {user_id}")
            raise HTTPException(
                status_code=404, detail={"error": f"Profile {uuid} was not found under user {user_id}"}
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
                if len(employers) >= 4:
                    break
                company_obj = experience["company"]
                if company_obj:
                    company = company_obj["name"]
                    if company:
                        final_experience.append(experience)
                        employers.add(company)

            # Convert to custom title case for consistency
            return to_custom_title_case(final_experience)

        logger.error(f"Profile {uuid} was not found under tenant {user_id}")
        raise HTTPException(
            status_code=404, detail={"error": f"Profile {uuid} was not found under tenant {user_id}"}
        )
    
    def get_profile_category_stats(self):
        profiles = self.profiles_repository.get_all_profiles()
        categories_count = {}
        strengths_count = {}
        for profile in profiles:
            for strength in profile.strengths:
                if strength.strength_name in strengths_count:
                    strengths_count[strength.strength_name] += 1
                else:
                    strengths_count[strength.strength_name] = 1
            profile_category = determine_profile_category(profile.strengths)
            if profile_category.category in categories_count:
                categories_count[profile_category.category] += 1
            else:
                categories_count[profile_category.category] = 1

        logger.info(f"Profile strengths count: {strengths_count}")
        logger.info(f"Profile categories count: {categories_count}")
        for key, value in categories_count.items():
            print(f"{key} - {value}")

