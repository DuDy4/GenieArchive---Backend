import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data.importers.profile_pictures import get_profile_picture

from common.genie_logger import GenieLogger

from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
)

logger = GenieLogger()

persons_repository = persons_repository()
personal_data_repository = personal_data_repository()
profiles_repository = profiles_repository()


def get_all_persons_with_linkedin_url_or_social_media_links():
    profiles_uuid = profiles_repository.get_all_profiles_without_profile_picture()
    profiles_uuid = profiles_uuid[:10]
    for profile_uuid in profiles_uuid:
        profile_uuid = str(profile_uuid)
        social_media_links = personal_data_repository.get_social_media_links(profile_uuid)
        logger.info(f"Social media links for {profile_uuid}: {social_media_links}")
        person = persons_repository.get_person(profile_uuid)
        logger.info(f"Person for {profile_uuid}: {person}")
        profile_picture = get_profile_picture(person, social_media_links)
        logger.info(f"Profile picture for {person.uuid}: {profile_picture}")
        if profile_picture:
            profiles_repository.update_profile_picture(profile_uuid, profile_picture)
    logger.info(f"Found {len(profiles_uuid)} profiles without profile picture")


get_all_persons_with_linkedin_url_or_social_media_links()
