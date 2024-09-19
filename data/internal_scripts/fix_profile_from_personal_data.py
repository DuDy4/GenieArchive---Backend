import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger
from data.api.api_services_classes.admin_api_services import AdminApiService
from data.data_common.dependencies.dependencies import profiles_repository, persons_repository

admin_api_service = AdminApiService()

logger = GenieLogger()

profiles_repository = profiles_repository()
persons_repository = persons_repository()


def get_all_persons_with_personal_data_but_no_profile():
    profiles_uuid = profiles_repository.get_missing_profiles()
    logger.info(f"Found {len(profiles_uuid)} profiles without profile")
    for profile_uuid in profiles_uuid[:5]:
        logger.info(f"Syncing profile for {profile_uuid}")
        result = admin_api_service.sync_profile(profile_uuid)
        if result.get("error"):
            logger.error(f"Error syncing profile: {result}")
            email = persons_repository.get_person(profile_uuid).email
            result = admin_api_service.sync_email(profile_uuid)
            if result.get("error"):
                logger.error(f"Error syncing email: {result}")
            else:
                logger.info(f"Email synced for {email}")


get_all_persons_with_personal_data_but_no_profile()
