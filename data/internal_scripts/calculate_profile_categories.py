from common.genie_logger import GenieLogger
from data.data_common.repositories.profiles_repository import ProfilesRepository

logger = GenieLogger()
profiles_repository = ProfilesRepository()

all_profiles = profiles_repository.get_all_profiles_without_category()
logger.info(f"Found {len(all_profiles)} profiles")
for profile in all_profiles:
    logger.info(f"Updating profile {profile}")
    profiles_repository.update_profile_category(str(profile.uuid), profile.profile_category)

logger.info("Updated all profiles with profile_category")
