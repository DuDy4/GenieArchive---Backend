from common.utils import env_utils
from data.data_common.dependencies.dependencies import profiles_repository, persons_repository
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.internal_services.azure_storage_picture_uploader import AzureProfilePictureUploader, NotAnImageError
from common.genie_logger import GenieLogger

profiles_repository = profiles_repository()
persons_repository = persons_repository()
azure_profile_picture_uploader = AzureProfilePictureUploader()
logger = GenieLogger()

AZURE_STORAGE_CONNECTION_STRING = env_utils.get("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_NAME = env_utils.get("AZURE_STORAGE_ACCOUNT_NAME")
BLOB_CONTAINER_PICTURES_NAME = env_utils.get("BLOB_CONTAINER_PICTURES_NAME")
DEFAULT_PROFILE_PICTURE = env_utils.get("DEFAULT_PROFILE_PICTURE", "https://monomousumi.com/wp-content/uploads/anonymous-user-8.png")


def run():
    logger.info("Running fix database task")
    all_profile_pictures_without_blob = profiles_repository.get_all_profiles_pictures_to_upload() # This return a dict with uuid and picture url
    logger.info(f"Number of profiles without blob: {len(all_profile_pictures_without_blob)}")
    for profile_picture_object in all_profile_pictures_without_blob:
        profile_uuid = profile_picture_object.get("uuid")
        profile_picture_url = profile_picture_object.get("picture_url")
        logger.info(f"Profile picture url: {profile_picture_url}")
        logger.info(f"Uploading profile picture for {profile_uuid} from {profile_picture_url}")
        try:
            azure_profile_picture_uploader.upload_image_from_url(profile_picture_url, str(profile_uuid))
            upload_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_PICTURES_NAME}/{profile_uuid}.jpg"
            profiles_repository.update_profile_picture(str(profile_uuid), upload_url)
            logger.info(f"Profile picture uploaded successfully for {profile_uuid}")
        except NotAnImageError:
            logger.error(f"\nURL does not point to an image: {profile_picture_url}\n")
            profiles_repository.update_profile_picture(str(profile_uuid), DEFAULT_PROFILE_PICTURE)
            person = persons_repository.get_person(str(profile_uuid))
            event = GenieEvent(
                topic=Topic.FAILED_TO_GET_PROFILE_PICTURE,
                data={"person": person.to_dict()},
            )
            event.send()
            logger.error(f"Profile picture upload failed for {profile_uuid}")
        except Exception as e:
            logger.error(f"Error uploading profile picture for {profile_uuid}: {str(e)}")

    logger.info("Completed fix database task")
logger.info("Completed fix database task")


if __name__ == "__main__":
    run()
