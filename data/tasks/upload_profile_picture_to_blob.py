from data.data_common.dependencies.dependencies import profiles_repository
from data.internal_services.azure_storage_picture_uploader import AzureProfilePictureUploader
from common.genie_logger import GenieLogger

profiles_repository = profiles_repository()
azure_profile_picture_uploader = AzureProfilePictureUploader()
logger = GenieLogger()

def run():
    logger.info("Running fix database task")
    all_profile_pictures_without_blob = profiles_repository.get_all_profiles_pictures_to_upload() # This return a dict with uuid and picture url
    logger.info(f"Number of profiles without blob: {len(all_profile_pictures_without_blob)}")
    for profile_picture_object in all_profile_pictures_without_blob:
        profile_uuid = profile_picture_object.get("uuid")
        profile_picture_url = profile_picture_object.get("picture_url")
        try:
            azure_profile_picture_uploader.upload_image_from_url(profile_picture_url, profile_uuid)
            logger.info(f"Uploaded profile picture to blob for profile: {profile_uuid}")
        except Exception as e:
            logger.error(f"Failed to upload profile picture to blob for profile: {profile_uuid}. Error: {str(e)}")
            continue
    logger.info("Completed fix database task")


if __name__ == "__main__":
    run()
