import os

import requests
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.dependencies.dependencies import profiles_repository, persons_repository
from common.genie_logger import GenieLogger
from common.utils import env_utils
from data.data_common.events.genie_event import GenieEvent

# Set up environment variables for Azure credentials
AZURE_STORAGE_CONNECTION_STRING = env_utils.get("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_NAME = env_utils.get("AZURE_STORAGE_ACCOUNT_NAME")
BLOB_CONTAINER_PICTURES_NAME = env_utils.get("BLOB_CONTAINER_PICTURES_NAME")
DEFAULT_PROFILE_PICTURE = env_utils.get("DEFAULT_PROFILE_PICTURE", "https://monomousumi.com/wp-content/uploads/anonymous-user-8.png")

logger = GenieLogger()

class NotAnImageError(Exception):
    pass

class AzureProfilePictureUploader:
    def __init__(self):
        self.profiles_repository = profiles_repository()
        self.persons_repository = persons_repository()
        self.blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        self.container_client = self.blob_service_client.get_container_client(BLOB_CONTAINER_PICTURES_NAME)

    def is_image_url(self, url: str) -> bool:
        """
        Checks if the URL is an image by examining the Content-Type header
        and reading a small part of the content.
        """
        try:
            # Use a GET request with stream=True to avoid downloading the entire image
            response = requests.get(url, stream=True, timeout=5)
            content_type = response.headers.get("Content-Type", "").lower()

            # Verify if Content-Type starts with 'image/' and read a small chunk to confirm the file
            if content_type.startswith("image/"):
                response.raw.decode_content = True
                response.iter_content(chunk_size=1024).__next__()  # Read a small chunk
                return True
            else:
                logger.info(f"URL content type is not an image: {content_type}")
                return False
        except requests.RequestException as e:
            logger.error(f"Error checking URL content type: {e}")
            return False

    def upload_image_from_url(self, image_url: str, profile_uuid: str):
        # Check if the URL is an image
        if not self.is_image_url(image_url):
            logger.error(f"The URL does not point to an image: {image_url}")
            raise NotAnImageError("The URL does not point to an image.")

        # Use the profile UUID as the blob name
        blob_name = f"{profile_uuid}.jpg"

        # Initialize the BlobServiceClient with the connection string
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_PICTURES_NAME)

        try:
            # Fetch the image content
            response = requests.get(image_url)
            response.raise_for_status()

            # Create a blob client for the image
            blob_client = container_client.get_blob_client(blob_name)

            # Upload the image to Azure Blob Storage
            blob_client.upload_blob(response.content, overwrite=True)
            logger.info(f"Image uploaded successfully as {blob_name}.")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading image: {e}")
        except ResourceExistsError:
            logger.error(f"Blob {blob_name} already exists.")
        except Exception as e:
            logger.error(f"Error uploading image: {e}")

    def handle_profile_picture_upload(self, profile: ProfileDTO):
        """
        Handle the profile picture upload process.
        """
        logger.info(f"Uploading profile picture for {profile.uuid} from {profile.picture_url}")
        try:
            self.upload_image_from_url(profile.picture_url, str(profile.uuid))
            upload_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_PICTURES_NAME}/{profile.uuid}.jpg"
            self.profiles_repository.update_profile_picture(str(profile.uuid), upload_url)
            return True
        except NotAnImageError:
            logger.error(f"\nURL does not point to an image: {profile.picture_url}\n")
            self.profiles_repository.update_profile_picture(str(profile.uuid), DEFAULT_PROFILE_PICTURE)
            person = self.persons_repository.get_person(str(profile.uuid))
            event = GenieEvent(
                topic="FAILED_TO_GET_PROFILE_PICTURE",
                data={"person": person.to_dict()},
            )
            event.send()
            return False
        except Exception as e:
            logger.error(f"Error uploading profile picture for {profile.uuid}: {e}")
            return False

