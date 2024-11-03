import os
import requests
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from data.data_common.dependencies.dependencies import profiles_repository, logger

from common.utils import env_utils
from data.data_common.repositories.profiles_repository import DEFAULT_PROFILE_PICTURE

profiles_repository = profiles_repository()

# Set up environment variables for Azure credentials
AZURE_STORAGE_CONNECTION_STRING = env_utils.get("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_NAME = env_utils.get("AZURE_STORAGE_ACCOUNT_NAME")
BLOB_CONTAINER_PICTURES_NAME = env_utils.get("BLOB_CONTAINER_PICTURES_NAME")
DEFAULT_PROFILE_PICTURE = env_utils.get("DEFAULT_PROFILE_PICTURE", "https://monomousumi.com/wp-content/uploads/anonymous-user-8.png")

class NotAnImageError(Exception):
    pass


def is_image_url(url: str) -> bool:
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


# Function to download and upload the image
def upload_image_from_url(image_url: str, profile_uuid: str):
    # Check if the URL is an image
    if not is_image_url(image_url):
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
        logger.error(f"An error occurred: {e}")


def get_all_profile_pictures_url():
    """
    Get all profile pictures URL from the database.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing the profile uuid and picture url.
    """
    all_profile_picture_urls = profiles_repository.get_all_profiles_pictures_to_upload()
    return all_profile_picture_urls


def handle_profile_picture_upload(profile_pictures_urls: list[dict]):
    """
    Handle the profile picture upload process.
    """
    for profile_picture_url in profile_pictures_urls[:50]:
        profile_uuid = profile_picture_url.get("uuid")
        picture_url = profile_picture_url.get("picture_url")
        logger.info(f"Uploading profile picture for {profile_uuid} from {picture_url}")
        try:
            upload_image_from_url(picture_url, profile_uuid)
            upload_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_PICTURES_NAME}/{profile_uuid}.jpg"
            profiles_repository.update_profile_picture(profile_uuid, upload_url)
        except NotAnImageError:
            logger.error(f"\n\n\n\n\n\nURL does not point to an image: {picture_url}\n\n\n\n\n\n")
            profiles_repository.update_profile_picture(profile_uuid, DEFAULT_PROFILE_PICTURE)

        except Exception as e:
            logger.error(f"Error uploading profile picture for {profile_uuid}: {e}")

            continue

if __name__ == "__main__":
    # image_url = "https://frontedresources.blob.core.windows.net/images/cg-balaji.jpg"
    # profile_uuid = "Rak Bodek"  # Replace with the actual profile UUID
    # upload_image_from_url(image_url, profile_uuid)
    all_profiles_urls = get_all_profile_pictures_url()
    logger.info(f"All profile pictures URL: {all_profiles_urls}")
    logger.info(f"Got {len(all_profiles_urls)} profile pictures URL")
    handle_profile_picture_upload(all_profiles_urls)

