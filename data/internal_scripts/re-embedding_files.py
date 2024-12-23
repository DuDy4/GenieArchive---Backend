import asyncio
from datetime import datetime, timedelta

from common.utils import env_utils
from common.utils.file_utils import get_file_extension, extract_text_from_pdf, get_file_name_from_url, \
    extract_text_from_docx, extract_text_from_pptx
from data.api_services.embeddings import GenieEmbeddingsClient
from data.data_common.repositories.file_upload_repository import FileUploadRepository, FileStatusEnum
from data.data_common.data_transfer_objects.file_upload_dto import FileUploadDTO
from common.genie_logger import GenieLogger
from ai.langsmith.langsmith_loader import Langsmith
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from data.internal_services.files_upload_service import FileUploadService

file_upload_repository = FileUploadRepository()
langsmith = Langsmith()
embeddings_client = GenieEmbeddingsClient()
logger = GenieLogger()

DEV_MODE = env_utils.get("DEV_MODE", "")


UPLOADED_MATERIALS_CONTINAER_NAME = env_utils.get("UPLOADED_MATERIALS_CONTINAER_NAME")

# Should get all existing files from the database
def get_all_files_from_db():
    return file_upload_repository.get_all_files()

def get_file_from_url(blob_url: str, connection_string: str):
    try:
        # Extract the container and blob name from the URL
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(blob_url)

        # Fetch the blob properties
        blob_properties = blob_client.get_blob_properties()

        # Extract blob metadata (or use the blob name as the ID)
        blob_name = blob_client.blob_name
        logger.info(f"Blob name (file ID): {blob_name}")

        # You can return metadata or other properties as needed
        return blob_name
    except Exception as e:
        logger.error(f"Failed to fetch file ID from URL: {blob_url}. Error: {e}")
        return None


async def re_embed_file(file: FileUploadDTO):
    # if file.status != FileStatusEnum.PROCESSING:
    #     logger.info(f"Skipping file {file.file_name} with status {file.status}")
    #     return
    file_upload_repository.update_file_status(str(file.uuid), FileStatusEnum.PROCESSING)
    # get the file text content
    blob_name = f"{file.email}/uploads/{file.file_name}"
    complete_blob_url = f"https://useruploadedmaterials.blob.core.windows.net/{UPLOADED_MATERIALS_CONTINAER_NAME}/{blob_name}"
    logger.info(f"Fetching document content from {complete_blob_url}")
    text = await fetch_doc_content(complete_blob_url)
    if not text:
        logger.error(f"Could not fetch document content from {blob_name}")
        return
    # process the file content
    logger.info(f"Fetched file text - now processing")
    if not file.file_hash:
        file.file_hash = FileUploadDTO.calculate_hash(text) # update the file hash
        file_upload_repository.update_file_hash(file)
    processed_content = await langsmith.preprocess_uploaded_file_content(text)
    processed_content_text = processed_content.content
    if not file.categories:
        file_categories = await langsmith.run_prompt_doc_categories(processed_content_text)
        if file_categories:
            file.categories = file_categories
            file_upload_repository.update_file_categories(str(file.uuid), file_categories)
            logger.info(f"File categories updated")

    # update the embedding file in vector space with the new metadata
    try:
        metadata = {
            "id": file.file_hash,
            "user": file.email,
            "tenant_id": file.tenant_id,
            "type": "uploaded_file",
            "upload_time": str(file.upload_timestamp),
            "categories": file.categories,
            "file_name": file.file_name
        }
        embedding_result = embeddings_client.embed_document(processed_content_text, metadata)
        if embedding_result:
            logger.info(f"Document embedded successfully")
            file_upload_repository.update_file_status(str(file.uuid), FileStatusEnum.COMPLETED)
        else:
            logger.error(f"Document embedding failed for tenant {file.tenant_id}")
    except Exception as e:
        file_upload_repository.update_file_status(str(file.uuid), FileStatusEnum.FAILED)
        logger.error(
            f"An error occurred during document embedding for tenant {file.tenant_id}: {e}"
        )
        return
    # update the file status to COMPLETED
    file_upload_repository.update_file_status(str(file.uuid), FileStatusEnum.COMPLETED)


async def fetch_doc_content(url):
    file_content = FileUploadService.read_blob_file(url)
    file_type = get_file_extension(get_file_name_from_url(url))
    if file_type == ".pdf":
        logger.info(f"Reading PDF file")
        text = extract_text_from_pdf(file_content)
    elif file_type == ".docx" or file_type == ".doc":
        logger.info(f"Reading DOC file")
        text = extract_text_from_docx(file_content)
    elif file_type == ".pptx" or file_type == ".ppt":
        logger.info(f"Reading PPT file")
        text = extract_text_from_pptx(file_content)
    else:
        raise ValueError("Unsupported file type")
    return text


# file_upload_repository.change_all_file_statuses(FileStatusEnum.UPLOADED)
# all_files = get_all_files_from_db()
# logger.info(f"Files to re-embed: {len(all_files)}")
# for file in all_files:
#     logger.info(f"Re-embedding file: {file.file_name}")
# for file in all_files:
#     asyncio.run(re_embed_file(file))