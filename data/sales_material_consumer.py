import asyncio
import os
import sys
import json
from datetime import datetime, timedelta, timezone

from common.utils import env_utils
from data.data_common.data_transfer_objects.file_upload_dto import FileUploadDTO
from data.data_common.events.genie_event import GenieEvent

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.topics import Topic
from common.utils.file_utils import (
    extract_text_from_docx,
    extract_text_from_pdf,
    extract_text_from_pptx,
    get_file_extension,
    get_file_name_from_url,
)
from data.internal_services.files_upload_service import FileUploadService
from data.data_common.dependencies.dependencies import file_upload_repository

from data.api_services.embeddings import GenieEmbeddingsClient
from dotenv import load_dotenv
from common.genie_logger import GenieLogger

load_dotenv()
logger = GenieLogger()

CONSUMER_GROUP = "sales_material_consumer_group"

# Load sleep time for checking the last file from environment variables
SLEEP_TIME_FOR_LAST_FILE_CHECK = int(env_utils.get("SLEEP_TIME_FOR_LAST_FILE_CHECK", "15"))


class SalesMaterialConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(topics=[Topic.FILE_UPLOADED], consumer_group=CONSUMER_GROUP)
        self.embeddings_client = GenieEmbeddingsClient()
        self.file_upload_repository = file_upload_repository()
        # Dictionary to track successful embeddings for each tenant
        self.embedding_success_by_tenant = {}

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.FILE_UPLOADED:
                logger.info("Handling file upload event")
                return await self.embed_and_store_content(event)
            case _:
                logger.error(f"Unexpected topic: {topic}, consumer_group: {CONSUMER_GROUP}")

    async def embed_and_store_content(self, event):
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        event_data = event_body["event_data"]
        blob_name = event_data["blobUrl"]
        text = await self.fetch_doc_content(blob_name)
        if not text:
            logger.error(f"Could not fetch document content from {blob_name}")
            return

        logger.info(f"Processing file url: {blob_name}")
        file_name = get_file_name_from_url(blob_name)
        logger.info(f"Processing file with name: {file_name}")
        file_id = event_data.get("file_id")
        file_uploaded = event_body.get("file_uploaded")
        file_upload_dto = FileUploadDTO.from_dict(file_uploaded)
        if not file_upload_dto:
            logger.error(f"File upload DTO not found in the event data")
            return {"status": "error", "message": "File upload DTO not found in the event data"}

        logger.info(f"File upload DTO: {file_upload_dto}")
        file_uploaded_in_db = self.file_upload_repository.exists(file_upload_dto.file_hash)
        if file_uploaded_in_db:
            logger.info(f"File already exists in the database")
            return

        file_upload_dto.update_file_content(text)
        logger.info(f"File content updated in the DTO: {file_upload_dto.file_hash}")
        if self.file_upload_repository.exists(file_upload_dto.file_hash):
            logger.info(f"File already exists in the database")
            self.file_upload_repository.delete(file_upload_dto.uuid)
            return {"status": "error", "message": "File already exists in the database"}
        self.file_upload_repository.update_file_hash(file_upload_dto)
        logger.info(f"File uploaded in the database")

        # Try to embed the document content
        try:
            metadata = {
                "id": file_id,
                "user": file_upload_dto.email,
                "tenant_id": file_upload_dto.tenant_id,
                "type": "uploaded_file",
                "upload_time": file_upload_dto.upload_timestamp,
            }
            embedding_result = self.embeddings_client.embed_document(text, metadata)
            if embedding_result:
                logger.info(f"Document embedded successfully")
                self.embedding_success_by_tenant[file_upload_dto.tenant_id] = True
            else:
                logger.error(f"Document embedding failed for tenant {file_upload_dto.tenant_id}")
        except Exception as e:
            logger.error(
                f"An error occurred during document embedding for tenant {file_upload_dto.tenant_id}: {e}"
            )
            return

        # Check if this file is the last file uploaded for the tenant
        is_last_file = await self.check_last_file(file_upload_dto)
        if is_last_file:
            # If it's the last file, send an event if any file was successfully embedded for the tenant
            if self.embedding_success_by_tenant.get(file_upload_dto.tenant_id, False):
                event = GenieEvent(Topic.NEW_EMBEDDED_DOCUMENT, {"tenant_id": file_upload_dto.tenant_id})
                event.send()
                logger.info(
                    f"Triggered NEW_EMBEDDED_DOCUMENT event for tenant {file_upload_dto.tenant_id} after processing all files."
                )
                # Remove tenant from tracking after the event is sent
                self.embedding_success_by_tenant[file_upload_dto.tenant_id] = False
                return {"status": "success"}
            else:
                logger.warning(
                    f"No files were successfully embedded for tenant {file_upload_dto.tenant_id}. Skipping event trigger."
                )
                # Remove tenant from tracking even if no embedding was successful
                self.embedding_success_by_tenant[file_upload_dto.tenant_id] = False
                return {"status": "No successful embeddings"}
        else:
            logger.info(f"Document embedded successfully but not the last file. Waiting for more files.")
            return {"status": "Not the last file"}

    async def fetch_doc_content(self, url):
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

    async def check_last_file(self, file_upload_dto):
        await asyncio.sleep(SLEEP_TIME_FOR_LAST_FILE_CHECK)
        return self.file_upload_repository.is_last_file_added(
            file_upload_dto.tenant_id, file_upload_dto.upload_time_epoch
        )


if __name__ == "__main__":
    sales_material_consumer = SalesMaterialConsumer()
    try:
        asyncio.run(sales_material_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
