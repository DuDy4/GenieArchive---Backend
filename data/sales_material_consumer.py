import asyncio
import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.topics import Topic
from common.utils.file_utils import  (
    extract_text_from_docx, 
    extract_text_from_pdf, 
    extract_text_from_pptx, 
    get_file_extension,
    get_file_name_from_url
)
from data.internal_services.files_upload_service import FileUploadService

from data.api_services.embeddings import EmbeddingsClient
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
load_dotenv()
logger = GenieLogger()

CONSUMER_GROUP = "sales_material_consumer_group"


class SalesMaterialConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.FILE_UPLOADED
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.embeddings_client = EmbeddingsClient()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.FILE_UPLOADED:
                logger.info("Handling file upload event")
                await self.embed_and_store_content(event)
            case _:
                logger.error(f"Should not have reached here: {topic}, consumer_group: {CONSUMER_GROUP}")

    async def embed_and_store_content(self, event):
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        event_data = event_body['event_data']
        metadata = event_body['metadata']
        blob_name = event_data['blobUrl']
        text = await self.fetch_doc_content(blob_name)
        if not text:
            logger.error(f"Could not fetch doc content {blob_name}")
            return
        
        self.embeddings_client.embed_document(text, metadata)


    async def fetch_doc_content(self, url):
        file_content = FileUploadService.read_blob_file(url)
        file_type = get_file_extension(get_file_name_from_url(url))
        if file_type == ".pdf":
            text = extract_text_from_pdf(file_content)
        elif file_type == ".docx" or file_type == ".doc":
            text = extract_text_from_docx(file_content)
        elif file_type == ".pptx" or file_type == ".ppt":
            text = extract_text_from_pptx(file_content)
        else:
            raise ValueError("Unsupported file type")
        return text

if __name__ == "__main__":
    sales_material_consumer = SalesMaterialConsumer()
    try:
        asyncio.run(sales_material_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
