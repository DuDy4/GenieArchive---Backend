from common.utils import email_utils
from data.data_common.dependencies.dependencies import (
    tenants_repository,
    google_creds_repository,
    persons_repository,
    ownerships_repository,
    meetings_repository,
    profiles_repository,
)
from common.genie_logger import GenieLogger
from data.internal_services.files_upload_service import FileUploadService
from fastapi import HTTPException
from data.data_common.utils.str_utils import (
    upload_file_name_validation,
    ALLOWED_EXTENSIONS,
    MAX_FILE_NAME_LENGTH,
)
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

logger = GenieLogger()


class UserMaterialServices:
    def __init__(self):
        self.tenants_repository = tenants_repository()

    def file_uploaded(self, uploaded_files):
        logger.info(f"Event details: {uploaded_files}")
        for file in uploaded_files:
            file_data = file["data"]
            if not file_data:
                logger.error(f"Data not found in azure event {uploaded_files}")
                continue
            logger.info(f"Event data: {file_data}")
            user_email = email_utils.extract_email_from_url(file_data["blobUrl"])
            if not user_email:
                logger.error(f"User email is not part of the blob")
                continue
            tenant_id = self.tenants_repository.get_tenant_id_by_email(user_email)
            if not tenant_id:
                logger.error(f"Tenant ID not found the email: {user_email}")
                continue
            upload_time = file.get("eventTime")
            if not upload_time:
                logger.error(f"Upload time not found in the event data")
            logger.set_tenant_id(tenant_id)
            metadata = {
                "id": file["id"],
                "user": user_email,
                "tenant_id": tenant_id,
                "type": "uploaded_file",
                "upload_time": upload_time,
            }
            GenieEvent(Topic.FILE_UPLOADED, {"event_data": file_data, "metadata": metadata}, "public").send()

    def generate_upload_url(self, tenant_id, file_name):
        if not upload_file_name_validation(file_name):
            raise HTTPException(
                status_code=400,
                detail=f"File name not supported. Must be: 1. Less than {MAX_FILE_NAME_LENGTH} characters 2. No special characters. 3. Only extensions supported: {ALLOWED_EXTENSIONS}",
            )

        upload_url = FileUploadService.generate_upload_url(tenant_id, file_name)
        logger.info(f"Succesfullly create update url: {upload_url}")
        if upload_url:
            return upload_url
        else:
            raise HTTPException(status_code=500, detail="Failed to generate upload url")
