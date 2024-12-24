from datetime import datetime
from os.path import exists

from common.utils import email_utils
from data.data_common.data_transfer_objects.file_upload_dto import FileUploadDTO, FileCategoryEnum
from data.data_common.dependencies.dependencies import (
    tenants_repository,
    file_upload_repository,
)
from common.utils.file_utils import get_file_name_from_url
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
        self.file_upload_repository = file_upload_repository()

    def file_uploaded(self, uploaded_files):
        logger.info(f"Event details: {uploaded_files}")
        files_list = []
        for file in uploaded_files:
            file_data = file["data"]
            if not file_data:
                logger.error(f"Data not found in azure event {uploaded_files}")
                continue
            logger.info(f"Event data: {file_data}")
            file_url = file_data.get("blobUrl")
            if not file_url:
                logger.error(f"Blob URL not found in the event data")
                continue
            file_name = get_file_name_from_url(file_url)
            if not file_name:
                logger.error(f"File name not found in the blob URL")
                continue
            if file_name == "placeholder.txt":
                logger.info(f"Placeholder file uploaded. Skipping")
                continue
            user_email = email_utils.extract_email_from_url(file_data["blobUrl"])
            if not user_email:
                logger.error(f"User email is not part of the blob")
                continue
            tenant_id = self.tenants_repository.get_tenant_id_by_email(user_email)
            if not tenant_id:
                logger.error(f"Tenant ID not found the email: {user_email}")
                continue
            upload_time_str = file.get("eventTime")
            if not upload_time_str:
                logger.error(f"Upload time not found in the event data")
                continue
            file_id = file.get("id")
            if not file_id:
                logger.error(f"File ID not found in the event data")
                continue
            try:
                upload_time = datetime.fromisoformat(upload_time_str.replace("Z", "+00:00"))
                upload_time_epoch = int(upload_time.timestamp())
                logger.info(f"Upload time: {upload_time}, epoch time: {upload_time_epoch}")
            except Exception as e:
                logger.error(f"Failed to parse upload time: {upload_time_str}. Error: {e}")
                continue

            file_upload_dto = FileUploadDTO.from_file(
                file_name=file_name,
                file_content=None,
                email=user_email,
                tenant_id=tenant_id,
                upload_time=upload_time,
            )

            if self.file_upload_repository.exists_metadata(file_upload_dto):
                logger.info(f"File already exists in the database")
                continue
            if file.file_name == "placeholder.txt":
                logger.info(f"Placeholder file uploaded. Skipping")
                continue
            self.file_upload_repository.insert(file_upload_dto)

            logger.info(f"File upload DTO: {file_upload_dto}")

            logger.set_tenant_id(tenant_id)

            GenieEvent(
                Topic.FILE_UPLOADED,
                {"event_data": file_data, "file_uploaded": file_upload_dto.to_dict(), "file_id": file_id},
            ).send()
            files_list.append(file_upload_dto)
        return files_list

    def get_all_files(self, tenant_id):
        all_files = self.file_upload_repository.get_all_files_by_tenant_id(tenant_id)
        all_categories = FileCategoryEnum.get_all_categories()
        all_files_jsoned = [file.to_dict() for file in all_files] if all_files else []
        return {"files": all_files_jsoned, "categories": all_categories}

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
