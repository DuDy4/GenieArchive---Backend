from datetime import datetime
import json
import sys
import os

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger
from data.data_common.data_transfer_objects.file_upload_dto import FileUploadDTO
from data.data_common.repositories.file_upload_repository import FileUploadRepository

logger = GenieLogger()

def test_uploaded_file():
    file_upload_repository = FileUploadRepository()
    file_data = {'api': 'PutBlob', 'requestId': 'e3d97c0b-a01e-00e9-3602-232330000000', 'eTag': '0x8DCF11A0C955244', 'contentType': 'application/x-www-form-urlencoded', 'contentLength': 16585105, 'blobType': 'BlockBlob', 'accessTier': 'Default', 'blobUrl': 'https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/asaf@genieai.ai/uploads/Pitch Deck - Genie AI  .pdf', 'url': 'https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/asaf@genieai.ai/uploads/Pitch Deck - Genie AI  .pdf', 'sequencer': '00000000000000000000000000031383000000000060e007', 'identity': '$superuser', 'storageDiagnostics': {'batchId': '0ffe6d37-8006-001a-0002-2384a5000000'}}
    file_upload_dto = {'uuid': 'c0a47b1d-cb5c-462f-842e-2de3193d39ef', 'file_name': 'Pitch Deck - Genie AI  .pdf', 'file_hash': None, 'upload_timestamp': '2024-10-20T15:15:36.233456+00:00', 'upload_time_epoch': 1729437336, 'email': 'asaf@genieai.ai', 'tenant_id': 'org_RPLWQRTI8t7EWU1L', 'status': 'UPLOADED', 'categories': []}
    file_id = 'e3d97c0b-a01e-00e9-3602-232330064dc8'

    file_upload = FileUploadDTO.from_dict(file_upload_dto)
    file_upload.upload_timestamp = datetime.today()
    file_upload.upload_time_epoch = int(datetime.today().timestamp())
    if file_upload_repository.exists_metadata(file_upload):
        logger.info(f"File already exists in the database")
    else:
        file_upload_repository.insert(file_upload)

    test_data = {"event_data": file_data, "file_uploaded": file_upload.to_dict(), "file_id": file_id}

    event = GenieEvent(topic=Topic.FILE_UPLOADED, data=test_data)
    event.send()


# test_uploaded_file()