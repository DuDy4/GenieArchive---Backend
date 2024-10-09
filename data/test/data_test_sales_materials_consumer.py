import asyncio
from datetime import datetime
import json

# Import the SalesMaterialConsumer class from your consumer module
from data.api.api_services_classes.user_materials_services import UserMaterialServices

# Create an instance of the consumer
service = UserMaterialServices()

# Define the mock event data using the provided sample data
mock_events = [
    {
        "topic": "/subscriptions/35eebc2d-2f7f-419f-8b4f-3ae8cb1ad9ee/resourceGroups/first-resource-group/providers/Microsoft.Storage/storageAccounts/useruploadedmaterials",
        "subject": "/blobServices/default/containers/user-uploaded-materials/blobs/dan.shevel@genieai.ai/uploads/genie-blog-2.pdf",
        "eventType": "Microsoft.Storage.BlobCreated",
        "id": "0e5f0b09-401e-00e1-6761-19393f06380c",
        "data": {
            "api": "PutBlob",
            "requestId": "0e5f0b09-401e-00e1-6761-19393f000000",
            "eTag": "0x8DCE77834AD88FC",
            "contentType": "application/x-www-form-urlencoded",
            "contentLength": 479978,
            "blobType": "BlockBlob",
            "accessTier": "Default",
            "blobUrl": "https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/dan.shevel@genieai.ai/uploads/genie-blog-2.pdf",
            "url": "https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/dan.shevel@genieai.ai/uploads/genie-blog-2.pdf",
            "sequencer": "0000000000000000000000000003130500000000000005e0",
            "identity": "$superuser",
            "storageDiagnostics": {"batchId": "dbadc204-6006-00ab-0061-199ab0000000"},
        },
        "eventTime": "2024-10-08T09:04:24.5628775Z",
    },
    {
        "topic": "/subscriptions/35eebc2d-2f7f-419f-8b4f-3ae8cb1ad9ee/resourceGroups/first-resource-group/providers/Microsoft.Storage/storageAccounts/useruploadedmaterials",
        "subject": "/blobServices/default/containers/user-uploaded-materials/blobs/dan.shevel@genieai.ai/uploads/genie-blog-1.pdf",
        "eventType": "Microsoft.Storage.BlobCreated",
        "id": "0e5efbf6-401e-00e1-7561-19393f06915a",
        "data": {
            "api": "PutBlob",
            "requestId": "0e5efbf6-401e-00e1-7561-19393f000000",
            "eTag": "0x8DCE7782E564A80",
            "contentType": "application/x-www-form-urlencoded",
            "contentLength": 79605,
            "blobType": "BlockBlob",
            "accessTier": "Default",
            "blobUrl": "https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/dan.shevel@genieai.ai/uploads/genie-blog-1.pdf",
            "url": "https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/dan.shevel@genieai.ai/uploads/genie-blog-1.pdf",
            "sequencer": "0000000000000000000000000003130500000000000005d2",
            "identity": "$superuser",
            "storageDiagnostics": {"batchId": "dbada244-6006-00ab-0061-199ab0000000"},
        },
        "eventTime": "2024-10-08T09:04:13.925662Z",
    },
    {
        "topic": "/subscriptions/35eebc2d-2f7f-419f-8b4f-3ae8cb1ad9ee/resourceGroups/first-resource-group/providers/Microsoft.Storage/storageAccounts/useruploadedmaterials",
        "subject": "/blobServices/default/containers/user-uploaded-materials/blobs/dan.shevel@genieai.ai/uploads/genie-website.pdf",
        "eventType": "Microsoft.Storage.BlobCreated",
        "id": "0e5ef4c6-401e-00e1-0261-19393f064723",
        "data": {
            "api": "PutBlob",
            "requestId": "0e5ef4c6-401e-00e1-0261-19393f000000",
            "eTag": "0x8DCE7782B6A3138",
            "contentType": "application/x-www-form-urlencoded",
            "contentLength": 1074910,
            "blobType": "BlockBlob",
            "accessTier": "Default",
            "blobUrl": "https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/dan.shevel@genieai.ai/uploads/genie-website.pdf",
            "url": "https://useruploadedmaterials.blob.core.windows.net/user-uploaded-materials/dan.shevel@genieai.ai/uploads/genie-website.pdf",
            "sequencer": "0000000000000000000000000003130500000000000005cc",
            "identity": "$superuser",
            "storageDiagnostics": {"batchId": "dbad93dc-6006-00ab-0061-199ab0000000"},
        },
        "eventTime": "2024-10-08T09:04:09.0222262Z",
    },
]


async def test_consumer():
    # Convert mock events to GenieEvents and process them with the consumer
    for event in mock_events:
        service.file_uploaded([event])
        await asyncio.sleep(10)


# Run the test asynchronously
if __name__ == "__main__":
    asyncio.run(test_consumer())
