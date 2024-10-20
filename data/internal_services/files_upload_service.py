from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from dotenv import load_dotenv
from datetime import datetime, timedelta
from common.genie_logger import GenieLogger
from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.dependencies.dependencies import (
    tenants_repository,
)
from common.utils import env_utils

load_dotenv()
logger = GenieLogger()

CONNECTION_STRING = env_utils.get("AZURE_UPLOAD_STORAGE_CONNECTION_STRING")
ACCOUNT_KEY = env_utils.get("AZURE_UPLOAD_STORAGE_ACCOUNT_KEY")
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

UPLOADED_MATERIALS_CONTINAER_NAME = 'user-uploaded-materials'


class FileUploadService:
    tenants_repository: TenantsRepository = tenants_repository()

    @staticmethod
    def generate_upload_url(tenant_id: str, file_name: str):
        user_id = FileUploadService.tenants_repository.get_tenant_email(tenant_id)
        try:
            if not user_id:
                logger.error(f"Could not find user with tenant ID: {tenant_id}")
                return None
            blob_full_name = f"{user_id}/uploads/{file_name}"        
            if not FileUploadService.does_folder_exist(UPLOADED_MATERIALS_CONTINAER_NAME, user_id):
                FileUploadService.create_user_folder(UPLOADED_MATERIALS_CONTINAER_NAME, user_id)
            upload_url = FileUploadService.generate_azure_upload_url(UPLOADED_MATERIALS_CONTINAER_NAME, blob_full_name, user_id, file_name)            
            return upload_url
        except Exception as e:
            logger.error(f"Could not generate SAS token for user: {user_id}. Exception details: {e}")
            return None
        
    
    def does_folder_exist(container_name: str, user_id: str):
        container_client = blob_service_client.get_container_client(container_name)
        
        # List blobs with the prefix (e.g., "customerA/")
        blob_list = container_client.list_blobs(name_starts_with=f"{user_id}/uploads/")
        
        # If any blobs exist under the prefix, return True
        for _ in blob_list:
            return True
        
        # If no blobs are found, return False
        return False

    
    def create_user_folder(container_name: str, user_id: str):
        container_client = blob_service_client.get_container_client(container_name)
        # Create a placeholder file (empty.txt) in the user's folder
        blob_client = container_client.get_blob_client(f"{user_id}/uploads/placeholder.txt")
        
        # Upload an empty blob as a placeholder
        blob_client.upload_blob(b"", overwrite=True)

    
    def generate_azure_sas_token(container_name, blob_name):
        sas_token = generate_blob_sas(
            account_name="useruploadedmaterials",
            container_name=container_name,
            blob_name=blob_name,
            account_key=ACCOUNT_KEY,  # Retrieve from Azure Portal -> Access keys
            permission=BlobSasPermissions(write=True),
            expiry=datetime.utcnow() + timedelta(hours=1)  # Set the token to expire in 1 hour
        )
        return sas_token
    
    
    def generate_azure_upload_url(container_name, blob_name, user_id, file_name):
        sas_token  = FileUploadService.generate_azure_sas_token(container_name, blob_name)
        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{UPLOADED_MATERIALS_CONTINAER_NAME}/{blob_name}?{sas_token}"
        return blob_url
    

    def read_blob_file(blob_name: str):
        blob_short_name = blob_name.split(UPLOADED_MATERIALS_CONTINAER_NAME + '/')[1]
        logger.info(f"Trying to fetch azure blob - {blob_name}")
        blob_client = blob_service_client.get_blob_client(container=UPLOADED_MATERIALS_CONTINAER_NAME, blob=blob_short_name)
        download_stream = blob_client.download_blob()
        file_contents = download_stream.readall()  
        return file_contents
    
