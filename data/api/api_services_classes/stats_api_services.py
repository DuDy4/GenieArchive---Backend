
import datetime
from data.data_common.dependencies.dependencies import stats_repository, tenants_repository
from ..api_services_classes.badges_api_services import BadgesApiService
from data.data_common.data_transfer_objects.stats_dto import StatsDTO
from common.utils.str_utils import get_uuid4
from common.genie_logger import GenieLogger
from ...data_common.data_transfer_objects.file_upload_dto import FileUploadDTO
from ...data_common.repositories.users_repository import UsersRepository

logger = GenieLogger()


class StatsApiService:
    def __init__(self):
        self.stats_repository = stats_repository()
        self.tenants_repository = tenants_repository()
        self.badges_api_service = BadgesApiService()
        self.users_repository = UsersRepository()

    def file_uploaded_event(self, file_upload_dto: FileUploadDTO):
        """
        Posts a file uploaded event to the database.
        """
        logger.info(f"Event details: {file_upload_dto}")
        if not file_upload_dto.tenant_id or not file_upload_dto.file_name:
            return
        email = self.tenants_repository.get_tenant_email(file_upload_dto.tenant_id)
        stats_data = {
            "email": email,
            "tenant_id": file_upload_dto.tenant_id,
            "user_id": file_upload_dto.user_id,
            "action": "UPLOAD",
            "entity": "FILE",
            "entity_id": str(file_upload_dto.uuid),
        }
        self.post_stats(stats_data)

    def file_category_uploaded_event(self, file_categories: list[str], user_id:str, tenant_id: str, email: str = None):
        """
        Posts a file category uploaded event to the database.
        """
        if not file_categories or not user_id:
            return
        email = email if email else self.tenants_repository.get_tenant_email(tenant_id)
        for file_category in file_categories:
            stats_data = {
                "email": email,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "action": "UPLOAD",
                "entity": "FILE_CATEGORY",
                "entity_id": file_category,
            }
            self.post_stats(stats_data)


    def view_meeting_event(self, user_id: str, meeting_id: str):
        """
        Posts a view meeting event to the database.
        """
        if not user_id or not meeting_id:
            return
        email = self.tenants_repository.get
        tenant_id = self.tenants_repository.get_tenant_id_by_email(email)
        # self.badges_api_service.handle_event(email, "VIEW_MEETING", meeting_id)
        stats_data = {
            "email": email,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "action": "VIEW",
            "entity": "MEETING",
            "entity_id": meeting_id,
        }
        self.post_stats(stats_data)


    def view_profile_event(self, user_id: str, profile_id: str, tenant_id: str = None):
        """
        Posts a view profile event to the database.
        """
        if not user_id or not profile_id:
            return
        email = self.users_repository.get_email_by_user_id(user_id)
        tenant_id = tenant_id if tenant_id else self.tenants_repository.get_tenant_id_by_email(email)
        stats_data = {
            "email": email,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "action": "VIEW",
            "entity": "PROFILE",
            "entity_id": profile_id,
        }
        self.post_stats(stats_data)

    def login_event(self, user_id: str, tenant_id: str = None):
        """
        Posts a view login event to the database.
        """
        if not user_id:
            logger.error("User ID not found in the login event")
            return
        email = self.users_repository.get_email_by_user_id(user_id)
        stats_data = {
            "email": email,
            "user_id": user_id,
            "tenant_id": tenant_id or self.tenants_repository.get_tenant_id_by_email(email),
            "action": "LOGIN",
            "entity": "USER",
            "entity_id": email
        }
        self.post_stats(stats_data)


    def post_stats(self, stats_data: dict):
        """
        Posts stats data to the database.
        """
        try:
            stats_data["uuid"] = get_uuid4()
            stats_data["timestamp"] = datetime.datetime.utcnow()
            stats_data["email"] = stats_data.get("email")
            stats_data["tenant_id"] = stats_data.get("tenant_id")
            stats_data["user_id"] = stats_data.get("user_id")
            stats_data["action"] = stats_data.get("action")
            stats_data["entity"] = stats_data.get("entity")
            stats_data["entity_id"] = stats_data.get("entity_id")
            stats_dto = StatsDTO(**stats_data)
            if self.stats_repository.should_log_event(stats_dto):
                self.stats_repository.insert(stats_dto)
                logger.info(f"Successfully posted stats data: [email={stats_data.get('email')}, action={stats_data.get('action')}, entity={stats_data.get('entity')}]")
                self.badges_api_service.handle_event(
                    stats_data.get("email"), 
                    stats_data.get("action"),
                    stats_data.get("entity"), 
                    stats_data.get("entity_id"))
        except Exception as e:
            logger.info(f"Error posting stats data: {e}")