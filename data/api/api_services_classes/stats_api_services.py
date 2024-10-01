
import datetime
from data.data_common.dependencies.dependencies import stats_repository, tenants_repository
from data.data_common.data_transfer_objects.stats_dto import StatsDTO
from common.utils.str_utils import get_uuid4
from common.genie_logger import GenieLogger


logger = GenieLogger()


class StatsApiService:
    def __init__(self):
        self.stats_repository = stats_repository()
        self.tenants_repository = tenants_repository()


    def view_meeting_event(self, tenant_id: str, meeting_id: str):
        """
        Posts a view meeting event to the database.
        """
        if not tenant_id or not meeting_id:
            return
        email = self.tenants_repository.get_tenant_email(tenant_id)
        stats_data = {
            "email": email,
            "tenant_id": tenant_id,
            "action": "VIEW",
            "entity": "MEETING",
            "entity_id": meeting_id,
        }
        self.post_stats(stats_data)


    def view_profile_event(self, tenant_id: str, profile_id: str):        
        """
        Posts a view profile event to the database.
        """
        if not tenant_id or not profile_id:
            return
        email = self.tenants_repository.get_tenant_email(tenant_id)
        stats_data = {
            "email": email,
            "tenant_id": tenant_id,
            "action": "VIEW",
            "entity": "PROFILE",
            "entity_id": profile_id,
        }
        self.post_stats(stats_data)

    def login_event(self, tenant_id: str):
        """
        Posts a view login event to the database.
        """
        if not tenant_id:
            return
        email = self.tenants_repository.get_tenant_email(tenant_id)
        stats_data = {
            "email": email,
            "tenant_id": tenant_id,
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
            stats_data["action"] = stats_data.get("action")
            stats_data["entity"] = stats_data.get("entity")
            stats_data["entity_id"] = stats_data.get("entity_id")
            stats_dto = StatsDTO(**stats_data)
            if self.stats_repository.should_log_event(stats_dto):
                self.stats_repository.insert(stats_dto)
                logger.info(f"Successfully posted stats data: [email={stats_data.get('email')}, action={stats_data.get('action')}, entity={stats_data.get('entity')}]")
        except Exception as e:
            logger.info(f"Error posting stats data: {e}")