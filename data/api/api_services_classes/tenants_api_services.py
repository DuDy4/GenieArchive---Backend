from common.utils import env_utils
from common.utils.str_utils import get_uuid4
from data.data_common.dependencies.dependencies import tenants_repository, google_creds_repository
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger
from fastapi import HTTPException
import datetime

from data.internal_services.tenant_service import TenantService

logger = GenieLogger()


class TenantsApiService:
    def __init__(self):
        self.tenants_repository = tenants_repository()
        self.google_creds_repository = google_creds_repository()
        self.google_client_id = env_utils.get("GOOGLE_CLIENT_ID")
        self.google_client_secret = env_utils.get("GOOGLE_CLIENT_SECRET")
        self.google_token_uri = "https://oauth2.googleapis.com/token"

    async def post_successful_login(self, auth_data: dict):
        """
        Returns a tenant ID.
        """
        logger.info(f"Received auth data: {auth_data}")
        auth_claims = auth_data["data"]["claims"]
        user_email = auth_claims.get("email")
        user_tenant_id = auth_claims.get("tenantId")
        user_name = auth_claims.get("userId")
        logger.info(f"Fetching google meetings for user email: {user_email}, tenant ID: {user_tenant_id}")
        tenant_data = {
            "tenantId": user_tenant_id,
            "name": user_name,
            "email": user_email,
            "user_id": user_name,
        }
        self.tenants_repository.insert(tenant_data)
        self.fetch_google_meetings(user_email)
        response = {
            "claims": {
                "tenantId": user_tenant_id,
            },
        }
        return response

    def fetch_google_meetings(self, user_email):
        logger.info(f"Received Google meetings request for tenant: {user_email}")

        google_credentials = self.google_creds_repository.get_creds(user_email)
        if not google_credentials:
            logger.error("Google credentials not found for the tenant")
            return {"error": "Google credentials not found"}

        last_fetch_meetings = google_credentials.get("last_fetch_meetings")
        if last_fetch_meetings:
            # If last_fetch_meetings is already a datetime object, no need to parse it
            if isinstance(last_fetch_meetings, datetime.datetime):
                time_diff = datetime.datetime.now() - last_fetch_meetings
            else:
                # Otherwise, convert it from a string (if needed)
                last_fetch_meetings = datetime.datetime.strptime(last_fetch_meetings, "%Y-%m-%d %H:%M:%S.%f")
                time_diff = datetime.datetime.now() - last_fetch_meetings

            if time_diff.total_seconds() < 30:
                logger.info("Meetings already fetched in the last minute. Skipping.")
                return {"message": "Meetings already fetched in the last hour. Skipping..."}
        else:
            logger.warning("Missing last_fetch_meetings. Skipping check.")

        # Construct the Credentials object
        credentials = Credentials(
            token=google_credentials.get("access_token"),
            refresh_token=google_credentials.get("refresh_token"),
            client_id=self.google_client_id,
            client_secret=self.google_client_secret,
            token_uri=self.google_token_uri,
        )

        # Log credentials before using them
        logger.debug(f"Google credentials before refresh: {credentials}")

        # Build the service using the credentials
        service = build("calendar", "v3", credentials=credentials)

        now = datetime.datetime.utcnow().isoformat() + "Z"
        now_minus_10_hours = (datetime.datetime.utcnow() - datetime.timedelta(hours=10)).isoformat() + "Z"
        logger.info(f"[EMAIL={user_email}] Fetching meetings starting from: {now_minus_10_hours}")

        try:
            events_result = (
                service.events()
                .list(
                    calendarId="primary",
                    timeMin=now_minus_10_hours,
                    maxResults=30,
                    singleEvents=True,
                    orderBy="startTime",
                )
                .execute()
            )
        except Exception as e:
            error_message = str(e)
            if "invalid_grant" in error_message:
                logger.error(f"Invalid grant error: {e}. The user may need to re-authenticate.")
                raise HTTPException(
                    status_code=401,
                    detail="Invalid grant: Refresh token is no longer valid. Please re-authenticate.",
                )
            else:
                logger.error(f"Error fetching events from Google Calendar: {e}")
                raise HTTPException(
                    status_code=500, detail=f"Error fetching events from Google Calendar: {e}"
                )

        meetings = events_result.get("items", [])
        logger.info(f"Fetched events: {meetings}")

        if not meetings:
            self.google_creds_repository.update_last_fetch_meetings(user_email)
            return {"message": "No upcoming events found."}
        tenant_id = self.tenants_repository.get_tenant_id_by_email(user_email)
        data_to_send = {"tenant_id": tenant_id, "meetings": meetings}
        event = GenieEvent(topic=Topic.NEW_MEETINGS_TO_PROCESS, data=data_to_send)
        event.send()
        self.google_creds_repository.update_last_fetch_meetings(user_email)
        logger.info(f"Sent {len(meetings)} meetings to the processing queue")

        return {"status": "success", "message": f"Sent {len(meetings)} meetings to the processing queue"}

    def create_tenant(self, tenant_data):
        tenant_id = tenant_data.get("tenantId")
        if not tenant_id:
            raise HTTPException(status_code=400, detail="Missing tenant ID")
        self.tenants_repository.insert(tenant_data)

    # Other tenant-related logic here...
    def login_event(self, user_info):
        try:
            user_name = user_info.get("name")
            user_id = user_info.get("user_id")
            user_email = user_info.get("user_email")
            user_access_token = user_info.get("google_access_token")
            user_refresh_token = user_info.get("google_refresh_token")
            tenant_id = user_info.get("tenant_id")
            tenant_name = user_info.get("tenant_name")

            if not user_email or not tenant_id:
                raise HTTPException(status_code=400, detail="Missing user email or tenant ID")

            if self.tenants_repository.exists(tenant_id):
                logger.debug(f"Tenant ID {tenant_id} already exists in database")
                self.google_creds_repository.save_creds(user_email, user_access_token, user_refresh_token)
                logger.debug(f"Updated google creds for user: {user_email}. About to fetch google meetings")
                self.fetch_google_meetings(user_email)
            elif self.tenants_repository.email_exists(user_email):
                logger.debug(
                    f"Another tenant ID exists for this email: {user_email}. About to update tenant ID"
                )
                old_tenant_id = self.tenants_repository.get_tenant_id_by_email(user_email)
                TenantService.changed_old_tenant_to_new_tenant(
                    new_tenant_id=tenant_id, old_tenant_id=old_tenant_id, user_id=user_id, user_name=user_name
                )
                self.google_creds_repository.save_creds(user_email, user_access_token, user_refresh_token)
            else:
                # Signup new user
                logger.debug(f"About to signup new user: {user_email}")
                self.tenants_repository.insert(
                    {
                        "uuid": get_uuid4(),
                        "tenantId": tenant_id,
                        "name": tenant_name,
                        "email": user_email,
                        "user_id": user_id,
                    }
                )
                self.google_creds_repository.save_creds(user_email, user_access_token, user_refresh_token)

        except Exception as e:
            logger.error(f"Error during login event: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
