from data.data_common.dependencies.dependencies import meetings_repository
from fastapi import HTTPException
from common.genie_logger import GenieLogger

logger = GenieLogger()


class MeetingsApiService:
    def __init__(self):
        self.meetings_repository = meetings_repository()

    def get_all_meetings(self, tenant_id):
        if not tenant_id:
            logger.error("Tenant ID not provided")
            return {"error": "Tenant ID not provided"}

        meetings = self.meetings_repository.get_all_meetings_by_tenant_id(tenant_id)
        dict_meetings = [meeting.to_dict() for meeting in meetings]
        # sort by meeting.start_time
        dict_meetings.sort(key=lambda x: x["start_time"])
        logger.info(f"About to sent to {tenant_id} meetings: {len(dict_meetings)}")
        return dict_meetings

    # Additional meeting-related logic
