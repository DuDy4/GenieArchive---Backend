import json
from data.data_common.utils.str_utils import get_uuid4


class MeetingDTO:
    def __init__(
        self,
        uuid,
        google_calendar_id,
        tenant_id,
        participants_emails,
        link,
        subject,
        start_time,
        end_time,
    ):
        self.uuid = uuid
        self.google_calendar_id = google_calendar_id
        self.tenant_id = tenant_id
        self.participants_emails = participants_emails
        self.link = link
        self.subject = subject
        self.start_time = start_time
        self.end_time = end_time

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "google_calendar_id": self.google_calendar_id,
            "tenant_id": self.tenant_id,
            "participants_emails": self.participants_emails,
            "link": self.link,
            "subject": self.subject,
            "start_time": self.start_time,
            "end_time": self.end_time,
        }

    @staticmethod
    def from_dict(data: dict):
        return MeetingDTO(
            uuid=data.get("uuid", ""),
            google_calendar_id=data.get("google_calendar_id", ""),
            tenant_id=data.get("tenant_id", ""),
            participants_emails=data.get("participants_emails", []),
            link=data.get("link", ""),
            subject=data.get("subject", ""),
            start_time=data.get("start_time", ""),
            end_time=data.get("end_time", ""),
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.google_calendar_id,
            self.tenant_id,
            self.participants_emails,
            self.link,
            self.subject,
            self.start_time,
            self.end_time,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "MeetingDTO":
        return MeetingDTO(
            uuid=row[0],
            google_calendar_id=row[1],
            tenant_id=row[2],
            participants_emails=row[3],
            link=row[4],
            subject=row[5],
            start_time=row[6],
            end_time=row[7],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return MeetingDTO.from_dict(data)

    @staticmethod
    def from_google_calendar_event(event, tenant_id):
        return MeetingDTO(
            uuid=event.get("uuid", get_uuid4()),
            google_calendar_id=event.get("id", ""),
            tenant_id=tenant_id,
            participants_emails=event.get("attendees", []),
            link=event.get("hangoutLink", ""),
            subject=event.get("summary", ""),
            start_time=event.get("start", ""),
            end_time=event.get("end", ""),
        )

    def __str__(self):
        return (
            f"MeetingDTO(uuid={self.uuid}, google_calendar_id={self.google_calendar_id}, tenant_id={self.tenant_id}, "
            f"participants_emails={self.participants_emails}, link={self.link}, "
            f"subject={self.subject}, start_time={self.start_time}, end_time={self.end_time})"
        )
