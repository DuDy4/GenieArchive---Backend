import hashlib
import json
from data.data_common.utils.str_utils import get_uuid4
from pydantic import BaseModel


class MeetingDTO:
    def __init__(
        self,
        uuid,
        google_calendar_id,
        tenant_id,
        participants_emails,
        participants_hash,
        link,
        subject,
        start_time,
        end_time,
    ):
        self.uuid = uuid
        self.google_calendar_id = google_calendar_id
        self.tenant_id = tenant_id
        self.participants_emails = participants_emails
        self.participants_hash = (
            participants_hash
            if participants_hash
            else hash_participants(participants_emails)
        )
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
            "participants_hash": self.participants_hash,
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
            participants_hash=data.get(
                "participants_hash",
                hash_participants(data.get("participants_emails", [])),
            ),
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
            self.participants_hash,
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
            participants_hash=row[4],
            link=row[5],
            subject=row[6],
            start_time=row[7],
            end_time=row[8],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        if not data.get("participants_hash"):
            data["participants_hash"] = hash_participants(
                data.get("participants_emails", [])
            )
        return MeetingDTO.from_dict(data)

    @staticmethod
    def from_google_calendar_event(event, tenant_id):
        return MeetingDTO(
            uuid=event.get("uuid", get_uuid4()),
            google_calendar_id=event.get("id", ""),
            tenant_id=tenant_id,
            participants_emails=event.get("attendees", []),
            participants_hash=event.get(
                "participants_hash", hash_participants(event.get("attendees", []))
            ),
            link=event.get("hangoutLink", ""),
            subject=event.get("summary", ""),
            start_time=event.get("start", "").get("dateTime", "")
            or event.get("start", "").get("date", ""),
            end_time=event.get("end", "").get("dateTime", "")
            or event.get("end", "").get("date", ""),
        )

    def __str__(self):
        return (
            f"MeetingDTO(uuid={self.uuid}, google_calendar_id={self.google_calendar_id}, tenant_id={self.tenant_id}, "
            f"participants_emails={self.participants_emails}, link={self.link}, "
            f"subject={self.subject}, start_time={self.start_time}, end_time={self.end_time})"
        )


def hash_participants(participants_emails: list[str]) -> str:
    emails_string = json.dumps(participants_emails, sort_keys=True)
    return hashlib.sha256(emails_string.encode("utf-8")).hexdigest()
