import hashlib
import json
import re
from typing import List, Dict, Any, Optional

from data.data_common.utils.str_utils import get_uuid4
from pydantic import BaseModel, field_validator
from common.genie_logger import GenieLogger

logger = GenieLogger()


class Guidelines(BaseModel):
    timing: str
    reasoning: str
    execution: str
    phrases: List[str]

    @field_validator("timing", "reasoning", "execution", "phrases")
    def not_empty(cls, value):
        if not value:
            raise ValueError("Field cannot be empty")
        return value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timing": self.timing,
            "reasoning": self.reasoning,
            "execution": self.execution,
            "phrases": self.phrases,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "Guidelines":
        return cls(
            timing=data.get("timing"),
            reasoning=data.get("reasoning"),
            execution=data.get("execution"),
            phrases=data.get("phrases"),
        )


class AgendaItem(BaseModel):
    goal: str
    guidelines: Guidelines

    @field_validator("goal", "guidelines")
    def not_empty(cls, value):
        if not value:
            raise ValueError("Field cannot be empty")
        return value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "goal": self.goal,
            "guidelines": self.guidelines.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "AgendaItem":
        return cls(
            goal=data.get("goal"),
            guidelines=Guidelines.from_dict(data.get("guidelines")),
        )


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
        location,
        start_time,
        end_time,
        agenda: List[AgendaItem] = None,
    ):
        self.uuid = uuid
        self.google_calendar_id = google_calendar_id
        self.tenant_id = tenant_id
        self.participants_emails = participants_emails
        self.participants_hash = (
            participants_hash if participants_hash else hash_participants(participants_emails)
        )
        self.link = link
        self.subject = subject
        self.location = location
        self.start_time = start_time
        self.end_time = end_time
        self.agenda = agenda

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "google_calendar_id": self.google_calendar_id,
            "tenant_id": self.tenant_id,
            "participants_emails": self.participants_emails,
            "participants_hash": self.participants_hash,
            "link": self.link,
            "subject": self.subject,
            "location": self.location,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "agenda": [agenda.to_dict() for agenda in self.agenda] if self.agenda else None,
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
            location=data.get("location", ""),
            start_time=data.get("start_time", ""),
            end_time=data.get("end_time", ""),
            agenda=[AgendaItem.from_dict(agenda) for agenda in data.get("agenda")],
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
            self.location,
            self.start_time,
            self.end_time,
            self.agenda,
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
            location=row[7],
            start_time=row[8],
            end_time=row[9],
            agenda=row[10],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        if not data.get("participants_hash"):
            data["participants_hash"] = hash_participants(data.get("participants_emails", []))
        return MeetingDTO.from_dict(data)

    @staticmethod
    def from_google_calendar_event(event, tenant_id):
        return MeetingDTO(
            uuid=event.get("uuid", get_uuid4()),
            google_calendar_id=event.get("id", ""),
            tenant_id=tenant_id,
            participants_emails=event.get("attendees", []),
            participants_hash=event.get("participants_hash", hash_participants(event.get("attendees", []))),
            link=extract_meeting_links(event),
            subject=event.get("summary", ""),
            location=event.get("location", ""),
            start_time=event.get("start", "").get("dateTime", "") or event.get("start", "").get("date", ""),
            end_time=event.get("end", "").get("dateTime", "") or event.get("end", "").get("date", ""),
            agenda=None,
        )

    def __str__(self):
        return (
            f"MeetingDTO(uuid={self.uuid}, google_calendar_id={self.google_calendar_id}, tenant_id={self.tenant_id}, "
            f"participants_emails={self.participants_emails}, link={self.link}, "
            f"subject={self.subject}, location={self.location}, start_time={self.start_time}, end_time={self.end_time}"
            f"agenda={self.agenda})"
        )


def hash_participants(participants_emails: list[str]) -> str:
    emails_string = json.dumps(participants_emails, sort_keys=True)
    return hashlib.sha256(emails_string.encode("utf-8")).hexdigest()


def extract_meeting_links(event):
    meeting_links = []

    # Patterns for different meeting links
    patterns = {
        "zoom": r"https://[a-zA-Z0-9.-]*zoom\.us/j/[^\s<]+",
        "google_meet": r"https://meet\.google\.com/[^\s<]+",
        "teams": r"https://teams\.microsoft\.com/[^\s<]+",
        "webex": r"https://[a-zA-Z0-9.-]*webex\.com/[^\s<]+",
        "gotomeeting": r"https://[a-zA-Z0-9.-]*gotomeeting\.com/[^\s<]+",
    }

    # Check in description
    description = event.get("description", "")
    for key, pattern in patterns.items():
        found = re.findall(pattern, description)
        meeting_links.extend(found)

    # Check in location
    location = event.get("location", "")
    for key, pattern in patterns.items():
        found = re.findall(pattern, location)
        meeting_links.extend(found)

    # Check in conferenceData
    conference_data = event.get("conferenceData", {})
    for entry_point in conference_data.get("entryPoints", []):
        uri = entry_point.get("uri", "")
        for key, pattern in patterns.items():
            found = re.findall(pattern, uri)
            meeting_links.extend(found)

    if len(meeting_links) == 0:
        return ""
    if len(meeting_links) > 1:
        meeting_links = list(set(meeting_links))
    return meeting_links[0] if meeting_links else ""
