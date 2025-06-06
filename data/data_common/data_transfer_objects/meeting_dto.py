import hashlib
import json
import re
from enum import Enum

from datetime import datetime, timedelta, timezone

from typing import List, Dict, Any, Optional

from data.data_common.utils.str_utils import get_uuid4
from common.utils.email_utils import filter_emails_with_additional_domains
from pydantic import BaseModel, field_validator
from common.genie_logger import GenieLogger

from data.data_common.repositories.companies_repository import CompaniesRepository
logger = GenieLogger()

companies_repository = CompaniesRepository()

class MeetingClassification(Enum):
    EXTERNAL = "external"
    INTERNAL = "internal"
    PRIVATE = "private"
    DELETED = "deleted"

    @staticmethod
    def from_str(label: str):
        label = label.lower()
        if label in ("external", "internal", "private", "deleted"):
            return MeetingClassification[label.upper()]
        else:
            raise ValueError(f"Invalid classification: {label}")


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
        uuid: str,
        google_calendar_id: str,
        user_id: str,
        tenant_id: str,
        participants_emails: List[dict],
        participants_hash: Optional[str],
        link: str,
        subject: str,
        location: str,
        start_time: str,
        end_time: str,
        agenda: List[AgendaItem] = None,
        classification: MeetingClassification = MeetingClassification.PRIVATE,  # New field
        fake: bool = False,
    ):
        self.uuid = uuid
        self.google_calendar_id = google_calendar_id
        self.user_id = user_id
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
        self.classification = classification  # Enum field
        self.fake = fake

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "google_calendar_id": self.google_calendar_id,
            "user_id": self.user_id,
            "tenant_id": self.tenant_id,
            "participants_emails": self.participants_emails,
            "participants_hash": self.participants_hash,
            "link": self.link,
            "subject": self.subject,
            "location": self.location,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "agenda": [agenda_item.to_dict() for agenda_item in self.agenda] if self.agenda else None,
            "classification": self.classification.value,  # Convert enum to string
            "fake": self.fake,
        }

    @staticmethod
    def from_dict(data: dict) -> "MeetingDTO":
        return MeetingDTO(
            uuid=data.get("uuid", ""),
            google_calendar_id=data.get("google_calendar_id", ""),
            user_id=data.get("user_id", ""),
            tenant_id=data.get("tenant_id", ""),
            participants_emails=data.get("participants_emails", []),
            participants_hash=data.get(
                "participants_hash", hash_participants(data.get("participants_emails", []))
            ),
            link=data.get("link", ""),
            subject=data.get("subject", ""),
            location=data.get("location", ""),
            start_time=data.get("start_time", ""),
            end_time=data.get("end_time", ""),
            agenda=[AgendaItem.from_dict(agenda) for agenda in data.get("agenda")]
            if data.get("agenda")
            else None,
            classification=MeetingClassification.from_str(data.get("classification"))
            if data.get("classification")
            else evaluate_meeting_classification(
                data.get("participants_emails", []) or MeetingClassification.EXTERNAL
            ),
            fake=data.get("fake", False),
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.google_calendar_id,
            self.user_id,
            self.tenant_id,
            self.participants_emails,
            self.participants_hash,
            self.link,
            self.subject,
            self.location,
            self.start_time,
            self.end_time,
            self.agenda,
            self.classification.value,  # Convert enum to string for DB
            self.fake,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "MeetingDTO":
        return MeetingDTO(
            uuid=row[0],
            google_calendar_id=row[1],
            user_id=row[2],
            tenant_id=row[3],
            participants_emails=row[4],
            participants_hash=row[5],
            link=row[6],
            subject=row[7],
            location=row[8],
            start_time=row[9],
            end_time=row[10],
            agenda=[AgendaItem.from_dict(agenda) for agenda in row[11]] if row[11] else None,
            classification=MeetingClassification.from_str(row[12]) if row[12]
            else evaluate_meeting_classification(row[4]),
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
    def from_google_calendar_event(event, user_id: str, tenant_id: str) -> "MeetingDTO":
        participants = event.get("attendees", [])
        return MeetingDTO(
            uuid=event.get("uuid", get_uuid4()),
            google_calendar_id=event.get("id", ""),
            user_id=user_id,
            tenant_id=tenant_id,
            participants_emails=participants,
            participants_hash=event.get("participants_hash", hash_participants(event.get("attendees", []))),
            link=extract_meeting_links(event),
            subject=event.get("summary", ""),
            location=event.get("location", ""),
            start_time=event.get("start", "").get("dateTime", "") or event.get("start", "").get("date", ""),
            end_time=event.get("end", "").get("dateTime", "") or event.get("end", "").get("date", ""),
            agenda=None,
            classification=evaluate_meeting_classification(participants),
            fake=event.get("fake", False),
        )

    def __str__(self):
        return (
            f"MeetingDTO(uuid={self.uuid}, google_calendar_id={self.google_calendar_id},"
            f"user_id={self.user_id}, tenant_id={self.tenant_id}, "
            f"participants_emails={self.participants_emails}, link={self.link}, "
            f"subject={self.subject}, location={self.location}, start_time={self.start_time}, end_time={self.end_time}"
            f"agenda={self.agenda}), classification={self.classification}, fake={self.fake}"
        )

    @staticmethod
    def calculate_reminder_schedule(start_time_str: str) -> Optional[datetime]:
        """
        Calculate reminder time 30 minutes before meeting start time.
        Expects start_time_str in ISO format with timezone info (e.g., "2024-08-21T15:00:00+03:00")
        Returns reminder time in UTC.
        """
        try:
            # Parse the start time with timezone information
            start_time = datetime.fromisoformat(start_time_str)
            logger.info(f"Start time: {start_time}")

            # Convert to UTC if not already
            start_time_utc = start_time.astimezone(timezone.utc)
            logger.info(f"Start time (UTC): {start_time_utc}")

            # Calculate the reminder time (30 minutes before) in UTC
            reminder_time = start_time_utc - timedelta(minutes=30)
            logger.info(f"Reminder time (UTC): {reminder_time}")

            # Round to the nearest 5-minute interval
            rounded_minutes = (reminder_time.minute // 5) * 5
            reminder_time = reminder_time.replace(minute=rounded_minutes, second=0, microsecond=0)
            logger.info(f"Reminder time (rounded): {reminder_time}")

            # Handle rounding that could push into the previous hour
            if reminder_time.minute != rounded_minutes:
                reminder_time = reminder_time + timedelta(minutes=5 - (reminder_time.minute % 5))
                logger.info(f"Reminder time minutes (rounded): {reminder_time}")

            return reminder_time

        except ValueError as e:
            logger.error(f"Invalid start_time format: {start_time_str} - {e}")
            return None


def evaluate_meeting_classification(participants_emails: List[dict]) -> MeetingClassification:
    if len(participants_emails) > 14:
        return MeetingClassification.INTERNAL
    if len(participants_emails) <= 1:
        logger.info(f"Classifying meeting as PRIVATE with participants {participants_emails}")
        return MeetingClassification.PRIVATE
    self_email = [email.get("email") for email in participants_emails if email.get("self")]
    if not self_email:
        logger.error(f"Self email not found in participants {participants_emails}")
        return MeetingClassification.PRIVATE
    self_domain = self_email[0].split("@")[1] if self_email and '@' in self_email[0] else ''
    if not self_domain:
        logger.error(f"Self domain not found in email {self_email[0]}")
        return MeetingClassification.PRIVATE
    additional_domains = companies_repository.get_additional_domains(self_domain)
    if len(filter_emails_with_additional_domains(self_email[0], participants_emails, additional_domains)) >= 1:
        logger.info(f"Classifying meeting as EXTERNAL with participants {participants_emails}")
        return MeetingClassification.EXTERNAL
    logger.info(f"Classifying meeting as INTERNAL with participants {participants_emails}")
    return MeetingClassification.INTERNAL


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



