import json


class MeetingDTO:
    def __init__(
        self,
        uuid,
        tenant_id,
        participants_emails,
        location,
        subject,
        start_time,
        end_time,
    ):
        self.uuid = uuid
        self.tenant_id = tenant_id
        self.participants_emails = participants_emails
        self.location = location
        self.subject = subject
        self.start_time = start_time
        self.end_time = end_time

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "tenant_id": self.tenant_id,
            "participants_emails": self.participants_emails,
            "location": self.location,
            "subject": self.subject,
            "start_time": self.start_time,
            "end_time": self.end_time,
        }

    @staticmethod
    def from_dict(data: dict):
        return MeetingDTO(
            uuid=data.get("uuid", ""),
            tenant_id=data.get("tenant_id", ""),
            participants_emails=data.get("participants_emails", []),
            location=data.get("location", ""),
            subject=data.get("subject", ""),
            start_time=data.get("start_time", ""),
            end_time=data.get("end_time", ""),
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.tenant_id,
            self.participants_emails,
            self.location,
            self.subject,
            self.start_time,
            self.end_time,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "MeetingDTO":
        return MeetingDTO(
            uuid=row[0],
            tenant_id=row[1],
            participants_emails=row[2],
            location=row[3],
            subject=row[4],
            start_time=row[5],
            end_time=row[6],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return MeetingDTO.from_dict(data)

    def __str__(self):
        return (
            f"MeetingDTO(uuid={self.uuid}, tenant_id={self.tenant_id}, "
            f"participants_emails={self.participants_emails}, location={self.location}, "
            f"subject={self.subject}, start_time={self.start_time}, end_time={self.end_time})"
        )
