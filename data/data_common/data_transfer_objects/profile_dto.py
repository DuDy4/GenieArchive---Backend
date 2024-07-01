import json


class ProfileDTO:
    def __init__(self, uuid, owner_id, name, challenges, strengths, summary):
        self.uuid = uuid
        self.owner_id = owner_id
        self.name = name
        self.challenges = challenges
        self.strengths = strengths
        self.summary = summary

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "owner_id": self.owner_id,
            "name": self.name,
            "challenges": self.challenges,
            "strengths": self.strengths,
            "summary": self.summary,
        }

    @staticmethod
    def from_dict(data: dict):
        return ProfileDTO(
            uuid=data.get("uuid", ""),
            owner_id=data.get("owner_id", ""),
            name=data.get("name", ""),
            challenges=data.get("challenges", ""),
            strengths=data.get("strengths", ""),
            summary=data.get("summary", ""),
        )

    def to_tuple(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.uuid,
            self.owner_id,
            self.name,
            self.challenges,
            self.strengths,
            self.summary,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "ProfileDTO":
        return ProfileDTO(
            uuid=row[0],
            owner_id=row[1],
            name=row[2],
            challenges=row[3],
            strengths=row[4],
            summary=row[5],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return ProfileDTO.from_dict(data)

    def __str__(self):
        return f"ProfileDTO(uuid={self.uuid}, owner_id={self.owner_id}, name={self.name}, challenges={self.challenges}, strengths={self.strengths}, summary={self.summary})"
