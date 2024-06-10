import json


class ProfileDTO:
    def __init__(self, uuid, name, challenges, strengths, summary):
        self.uuid = uuid
        self.name = name
        self.challenges = challenges
        self.strengths = strengths
        self.summary = summary

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": self.name,
            "challenges": self.challenges,
            "strengths": self.strengths,
            "summary": self.summary,
        }

    @staticmethod
    def from_dict(data: dict):
        return ProfileDTO(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            challenges=data.get("challenges", ""),
            strengths=data.get("strengths", ""),
            summary=data.get("summary", ""),
        )

    def to_tuple(self) -> tuple[str, str, str, str, str]:
        return (
            self.uuid,
            self.name,
            self.challenges,
            self.strengths,
            self.summary,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "ProfileDTO":
        return ProfileDTO(
            uuid=row[0],
            name=row[1],
            challenges=row[2],
            strengths=row[3],
            summary=row[4],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return ProfileDTO.from_dict(data)

    def __str__(self):
        return f"ProfileDTO(uuid={self.uuid}, name={self.name}, challenges={self.challenges}, strengths={self.strengths}, summary={self.summary})"
