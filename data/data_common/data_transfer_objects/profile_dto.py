import json


class ProfileDTO:
    def __init__(
        self,
        uuid,
        name,
        company,
        position,
        challenges,
        strengths,
        summary,
        picture_url,
    ):
        self.uuid = uuid
        self.name = name
        self.company = company
        self.position = position
        self.challenges = challenges
        self.strengths = strengths
        self.summary = summary
        self.picture_url = picture_url

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": self.name,
            "company": self.company,
            "position": self.position,
            "challenges": self.challenges,
            "strengths": self.strengths,
            "summary": self.summary,
            "picture_url": self.picture_url,
        }

    @staticmethod
    def from_dict(data: dict):
        return ProfileDTO(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            company=data.get("company", ""),
            position=data.get("position", ""),
            challenges=data.get("challenges", ""),
            strengths=data.get("strengths", ""),
            summary=data.get("summary", ""),
            picture_url=data.get("picture_url", ""),
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.name,
            self.company,
            self.position,
            self.challenges,
            self.strengths,
            self.summary,
            self.picture_url,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "ProfileDTO":
        return ProfileDTO(
            uuid=row[0],
            name=row[1],
            company=row[2],
            position=row[3],
            challenges=row[4],
            strengths=row[5],
            summary=row[6],
            picture_url=row[7],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return ProfileDTO.from_dict(data)

    def __str__(self):
        return f"ProfileDTO(uuid={self.uuid}, name={self.name}, company={self.company}, position={self.position}, challenges={self.challenges}, strengths={self.strengths}, summary={self.summary}), picture_url={self.picture_url})"
