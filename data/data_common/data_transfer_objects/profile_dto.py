import json


class ProfileDTO:
    def __init__(
        self,
        uuid,
        owner_id,
        name,
        company,
        position,
        challenges,
        strengths,
        summary,
        picture_url,
    ):
        self.uuid = uuid
        self.owner_id = owner_id
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
            "owner_id": self.owner_id,
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
            owner_id=data.get("owner_id", ""),
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
            self.owner_id,
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
            owner_id=row[1],
            name=row[2],
            company=row[3],
            position=row[4],
            challenges=row[5],
            strengths=row[6],
            summary=row[7],
            picture_url=row[8],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return ProfileDTO.from_dict(data)

    def __str__(self):
        return f"ProfileDTO(uuid={self.uuid}, owner_id={self.owner_id}, name={self.name}, company={self.company}, position={self.position}, challenges={self.challenges}, strengths={self.strengths}, summary={self.summary}), picture_url={self.picture_url})"
