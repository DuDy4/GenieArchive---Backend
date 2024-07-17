import json
from typing import List, Dict


class ProfileDTO:
    def __init__(
        self,
        uuid: str,
        name: str,
        company: str,
        position: str,
        challenges: List[Dict],
        strengths: List[Dict],
        hobbies: List[str],
        connections: List[str],
        news: List[str],
        summary: str,
        picture_url: str,
    ):
        self.uuid = uuid
        self.name = name
        self.company = company
        self.position = position
        self.challenges = challenges
        self.strengths = strengths
        self.hobbies = hobbies
        self.connections = connections
        self.news = news
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
            "hobbies": self.hobbies,
            "connections": self.connections,
            "news": self.news,
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
            challenges=data.get("challenges", []),
            strengths=data.get("strengths", []),
            hobbies=data.get("hobbies", []),
            connections=data.get("connections", []),
            news=data.get("news", []),
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
            self.hobbies,
            self.connections,
            self.news,
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
            hobbies=row[6],
            connections=row[7],
            news=row[8],
            summary=row[9],
            picture_url=row[10],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return ProfileDTO.from_dict(data)

    def __str__(self):
        return (
            f"ProfileDTO(uuid={self.uuid}, name={self.name}, company={self.company}, "
            f"position={self.position}, challenges={self.challenges}, strengths={self.strengths}, "
            f"hobbies={self.hobbies}, connections={self.connections}, news={self.news}, "
            f"summary={self.summary}, picture_url={self.picture_url})"
        )
