import json
from typing import List, Dict
from data.data_common.utils.str_utils import titleize_values, to_custom_title_case


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
        get_to_know: Dict[str, Dict | str],
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
        self.get_to_know = get_to_know
        self.summary = summary
        self.picture_url = picture_url

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": to_custom_title_case(self.name),
            "company": to_custom_title_case(self.company),
            "position": to_custom_title_case(self.position),
            "challenges": titleize_values(self.challenges),
            "strengths": self.strengths,
            "hobbies": titleize_values(self.hobbies),
            "connections": to_custom_title_case(self.connections),
            "news": titleize_values(self.news),
            "get_to_know": self.get_to_know,
            "summary": titleize_values(self.summary),
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
            get_to_know=data.get("get_to_know", {}),
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
            self.get_to_know,
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
            get_to_know=row[9],
            summary=row[10],
            picture_url=row[11],
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
            f"get_to_know={self.get_to_know}, summary={self.summary}, picture_url={self.picture_url})"
        )
