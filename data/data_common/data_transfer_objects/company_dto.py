import json
from typing import List, Dict, Optional, Union
from data.data_common.utils.str_utils import (
    titleize_values,
    to_custom_title_case,
    get_uuid4,
)


class CompanyDTO:
    def __init__(
        self,
        uuid: str,
        name: str,
        domain: str,
        size: Optional[str],
        description: Optional[str],
        overview: Optional[str],
        challenges: Optional[Union[Dict, List[Dict]]],
        technologies: Optional[Union[Dict, List[Dict]]],
        employees: Optional[Union[Dict, List[Dict]]],
    ):
        self.uuid = uuid
        self.name = name
        self.domain = domain
        self.size = size
        self.description = description
        self.overview = overview
        self.challenges = challenges
        self.technologies = technologies
        self.employees = employees

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": to_custom_title_case(self.name),
            "domain": self.domain,
            "size": self.size,
            "description": self.description,
            "overview": self.overview,
            "challenges": self.challenges,
            "technologies": self.technologies,
            "employees": self.employees,
        }

    @staticmethod
    def from_dict(data: dict):
        return CompanyDTO(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            domain=data.get("domain", ""),
            size=data.get("size", None),
            description=data.get("description", None),
            overview=data.get("overview", None),
            challenges=data.get("challenges", None),
            technologies=data.get("technologies", None),
            employees=data.get("employees", None),
        )

    @staticmethod
    def from_hunter_object(data: dict) -> "CompanyDTO":
        employees = data.get("emails") or data.get("employees") or []
        processed_employees = [
            {
                "name": f"{email.get('first_name', '')} {email.get('last_name', '')}".strip(),
                "email": email.get("value"),
                "position": email.get("position"),
                "linkedin": email.get("linkedin"),
                "department": email.get("department"),
            }
            for email in employees
        ]

        return CompanyDTO(
            uuid=data.get(
                "uuid", get_uuid4()
            ),  # Assuming get_uuid4() generates a new UUID
            name=data.get("organization", ""),
            domain=data.get("domain", ""),
            size=data.get("headcount", ""),
            description=data.get("description", ""),
            overview=data.get("overview", ""),
            challenges=data.get("challenges", {}),
            technologies=data.get("technologies", []),
            employees=processed_employees,
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.name,
            self.domain,
            self.size,
            self.description,
            self.overview,
            self.challenges,
            self.technologies,
            self.employees,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "CompanyDTO":
        return CompanyDTO(
            uuid=row[0],
            name=row[1],
            domain=row[2],
            size=row[3],
            description=row[4],
            overview=row[5],
            challenges=row[6],
            technologies=row[7],
            employees=row[8],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return CompanyDTO.from_dict(data)

    def __str__(self):
        return (
            f"CompanyDTO(uuid={self.uuid},\n name={self.name},\n domain={self.domain},\n size={self.size},\n "
            f"description={self.description},\n overview={self.overview},\n challenges={self.challenges},\n "
            f"technologies={self.technologies},\n employees={self.employees})"
        )
