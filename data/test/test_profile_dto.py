import pytest
from uuid import UUID, uuid4
from pydantic import ValidationError, HttpUrl
from data.data_common.data_transfer_objects.profile_dto import (
    Hobby,
    Phrase,
    Strength,
    Connection,
    ProfileDTO,
)
from data.data_common.data_transfer_objects.company_dto import CompanyDTO, NewsData

# Test data
valid_hobby_data = {"hobby_name": "Reading", "icon_url": "https://example.com/icon.png"}
invalid_hobby_data = {"icon_url": "https://example.com/icon.png"}

valid_phrase_data = {
    "phrase_text": "Great job",
    "reasoning": "Encourages motivation",
    "confidence_score": 90,
}
invalid_phrase_data = {"reasoning": "Encourages motivation", "confidence_score": 90}

valid_strength_data = {
    "strength_name": "Leadership",
    "reasoning": "Leads team well",
    "score": 80,
}
invalid_strength_data = {"reasoning": "Leads team well", "score": 80}

valid_connection_data = {
    "name": "John Doe",
    "image_url": "https://example.com/image.png",
    "linkedin_url": "https://linkedin.com/in/johndoe",
}
invalid_connection_data = {
    "name": "",
    "image_url": "https://example.com/image.png",
    "linkedin_url": "https://linkedin.com/in/johndoe",
}

valid_profile_data = {
    "uuid": str(uuid4()),
    "name": "Alice Smith",
    "company": "Tech Corp",
    "position": "Developer",
    "summary": "A skilled developer",
    "picture_url": "https://example.com/picture.png",
    "get_to_know": {"avoid": [], "best_practices": [], "phrases_to_use": []},
    "connections": [valid_connection_data],
    "strengths": [valid_strength_data],
    "hobbies": [str(uuid4())],
}
invalid_profile_data = {
    "uuid": str(uuid4()),
    "name": "",
    "company": "Tech Corp",
    "position": "Developer",
    "summary": "A skilled developer",
    "picture_url": "https://example.com/picture.png",
    "get_to_know": {"avoid": [], "best_practices": [], "phrases_to_use": []},
    "connections": [valid_connection_data],
    "strengths": [valid_strength_data],
    "hobbies": [str(uuid4())],
}


def test_hobby_creation():
    hobby = Hobby(**valid_hobby_data)
    assert hobby.hobby_name == "Reading"
    assert hobby.icon_url == "https://example.com/icon.png"

    with pytest.raises(ValidationError):
        Hobby(**invalid_hobby_data)


def test_phrase_creation():
    phrase = Phrase(**valid_phrase_data)
    assert phrase.phrase_text == "Great job"
    assert phrase.reasoning == "Encourages motivation"
    assert phrase.confidence_score == 90

    with pytest.raises(ValidationError):
        Phrase(**invalid_phrase_data)


def test_strength_creation():
    strength = Strength(**valid_strength_data)
    assert strength.strength_name == "Leadership"
    assert strength.reasoning == "Leads team well"
    assert strength.score == 80

    with pytest.raises(ValidationError):
        Strength(**invalid_strength_data)


def test_connection_creation():
    connection = Connection(**valid_connection_data)
    assert connection.name == "John Doe"
    assert isinstance(connection.name, str)
    assert str(connection.image_url) == "https://example.com/image.png"
    assert not connection.image_url == "https://example.com/image.png"
    assert str(connection.linkedin_url) == "https://linkedin.com/in/johndoe"
    assert not connection.linkedin_url == "https://linkedin.com/in/johndoe"

    with pytest.raises(ValidationError):
        Connection(**invalid_connection_data)


def test_profile_dto_creation():
    profile = ProfileDTO(**valid_profile_data)
    assert profile.name == "Alice Smith"
    assert profile.company == "Tech Corp"
    assert profile.position == "Developer"
    assert profile.summary == "A skilled developer"

    with pytest.raises(ValidationError):
        ProfileDTO(**invalid_profile_data)
