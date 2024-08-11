import pytest
from datetime import date
from pydantic import ValidationError
from typing import Tuple, Dict, Any, List, Union
import psycopg2
from unittest.mock import Mock, patch, MagicMock

from data.data_common.data_transfer_objects.company_dto import NewsData, CompanyDTO
from data.data_common.repositories.companies_repository import CompaniesRepository

# Helper function to create a mock connection
def create_mock_connection():
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value = cursor

    # Make the cursor a context manager
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock(return_value=None)

    return conn


# Test NewsData class
def test_news_data_creation():
    news_data = NewsData(
        date=date.today(),
        link="https://example.com",
        media="Example Media",
        title="Example Title",
        summary="Example Summary",
    )
    assert news_data.date == date.today()
    assert str(news_data.link) == "https://example.com/"
    assert news_data.media == "Example Media"
    assert news_data.title == "Example Title"
    assert news_data.summary == "Example Summary"


def test_news_data_empty_fields():
    with pytest.raises(ValueError):
        NewsData(
            date=date.today(),
            link="https://example.com",
            media=" ",
            title="Example Title",
            summary="Example Summary",
        )


def test_news_data_serialization():
    news_data = NewsData(
        date=date.today(),
        link="https://example.com",
        media="Example Media",
        title="Example Title",
        summary="Example Summary",
    )
    json_str = news_data.to_json()
    assert "Example Media" in json_str


def test_news_data_deserialization():
    json_str = '{"date": "2023-01-01", "link": "https://example.com", "media": "Example Media", "title": "Example Title", "summary": "Example Summary"}'
    news_data = NewsData.from_json(json_str)
    assert news_data.date == date(2023, 1, 1)


def test_news_data_to_tuple():
    news_data = NewsData(
        date=date.today(),
        link="https://example.com",
        media="Example Media",
        title="Example Title",
        summary="Example Summary",
    )
    data_tuple = news_data.to_tuple()
    assert isinstance(data_tuple, Tuple)


def test_news_data_from_tuple():
    data_tuple = (
        date.today(),
        "https://example.com",
        "Example Media",
        "Example Title",
        "Example Summary",
    )
    news_data = NewsData.from_tuple(data_tuple)
    assert news_data.media == "Example Media"


def test_news_data_to_dict():
    news_data = NewsData(
        date=date.today(),
        link="https://example.com",
        media="Example Media",
        title="Example Title",
        summary="Example Summary",
    )
    data_dict = news_data.to_dict()
    assert isinstance(data_dict, Dict)


def test_news_data_from_dict():
    data_dict = {
        "date": "2023-01-01",
        "link": "https://example.com",
        "media": "Example Media",
        "title": "Example Title",
        "summary": "Example Summary",
    }
    news_data = NewsData.from_dict(data_dict)
    assert news_data.media == "Example Media"


# Test CompanyDTO class
def test_company_dto_creation():
    company_dto = CompanyDTO(
        uuid="1234",
        name="Example Company",
        domain="example.com",
        size="100-200",
        description="Example Description",
        overview="Example Overview",
        challenges={},
        technologies=[],
        employees=[],
        news=[],
    )
    assert company_dto.uuid == "1234"
    assert company_dto.name == "Example Company"


def test_company_dto_serialization():
    company_dto = CompanyDTO(
        uuid="1234",
        name="Example Company",
        domain="example.com",
        size="100-200",
        description="Example Description",
        overview="Example Overview",
        challenges={},
        technologies=[],
        employees=[],
        news=[],
    )
    json_str = company_dto.to_json()
    assert "Example Company" in json_str


def test_company_dto_deserialization():
    json_str = '{"uuid": "1234", "name": "Example Company", "domain": "example.com", "size": "100-200", "description": "Example Description", "overview": "Example Overview", "challenges": {}, "technologies": [], "employees": [], "news": []}'
    company_dto = CompanyDTO.from_json(json_str)
    assert company_dto.name == "Example Company"


def test_company_dto_to_dict():
    company_dto = CompanyDTO(
        uuid="1234",
        name="Example Company",
        domain="example.com",
        size="100-200",
        description="Example Description",
        overview="Example Overview",
        challenges={},
        technologies=[],
        employees=[],
        news=[],
    )
    data_dict = company_dto.to_dict()
    assert isinstance(data_dict, Dict)


def test_company_dto_from_dict():
    data_dict = {
        "uuid": "1234",
        "name": "Example Company",
        "domain": "example.com",
        "size": "100-200",
        "description": "Example Description",
        "overview": "Example Overview",
        "challenges": {},
        "technologies": [],
        "employees": [],
        "news": [],
    }
    company_dto = CompanyDTO.from_dict(data_dict)
    assert company_dto.name == "Example Company"


# Test CompaniesRepository class
@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_create_table_if_not_exists(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    repo.create_table_if_not_exists()
    conn.cursor().execute.assert_called()


@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_get_company(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    conn.cursor().fetchone.return_value = (
        "1234",
        "Example Company",
        "example.com",
        "100-200",
        "Example Description",
        "Example Overview",
        {},
        [],
        [],
        [],
    )
    company = repo.get_company("1234")
    assert company.name == "Example Company"


@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_get_company_not_exists(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    conn.cursor().fetchone.return_value = None
    company = repo.get_company("1234")
    assert company is None


@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_save_company_insert(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    company_dto = CompanyDTO(
        uuid="1234",
        name="Example Company",
        domain="example.com",
        size="100-200",
        description="Example Description",
        overview="Example Overview",
        challenges={},
        technologies=[],
        employees=[],
        news=[],
    )
    with patch.object(repo, "exists_domain", return_value=False):
        cursor = conn.cursor.return_value
        cursor.fetchone.return_value = [1]  # Mocking the return value of fetchone
        repo.save_company_without_news(company_dto)
        conn.cursor().execute.assert_called()


@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_save_company_update(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    company_dto = CompanyDTO(
        uuid="1234",
        name="Example Company",
        domain="example.com",
        size="100-200",
        description="Example Description",
        overview="Example Overview",
        challenges={},
        technologies=[],
        employees=[],
        news=[],
    )
    with patch.object(repo, "exists_domain", return_value=True):
        repo.save_company_without_news(company_dto)
        conn.cursor().execute.assert_called()


@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_delete_company(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    repo.delete_company("1234")
    conn.cursor().execute.assert_called()


# Extreme tests for NewsData handling in save_news, get_news, get_news_data_by_email
@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_save_news(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    news_data = [
        NewsData(
            date=date.today(),
            link="https://example.com/news1",
            media="Media1",
            title="Title1",
            summary="Summary1",
        ),
        {
            "date": "2023-01-01",
            "link": "https://example.com/news2",
            "media": "Media2",
            "title": "Title2",
            "summary": "Summary2",
        },
        {
            "date": "invalid-date",
            "link": "invalid-link",
            "media": " ",
            "title": " ",
            "summary": " ",
        },
    ]
    with patch.object(repo, "validate_news", wraps=repo.validate_news) as mock_validate:
        repo.save_news("1234", news_data)
        mock_validate.assert_called_once_with(news_data)


@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_get_news(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    conn.cursor().fetchone.return_value = [
        [
            {
                "date": "2023-01-01",
                "link": "https://example.com/news1",
                "media": "Media1",
                "title": "Title1",
                "summary": "Summary1",
            },
            {
                "date": "invalid-date",
                "link": "invalid-link",
                "media": " ",
                "title": " ",
                "summary": " ",
            },
        ]
    ]
    news = repo.get_news("1234")
    assert len(news) == 1  # Only the valid news data should be returned
    assert news[0].media == "Media1"


@patch("data.data_common.repositories.companies_repository.psycopg2.connect")
def test_get_news_data_by_email(mock_connect):
    conn = create_mock_connection()
    mock_connect.return_value = conn
    repo = CompaniesRepository(conn)
    conn.cursor().fetchone.return_value = [
        [
            {
                "date": "2023-01-01",
                "link": "https://example.com/news1",
                "media": "Media1",
                "title": "Title1",
                "summary": "Summary1",
            },
            {
                "date": "invalid-date",
                "link": "invalid-link",
                "media": " ",
                "title": " ",
                "summary": " ",
            },
        ]
    ]
    news = repo.get_news_data_by_email("test@example.com")
    assert len(news) == 1  # Only the valid news data should be returned
    assert news[0].media == "Media1"
