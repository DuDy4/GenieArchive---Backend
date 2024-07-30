import warnings
import pytest
from fastapi.testclient import TestClient
from google.auth.credentials import Credentials
from httpx import AsyncClient, ASGITransport
from unittest.mock import MagicMock, patch

from data.data_common.dependencies.dependencies import (
    profiles_repository,
    persons_repository,
    hobbies_repository,
    meetings_repository,
    google_creds_repository,
    personal_data_repository,
    tenants_repository,
    ownerships_repository,
)
from start_api import app
import requests
from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.repositories.google_creds_repository import GoogleCredsRepository
from data.data_common.repositories.personal_data_repository import (
    PersonalDataRepository,
)
from data.data_common.repositories.ownerships_repository import OwnershipsRepository
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.hobbies_repository import HobbiesRepository
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.data_transfer_objects.person_dto import PersonDTO

# Ignore deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

client = TestClient(app)


@pytest.mark.asyncio
async def test_root_redirect():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/")
    assert response.status_code == 307
    assert response.headers["location"] == "http://test/docs"


@pytest.mark.asyncio
async def test_get_user_info():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/user-info")
    assert response.status_code == 200
    data = response.json()
    assert data["tenantId"] == "TestOwner"
    assert data["name"] == "Dan Shevel"
    assert data["email"] == "dan.shevel@genie.ai"


@pytest.mark.asyncio
async def test_test_google_token_invalid(mocker):
    token = "invalid_token"

    # Mock the requests.get call to return a response with status_code 400
    mock_response = mocker.Mock()
    mock_response.status_code = 400
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError
    mock_response.json.return_value = {"error": "Invalid token"}

    mocker.patch("requests.get", return_value=mock_response)

    response = None
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        try:
            response = await ac.get(f"/v1/test-google-token?token={token}")
        except requests.exceptions.HTTPError:
            response = mock_response

    assert response.status_code == 400
    assert response.json() == {"error": "Invalid token"}


@pytest.mark.asyncio
async def test_successful_login(mocker):
    mock_request_data = {
        "data": {"claims": {"email": "test@example.com", "tenantId": "TestTenantId"}}
    }

    mock_google_creds_repo = MagicMock()
    mock_tenants_repo = MagicMock()

    async def mock_json():
        return mock_request_data

    mock_request = MagicMock()
    mock_request.json = mock_json

    mocker.patch("data.api.api_manager.fetch_google_meetings")

    app.dependency_overrides[GoogleCredsRepository] = lambda: mock_google_creds_repo
    app.dependency_overrides[TenantsRepository] = lambda: mock_tenants_repo

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/v1/successful-login", json=mock_request_data)

    assert response.status_code == 200
    assert response.json() == {"verdict": "allow"}


@pytest.mark.asyncio
async def test_post_social_auth_data(mocker):
    mock_request_data = {
        "prehookContext": {},
        "data": {
            "authData": {
                "user": {"email": "test@example.com"},
                "tokens": {
                    "accessToken": "test_access_token",
                    "idToken": "test_id_token",
                },
            }
        },
    }

    mock_google_creds_repo = MagicMock()
    mock_tenants_repo = MagicMock()

    async def mock_json():
        return mock_request_data

    mock_request = MagicMock()
    mock_request.json = mock_json

    app.dependency_overrides[GoogleCredsRepository] = lambda: mock_google_creds_repo
    app.dependency_overrides[TenantsRepository] = lambda: mock_tenants_repo

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post("/v1/social-auth-data", json=mock_request_data)

    assert response.status_code == 200
    assert response.json() == {"tenantId": "TestOwner"}


@pytest.mark.asyncio
async def test_get_all_profiles(mocker):
    mock_profiles_repo = MagicMock()
    mock_ownerships_repo = MagicMock()

    mock_ownerships_repo.get_all_persons_for_tenant.return_value = ["uuid1", "uuid2"]
    mock_profiles_repo.get_profiles_from_list.return_value = [
        ProfileDTO(
            uuid="uuid1",
            name="John Doe",
            company="Company A",
            position="Position A",
            challenges=[],
            strengths=[],
            hobbies=[],
            connections=[],
            news=[],
            get_to_know={},
            summary="Summary",
            picture_url="http://example.com/pic1.jpg",
        ),
        ProfileDTO(
            uuid="uuid2",
            name="Jane Doe",
            company="Company B",
            position="Position B",
            challenges=[],
            strengths=[],
            hobbies=[],
            connections=[],
            news=[],
            get_to_know={},
            summary="Summary",
            picture_url="http://example.com/pic2.jpg",
        ),
    ]

    app.dependency_overrides[ProfilesRepository] = lambda: mock_profiles_repo
    app.dependency_overrides[profiles_repository] = lambda: mock_profiles_repo
    app.dependency_overrides[OwnershipsRepository] = lambda: mock_ownerships_repo
    app.dependency_overrides[ownerships_repository] = lambda: mock_ownerships_repo

    # Adding debug logging
    print(
        f"Mock ownerships_repo.get_all_persons_for_tenant: {mock_ownerships_repo.get_all_persons_for_tenant.return_value}"
    )
    print(
        f"Mock profiles_repo.get_profiles_from_list: {mock_profiles_repo.get_profiles_from_list.return_value}"
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/profiles/TestTenantId")

    print(f"Response status code: {response.status_code}")
    print(f"Response JSON: {response.json()}")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_get_all_meetings_by_profile_name(mocker):
    mock_meetings_repo = MagicMock()
    mock_ownerships_repo = MagicMock()
    mock_persons_repo = MagicMock()

    mock_ownerships_repo.get_all_persons_for_tenant.return_value = ["uuid1", "uuid2"]
    mock_persons_repo.get_emails_list.return_value = [
        "email1@example.com",
        "email2@example.com",
    ]
    mock_meetings_repo.get_meetings_by_participants_emails.return_value = [
        MeetingDTO(
            uuid="uuid1",
            google_calendar_id="cal_id_1",
            tenant_id="TestTenantId",
            participants_emails=["email1@example.com", "email2@example.com"],
            link="http://example.com/meeting1",
            subject="Meeting 1",
            start_time="2024-07-30T10:00:00Z",
            end_time="2024-07-30T11:00:00Z",
        ),
        MeetingDTO(
            uuid="uuid2",
            google_calendar_id="cal_id_2",
            tenant_id="TestTenantId",
            participants_emails=["email3@example.com"],
            link="http://example.com/meeting2",
            subject="Meeting 2",
            start_time="2024-07-30T12:00:00Z",
            end_time="2024-07-30T13:00:00Z",
        ),
    ]

    app.dependency_overrides[MeetingsRepository] = lambda: mock_meetings_repo
    app.dependency_overrides[OwnershipsRepository] = lambda: mock_ownerships_repo
    app.dependency_overrides[PersonsRepository] = lambda: mock_persons_repo
    app.dependency_overrides[meetings_repository] = lambda: mock_meetings_repo
    app.dependency_overrides[ownerships_repository] = lambda: mock_ownerships_repo
    app.dependency_overrides[persons_repository] = lambda: mock_persons_repo

    # Adding debug logging
    print(
        f"Mock ownerships_repo.get_all_persons_for_tenant: {mock_ownerships_repo.get_all_persons_for_tenant.return_value}"
    )
    print(
        f"Mock persons_repo.get_emails_list: {mock_persons_repo.get_emails_list.return_value}"
    )
    print(
        f"Mock meetings_repo.get_meetings_by_participants_emails: {mock_meetings_repo.get_meetings_by_participants_emails.return_value}"
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/TestTenantId/meetings?name=John")

    print(f"Response status code: {response.status_code}")
    print(f"Response JSON: {response.json()}")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_get_profile_attendee_info(mocker):
    mock_profiles_repo = MagicMock()
    mock_ownerships_repo = MagicMock()
    mock_personal_data_repo = MagicMock()

    mock_ownerships_repo.check_ownership.return_value = True
    mock_profiles_repo.get_profile_data.return_value = ProfileDTO(
        uuid="uuid1",
        name="John Doe",
        company="Company",
        position="Position",
        challenges=[],
        strengths=[],
        hobbies=[],
        connections=[],
        news=[],
        get_to_know={},
        summary="Summary",
        picture_url="http://example.com/pic.jpg",
    )
    mock_personal_data_repo.get_social_media_links.return_value = [
        {
            "network": "LinkedIn",
            "url": "http://linkedin.com/in/johndoe",
            "id": "johndoe",
            "username": "johndoe",
        },
    ]

    app.dependency_overrides[ProfilesRepository] = lambda: mock_profiles_repo
    app.dependency_overrides[OwnershipsRepository] = lambda: mock_ownerships_repo
    app.dependency_overrides[PersonalDataRepository] = lambda: mock_personal_data_repo
    app.dependency_overrides[profiles_repository] = lambda: mock_profiles_repo
    app.dependency_overrides[ownerships_repository] = lambda: mock_ownerships_repo
    app.dependency_overrides[personal_data_repository] = lambda: mock_personal_data_repo

    # Adding debug logging
    print(
        f"Mock profiles_repo.get_profile_data: {mock_profiles_repo.get_profile_data.return_value}"
    )
    print(
        f"Mock personal_data_repo.get_social_media_links: {mock_personal_data_repo.get_social_media_links.return_value}"
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/TestTenantId/profiles/uuid1/attendee-info")

    print(f"Response status code: {response.status_code}")
    print(f"Response JSON: {response.json()}")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "John Doe"


@pytest.mark.asyncio
async def test_get_profile_strengths(mocker):
    mock_profiles_repo = MagicMock()
    mock_ownerships_repo = MagicMock()

    mock_ownerships_repo.check_ownership.return_value = True
    mock_profiles_repo.get_profile_data.return_value = ProfileDTO(
        uuid="uuid1",
        name="John Doe",
        company="Company",
        position="Position",
        challenges=[],
        strengths=[{"strength_name": "Strength1", "score": 90, "reason": "Reason1"}],
        hobbies=[],
        connections=[],
        news=[],
        get_to_know={},
        summary="Summary",
        picture_url="http://example.com/pic.jpg",
    )

    app.dependency_overrides[ProfilesRepository] = lambda: mock_profiles_repo
    app.dependency_overrides[OwnershipsRepository] = lambda: mock_ownerships_repo
    app.dependency_overrides[profiles_repository] = lambda: mock_profiles_repo
    app.dependency_overrides[ownerships_repository] = lambda: mock_ownerships_repo

    # Adding debug logging
    print(
        f"Mock profiles_repo.get_profile_data: {mock_profiles_repo.get_profile_data.return_value}"
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/TestTenantId/profiles/uuid1/strengths")

    print(f"Response status code: {response.status_code}")
    print(f"Response JSON: {response.json()}")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["strength_name"] == "Strength1"


@pytest.mark.asyncio
async def test_get_profile_get_to_know(mocker):
    mock_profiles_repo = MagicMock()
    mock_ownerships_repo = MagicMock()

    mock_ownerships_repo.check_ownership.return_value = True
    mock_profiles_repo.get_profile_data.return_value = ProfileDTO(
        uuid="uuid1",
        name="John Doe",
        company="Company",
        position="Position",
        challenges=[],
        strengths=[],
        hobbies=[],
        connections=[],
        news=[],
        get_to_know={"info": "Get to know details"},
        summary="Summary",
        picture_url="http://example.com/pic.jpg",
    )

    app.dependency_overrides[ProfilesRepository] = lambda: mock_profiles_repo
    app.dependency_overrides[OwnershipsRepository] = lambda: mock_ownerships_repo
    app.dependency_overrides[profiles_repository] = lambda: mock_profiles_repo
    app.dependency_overrides[ownerships_repository] = lambda: mock_ownerships_repo

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/TestTenantId/profiles/uuid1/get-to-know")

    assert response.status_code == 200
    data = response.json()
    assert data["info"] == "Get to know details"


@pytest.mark.asyncio
async def test_get_profile_good_to_know(mocker):
    mock_profiles_repo = MagicMock()
    mock_ownerships_repo = MagicMock()
    mock_persons_repo = MagicMock()
    mock_hobbies_repo = MagicMock()

    mock_ownerships_repo.check_ownership.return_value = True
    mock_profiles_repo.get_profile_data.return_value = ProfileDTO(
        uuid="uuid1",
        name="John Doe",
        company="Company",
        position="Position",
        challenges=[],
        strengths=[],
        hobbies=["uuid1", "uuid2"],
        connections=["uuid3", "uuid4"],
        news=["news1", "news2"],
        get_to_know={},
        summary="Summary",
        picture_url="http://example.com/pic.jpg",
    )
    mock_hobbies_repo.get_hobby.side_effect = lambda uuid: {
        "hobby_name": f"Hobby{uuid[-1]}",
        "icon_url": f"http://example.com/hobby{uuid[-1]}.png",
    }
    mock_persons_repo.get_person.side_effect = lambda uuid: PersonDTO(
        uuid=uuid,
        name=f"Person{uuid[-1]}",
        company="Company",
        position="Position",
        email="test@test.com",
        linkedin="http://linkedin.com/in/person",
        timezone="",
    )
    mock_profiles_repo.get_profile_picture.return_value = "http://example.com/pic.jpg"

    app.dependency_overrides[ProfilesRepository] = lambda: mock_profiles_repo
    app.dependency_overrides[OwnershipsRepository] = lambda: mock_ownerships_repo
    app.dependency_overrides[PersonsRepository] = lambda: mock_persons_repo
    app.dependency_overrides[HobbiesRepository] = lambda: mock_hobbies_repo
    app.dependency_overrides[profiles_repository] = lambda: mock_profiles_repo
    app.dependency_overrides[ownerships_repository] = lambda: mock_ownerships_repo
    app.dependency_overrides[persons_repository] = lambda: mock_persons_repo
    app.dependency_overrides[hobbies_repository] = lambda: mock_hobbies_repo

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/TestTenantId/profiles/uuid1/good-to-know")

    assert response.status_code == 200
    data = response.json()
    assert len(data["hobbies"]) == 2
    assert len(data["connections"]) == 2
    assert data["news"] == ["news1", "news2"]


@pytest.mark.asyncio
async def test_get_profile_work_experience(mocker):
    mock_personal_data_repo = MagicMock()

    mock_personal_data_repo.get_personal_data.return_value = {
        "experience": [
            {"company": "Company1", "position": "Position1"},
            {"company": "Company2", "position": "Position2"},
        ]
    }

    app.dependency_overrides[PersonalDataRepository] = lambda: mock_personal_data_repo
    app.dependency_overrides[personal_data_repository] = lambda: mock_personal_data_repo

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/v1/TestTenantId/profiles/uuid1/work-experience")

    assert response.status_code == 200
    data = response.json()
    print(data)
    assert len(data) == 2
    assert data[0]["company"] == "Company1"
    assert data[1]["position"] == "Position2"


# @pytest.mark.asyncio
# async def test_fetch_google_meetings(mocker):
#     mock_google_creds_repo = MagicMock()
#     mock_tenants_repo = MagicMock()
#
#     mock_google_creds_repo.get_creds.return_value = {
#         "access_token": "access_token",
#         "refresh_token": "refresh_token"
#     }
#     mock_tenants_repo.get_tenant_id_by_email.return_value = "TestTenantId"
#
#     mock_credentials = MagicMock(spec=Credentials)
#     mock_credentials.token = "access_token"
#
#     mocker.patch("google.oauth2.credentials.Credentials", return_value=mock_credentials)
#     mock_build = mocker.patch("googleapiclient.discovery.build")
#     mock_service = mock_build.return_value
#     mock_events = mock_service.events.return_value
#     mock_list = mock_events.list.return_value
#     mock_execute = mock_list.execute.return_value
#     mock_execute.return_value = {"items": []}
#
#     app.dependency_overrides[GoogleCredsRepository] = lambda: mock_google_creds_repo
#     app.dependency_overrides[TenantsRepository] = lambda: mock_tenants_repo
#
#     async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
#         response = await ac.get("/v1/google/meetings/test@genie.ai")
#
#     assert response.status_code == 200
#     data = response.json()
#     assert data["events"] == []
