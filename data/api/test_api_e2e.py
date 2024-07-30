import pytest
import httpx
from fastapi import FastAPI
from starlette.testclient import TestClient
from unittest.mock import patch, MagicMock, create_autospec
from google.oauth2 import credentials as google_credentials

from data.api.api_manager import v1_router

app = FastAPI()
app.include_router(v1_router)

client = TestClient(app)


@pytest.fixture
def test_client():
    return client


def test_test_google_token(test_client):
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"mock": "data"}

        token = "sample_invalid_token"
        response = test_client.get(f"/v1/test-google-token?token={token}")
        assert response.status_code == 200
        assert response.json() == {"mock": "data"}


def test_get_user(test_client):
    response = test_client.get("/v1/user-info")
    assert response.status_code == 200
    assert response.json() == {
        "tenantId": "TestOwner",
        "name": "Dan Shevel",
        "email": "dan.shevel@genie.ai",
    }


def test_post_social_auth_data(test_client):
    with patch(
        "data.data_common.repositories.google_creds_repository.GoogleCredsRepository.insert"
    ) as mock_insert:
        mock_insert.return_value = None

        auth_data = {
            "prehookContext": {},
            "data": {
                "authData": {
                    "user": {"email": "test@genie.ai"},
                    "tokens": {"accessToken": "access_token", "idToken": "id_token"},
                },
            },
        }
        response = test_client.post("/v1/social-auth-data", json=auth_data)
        assert response.status_code == 200
        assert response.json() == {"tenantId": "TestOwner"}


def test_get_user_account(test_client):
    with patch(
        "data.data_common.repositories.tenants_repository.TenantsRepository.exists"
    ) as mock_exists:
        mock_exists.return_value = None
        data = {"tenantId": "testTenantId", "name": "Test User"}
        response = test_client.post("/v1/users/signin", json=data)
        assert response.status_code == 200
        assert "message" in response.json()


def test_get_all_profiles(test_client):
    tenant_id = "testTenantId"
    response = test_client.get(f"/v1/profiles/{tenant_id}")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_all_meetings_by_profile_name(test_client):
    tenant_id = "testTenantId"
    response = test_client.get(f"/v1/{tenant_id}/meetings")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_all_profile_for_meeting(test_client):
    tenant_id = "testTenantId"
    meeting_id = "testMeetingId"
    response = test_client.get(f"/v1/{tenant_id}/{meeting_id}/profiles/")
    assert response.status_code == 200


def test_get_profile_attendee_info(test_client):
    tenant_id = "testTenantId"
    profile_uuid = "testProfileUuid"
    response = test_client.get(f"/v1/{tenant_id}/profiles/{profile_uuid}/attendee-info")
    assert response.status_code == 200


def test_get_profile_strengths(test_client):
    tenant_id = "testTenantId"
    profile_uuid = "testProfileUuid"
    response = test_client.get(f"/v1/{tenant_id}/profiles/{profile_uuid}/strengths")
    assert response.status_code == 200


def test_get_profile_get_to_know(test_client):
    tenant_id = "testTenantId"
    profile_uuid = "testProfileUuid"
    response = test_client.get(f"/v1/{tenant_id}/profiles/{profile_uuid}/get-to-know")
    assert response.status_code == 200


def test_get_profile_good_to_know(test_client):
    tenant_id = "testTenantId"
    profile_uuid = "testProfileUuid"
    response = test_client.get(f"/v1/{tenant_id}/profiles/{profile_uuid}/good-to-know")
    assert response.status_code == 200


def test_get_profile_work_experience(test_client):
    tenant_id = "testTenantId"
    profile_uuid = "testProfileUuid"
    response = test_client.get(
        f"/v1/{tenant_id}/profiles/{profile_uuid}/work-experience"
    )
    assert response.status_code == 200


# def test_fetch_google_meetings(test_client):
#     # Create a mock for the base class of Credentials
#     mock_base = MagicMock()
#
#     # Patch the base class of Credentials
#     with patch.object(google_credentials.Credentials, '__bases__', (mock_base,)):
#         # Now create a real Credentials instance, but with a mocked base class
#         real_credentials = google_credentials.Credentials(
#             token="fake_token",
#             refresh_token="fake_refresh_token",
#             client_id="fake_client_id",
#             client_secret="fake_client_secret",
#             token_uri="fake_token_uri"
#         )
#
#         with patch('googleapiclient.discovery.build') as mock_build, \
#                 patch('data.api.api_manager.Credentials', return_value=real_credentials):
#
#             # Mock the Google Calendar API service
#             mock_service = mock_build.return_value
#             mock_events = mock_service.events.return_value
#             mock_list = mock_events.list.return_value
#             mock_execute = mock_list.execute
#             mock_execute.return_value = {"items": []}
#
#             # Mock the repositories
#             with patch('data.data_common.repositories.google_creds_repository.GoogleCredsRepository.get_creds') as mock_get_creds, \
#                     patch('data.data_common.repositories.tenants_repository.TenantsRepository.get_tenant_id_by_email') as mock_get_tenant_id:
#
#                 mock_get_creds.return_value = {
#                     "access_token": "fake_access_token",
#                     "refresh_token": "fake_refresh_token"
#                 }
#                 mock_get_tenant_id.return_value = "fake_tenant_id"
#
#                 user_email = "test@genie.ai"
#                 response = test_client.get(f"/v1/google/meetings/{user_email}")
#
#                 assert response.status_code == 200
#                 assert "events" in response.json()
#                 assert response.json()["events"] == []
