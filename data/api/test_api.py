import os
import traceback
import datetime

import requests
from fastapi import Depends, Request, HTTPException
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import RedirectResponse, JSONResponse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build

from data.data_common.repositories.google_creds_repository import GoogleCredsRepository
from data.data_common.dependencies.dependencies import google_creds_repository

from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

SELF_URL = os.environ.get("SELF_URL", "https://localhost:8002")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID_DAN")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET_DAN")
REDIRECT_URI = f"{SELF_URL}/v1/google-callback"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")


@v1_router.get("/oauth")
async def oauth(request: Request):
    authorization_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        "?response_type=code"
        f"&client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&scope=https://www.googleapis.com/auth/calendar"
        "&access_type=offline"
        "&prompt=consent"
    )
    return RedirectResponse(url=authorization_url)


@v1_router.get("/google-callback")
async def callback(
    request: Request,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
):
    code = request.query_params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="Code not found in request")

    token_url = "https://oauth2.googleapis.com/token"
    token_data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }

    token_response = requests.post(token_url, data=token_data)
    if token_response.status_code != 200:
        logger.error(f"Token request failed: {token_response.text}")
        raise HTTPException(status_code=400, detail="Failed to fetch token")

    tokens = token_response.json()
    logger.debug(f"Tokens: {tokens}")
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")

    # Store tokens in the database
    tenant_id = "asaf-savich"  # Replace with actual tenant ID

    google_creds_repository.insert(
        {
            "uuid": "example-uuid",
            "tenantId": tenant_id,
            "accessToken": access_token,
            "refreshToken": refresh_token,
        }
    )

    return JSONResponse(
        content={"message": "OAuth flow completed", "access token": access_token}
    )


@v1_router.get("/{tenant_id}/meetings", response_class=JSONResponse)
def get_all_meetings(
    tenant_id: str,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
) -> JSONResponse:
    logger.info(f"Got events request for tenant: {tenant_id}")

    google_credentials = google_creds_repository.get_creds(tenant_id)
    if not google_credentials:
        raise HTTPException(
            status_code=404, detail="Google credentials not found for the tenant"
        )

    google_credentials = Credentials(
        token=google_credentials["access_token"],
        refresh_token=google_credentials["refresh_token"],
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        token_uri=GOOGLE_TOKEN_URI,
    )

    google_credentials.refresh(GoogleRequest())
    logger.debug(f"Google credentials: {google_credentials}")

    google_creds_repository.update_creds(
        {
            "tenant_id": tenant_id,
            "access_token": google_credentials.token,
            "refresh_token": google_credentials.refresh_token,
        }
    )

    access_token = google_credentials.token

    credentials = Credentials(token=access_token)
    service = build("calendar", "v3", credentials=credentials)

    now = datetime.datetime.utcnow().isoformat() + "Z"  # 'Z' indicates UTC time
    logger.info(f"Fetching meetings starting from: {now}")

    events_result = (
        service.events()
        .list(
            calendarId="primary",
            timeMin=now,
            maxResults=30,
            singleEvents=True,
            orderBy="startTime",
        )
        .execute()
    )

    meetings = events_result.get("items", [])
    logger.info(f"Fetched events: {meetings}")

    if not meetings:
        return JSONResponse(content={"message": "No upcoming events found."})

    for meeting in meetings:

        meeting = MeetingDTO.from_google_calendar_event(meeting, tenant_id)

        event = GenieEvent(
            topic=Topic.NEW_MEETING, data=meeting.to_json(), scope="public"
        )
        event.send()

    return JSONResponse(content={"events": meetings})
