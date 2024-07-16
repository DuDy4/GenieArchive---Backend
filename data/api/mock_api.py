import os
import traceback

import requests
from fastapi import Depends, FastAPI, Request, HTTPException
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse, JSONResponse

from redis import Redis

SELF_URL = os.environ.get("PERSON_URL", "https://localhost:8001")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")

redis_client = Redis(host="localhost", port=6379, db=0)

meetings = [
    {
        "meeting_uuid": "65b5afe8",
        "google_calender_id": "d02e29",
        "tenant_id": "abcde",
        "link": "https://meet.google.com/bla-bla-bla",
        "subject": "Intro Me <> You",
        "start_time": "2024-07-27T17:00:00+03:00",
        "end_time": "2024-07-27T17:30:00+03:00",
    },
    {
        "meeting_uuid": "65b5afe9",
        "google_calender_id": "d02e30",
        "tenant_id": "abcde",
        "link": "https://meet.google.com/bla-bla-bla2",
        "subject": "Second intro Me <> You",
        "start_time": "2024-07-24T16:00:00+03:00",
        "end_time": "2024-07-24T17:30:00+03:00",
    },
    {
        "meeting_uuid": "65b5afd0",
        "google_calender_id": "d02e31",
        "tenant_id": "abcde",
        "link": "https://meet.google.com/bla-bla-bla3",
        "subject": "Hackathon",
        "start_time": "2024-07-30",
        "end_time": "2024-07-31",
    },
]

profiles = [
    {
        "uuid": "d91b83dd-44bd-443d-8ed0-b41ba2779a30",
        "tenant_id": "abcde",
        "name": "Asaf Savich",
        "company": "GenieAI",
        "position": "CTO",
        "engagement_level": "65",
        "profile_certainty": "85",
        "strengths": [
            {"strength_name": "Leadership", "strength_level": "80"},
            {"strength_name": "Communication", "strength_level": "90"},
            {"strength_name": "Problem Solving", "strength_level": "70"},
            {"strength_name": "Teamwork", "strength_level": "85"},
            {"strength_name": "Technical Skills", "strength_level": "75"},
            {"strength_name": "Creativity", "strength_level": "80"},
            {"strength_name": "Adaptability", "strength_level": "85"},
            {"strength_name": "Work Ethic", "strength_level": "90"},
        ],
        "good_to_know": {
            "connections": [
                {
                    "name": "John Doe",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
                {
                    "name": "Jane Doe",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
                {
                    "name": "John Smith",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
                {
                    "name": "Jane Smith",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
            ],
            "hobbies": [
                {
                    "hobby_name": "Skiing",
                    "image_url": "https://img.icons8.com/dusk/64/skiing.png",
                },
                {
                    "hobby_name": "Reading",
                    "image_url": "https://img.icons8.com/color/48/reading.png",
                },
            ],
            "news": [
                {
                    "news_title": "New AI breakthrough",
                    "news_url": "https://www.bbc.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
                {
                    "news_title": "AI is a fraud",
                    "news_url": "https://www.bbc-lies.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
                {
                    "news_title": "Asaf announced something important",
                    "news_url": "https://www.linkedin.com/some-id-4321",
                    "news_icon": "https://img.icons8.com/color/48/linkedin.png",
                },
            ],
        },
        "get-to-know": {
            "title": "Asaf is a great guy",
            "phrases_to_use": [
                "He is a great leader",
                "He is very creative",
                "He is a great team player",
            ],
            "best_practices": [
                "Ask him about his work",
                "Ask him about his hobbies",
                "Ask him about his family",
            ],
            "avoid": [
                "Avoid talking about politics",
                "Avoid talking about religion",
                "Avoid talking about his ex",
            ],
        },
        "picture_url": "https://img.icons8.com/officel/16/test-account.png",
        "work_experience": [
            {"company": "GenieAI", "position": "CTO", "start_date": "2024-03-01"},
            {"company": "Kubiya.ai", "position": "CTO", "start_date": "2023-01-01"},
            {
                "company": "WhiteSource",
                "position": "Head of everything",
                "start_date": "2016-01-01",
            },
        ],
    },
    {
        "uuid": "5d73ec10-4a5b-4d8c-8d28-626ed503d87e",
        "tenant_id": "abcde",
        "name": "Emma Johnson",
        "company": "InnovateTech",
        "position": "CEO",
        "engagement_level": "75",
        "profile_certainty": "90",
        "strengths": [
            {"strength_name": "Visionary", "strength_level": "95"},
            {"strength_name": "Communication", "strength_level": "85"},
            {"strength_name": "Strategic Thinking", "strength_level": "90"},
            {"strength_name": "Team Building", "strength_level": "80"},
            {"strength_name": "Problem Solving", "strength_level": "75"},
            {"strength_name": "Adaptability", "strength_level": "85"},
            {"strength_name": "Decision Making", "strength_level": "90"},
        ],
        "good_to_know": {
            "connections": [
                {
                    "name": "Alice Brown",
                    "image_url": "https://img.icons8.com/fluency/48/person-female.png",
                },
                {
                    "name": "Bob Smith",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
                {
                    "name": "Carol White",
                    "image_url": "https://img.icons8.com/fluency/48/person-female.png",
                },
            ],
            "hobbies": [
                {
                    "hobby_name": "Hiking",
                    "image_url": "https://img.icons8.com/dusk/64/hiking.png",
                },
                {
                    "hobby_name": "Painting",
                    "image_url": "https://img.icons8.com/color/48/painting.png",
                },
            ],
            "news": [
                {
                    "news_title": "InnovateTech Launches New Product",
                    "news_url": "https://www.techcrunch.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
                {
                    "news_title": "Emma Johnson in Forbes 30 Under 30",
                    "news_url": "https://www.forbes.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
            ],
        },
        "get-to-know": {
            "title": "Emma is a visionary leader",
            "phrases_to_use": [
                "She has a clear vision",
                "She communicates effectively",
                "She builds strong teams",
            ],
            "best_practices": [
                "Discuss industry trends",
                "Ask about her vision for the company",
                "Engage in strategic discussions",
            ],
            "avoid": [
                "Avoid micromanagement",
                "Avoid doubting her decisions",
                "Avoid unnecessary formalities",
            ],
        },
        "picture_url": "https://img.icons8.com/officel/16/test-account.png",
        "work_experience": [
            {
                "company": "InnovateTech",
                "position": "CEO",
                "start_date": "2022-01-01",
            },
            {
                "company": "TechDynamics",
                "position": "COO",
                "start_date": "2019-05-01",
            },
            {
                "company": "FutureSolutions",
                "position": "Product Manager",
                "start_date": "2015-09-01",
            },
        ],
    },
    {
        "uuid": "34b8bc3a-529c-48db-93b1-d1b2a5d8bcb8",
        "tenant_id": "abcde",
        "name": "David Lee",
        "company": "TechWave",
        "position": "Lead Developer",
        "engagement_level": "80",
        "profile_certainty": "88",
        "strengths": [
            {"strength_name": "Technical Skills", "strength_level": "95"},
            {"strength_name": "Problem Solving", "strength_level": "85"},
            {"strength_name": "Creativity", "strength_level": "80"},
            {"strength_name": "Collaboration", "strength_level": "75"},
            {"strength_name": "Leadership", "strength_level": "70"},
            {"strength_name": "Work Ethic", "strength_level": "90"},
        ],
        "good_to_know": {
            "connections": [
                {
                    "name": "Ella Wong",
                    "image_url": "https://img.icons8.com/fluency/48/person-female.png",
                },
                {
                    "name": "Frank Harris",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
            ],
            "hobbies": [
                {
                    "hobby_name": "Gaming",
                    "image_url": "https://img.icons8.com/dusk/64/gaming.png",
                },
                {
                    "hobby_name": "Cooking",
                    "image_url": "https://img.icons8.com/color/48/cooking.png",
                },
            ],
            "news": [
                {
                    "news_title": "TechWave Launches New App",
                    "news_url": "https://www.techradar.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
                {
                    "news_title": "David Lee on Top Developers List",
                    "news_url": "https://www.devweekly.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
            ],
        },
        "get-to-know": {
            "title": "David is a tech enthusiast",
            "phrases_to_use": [
                "He is very skilled technically",
                "He loves solving problems",
                "He is very hardworking",
            ],
            "best_practices": [
                "Discuss latest tech trends",
                "Ask about his recent projects",
                "Engage in problem-solving discussions",
            ],
            "avoid": [
                "Avoid non-technical topics",
                "Avoid lengthy meetings",
                "Avoid micromanagement",
            ],
        },
        "picture_url": "https://img.icons8.com/officel/16/test-account.png",
        "work_experience": [
            {
                "company": "TechWave",
                "position": "Lead Developer",
                "start_date": "2020-06-01",
            },
            {
                "company": "CodeMasters",
                "position": "Senior Developer",
                "start_date": "2017-03-01",
            },
            {
                "company": "DevSolutions",
                "position": "Developer",
                "start_date": "2014-09-01",
            },
        ],
    },
    {
        "uuid": "c88e5d2b-8dbb-4b5e-bbd8-e2b94609b9a6",
        "tenant_id": "abcde",
        "name": "Sophia Miller",
        "company": "MarketPro",
        "position": "Marketing Director",
        "engagement_level": "85",
        "profile_certainty": "92",
        "strengths": [
            {"strength_name": "Communication", "strength_level": "90"},
            {"strength_name": "Creativity", "strength_level": "85"},
            {"strength_name": "Strategic Thinking", "strength_level": "80"},
            {"strength_name": "Team Building", "strength_level": "75"},
            {"strength_name": "Adaptability", "strength_level": "80"},
            {"strength_name": "Analytical Skills", "strength_level": "85"},
        ],
        "good_to_know": {
            "connections": [
                {
                    "name": "George Clark",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
                {
                    "name": "Hannah Scott",
                    "image_url": "https://img.icons8.com/fluency/48/person-female.png",
                },
            ],
            "hobbies": [
                {
                    "hobby_name": "Traveling",
                    "image_url": "https://img.icons8.com/dusk/64/travel.png",
                },
                {
                    "hobby_name": "Photography",
                    "image_url": "https://img.icons8.com/color/48/photography.png",
                },
            ],
            "news": [
                {
                    "news_title": "MarketPro Wins Marketing Award",
                    "news_url": "https://www.marketingweek.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
                {
                    "news_title": "Sophia Miller on Creative Marketing",
                    "news_url": "https://www.creativereview.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
            ],
        },
        "get-to-know": {
            "title": "Sophia is a creative marketer",
            "phrases_to_use": [
                "She communicates effectively",
                "She has creative ideas",
                "She thinks strategically",
            ],
            "best_practices": [
                "Discuss marketing strategies",
                "Ask about her creative process",
                "Engage in brainstorming sessions",
            ],
            "avoid": [
                "Avoid generic marketing topics",
                "Avoid dismissing her ideas",
                "Avoid being overly critical",
            ],
        },
        "picture_url": "https://img.icons8.com/officel/16/test-account.png",
        "work_experience": [
            {
                "company": "MarketPro",
                "position": "Marketing Director",
                "start_date": "2021-04-01",
            },
            {
                "company": "AdStar",
                "position": "Marketing Manager",
                "start_date": "2018-02-01",
            },
            {
                "company": "BrandBoost",
                "position": "Marketing Specialist",
                "start_date": "2015-08-01",
            },
        ],
    },
    {
        "uuid": "e1f85b2d-9938-41e2-bdb9-c12555d1b5df",
        "tenant_id": "abcde",
        "name": "Michael Brown",
        "company": "DataWorks",
        "position": "Data Scientist",
        "engagement_level": "70",
        "profile_certainty": "87",
        "strengths": [
            {"strength_name": "Analytical Skills", "strength_level": "90"},
            {"strength_name": "Problem Solving", "strength_level": "80"},
            {"strength_name": "Technical Skills", "strength_level": "85"},
            {"strength_name": "Communication", "strength_level": "75"},
            {"strength_name": "Creativity", "strength_level": "70"},
            {"strength_name": "Collaboration", "strength_level": "80"},
        ],
        "good_to_know": {
            "connections": [
                {
                    "name": "Isla Green",
                    "image_url": "https://img.icons8.com/fluency/48/person-female.png",
                },
                {
                    "name": "Jack White",
                    "image_url": "https://img.icons8.com/fluency/48/person-male.png",
                },
            ],
            "hobbies": [
                {
                    "hobby_name": "Cycling",
                    "image_url": "https://img.icons8.com/dusk/64/cycling.png",
                },
                {
                    "hobby_name": "Gardening",
                    "image_url": "https://img.icons8.com/color/48/gardening.png",
                },
            ],
            "news": [
                {
                    "news_title": "DataWorks Achieves Record Growth",
                    "news_url": "https://www.datainsider.com",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
                {
                    "news_title": "Michael Brown on Data Science Trends",
                    "news_url": "https://www.datascienceweekly.org",
                    "news_icon": "https://img.icons8.com/bubbles/50/news.png",
                },
            ],
        },
        "get-to-know": {
            "title": "Michael is a data expert",
            "phrases_to_use": [
                "He is very analytical",
                "He solves complex problems",
                "He collaborates well",
            ],
            "best_practices": [
                "Discuss data analysis",
                "Ask about his recent projects",
                "Engage in technical discussions",
            ],
            "avoid": [
                "Avoid non-technical topics",
                "Avoid interrupting his work",
                "Avoid being vague",
            ],
        },
        "picture_url": "https://img.icons8.com/officel/16/test-account.png",
        "work_experience": [
            {
                "company": "DataWorks",
                "position": "Data Scientist",
                "start_date": "2019-07-01",
            },
            {
                "company": "Insight Analytics",
                "position": "Data Analyst",
                "start_date": "2016-10-01",
            },
            {
                "company": "Quantify",
                "position": "Junior Data Analyst",
                "start_date": "2014-01-01",
            },
        ],
    },
]


@v1_router.get("/profiles/{meeting_id}/{tenant_id}", response_class=JSONResponse)
def get_all_profile_ids_for_meeting(
    tenant_id: str,
    meeting_id: str,
) -> JSONResponse:
    """
    Get all profile IDs for a specific meeting - Mock version.
    (For the mock version - the right tenant_id is 'abcde',
     and there is no filtering by meeting_id - every meeting gets all profiles.)

    - **tenant_id**: Tenant ID - the right one is 'abcde'
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Got profiles request for meeting: {meeting_id}")

    response = []
    for profile in profiles:
        if tenant_id == profile["tenant_id"]:
            response.append({"id": profile["uuid"], "name": profile["name"]})
    return JSONResponse(content=response)


@v1_router.get("/profile/{tenant_id}/{uuid}/strengths", response_class=JSONResponse)
def get_profile_strengths(
    uuid: str,
    tenant_id: str,
) -> JSONResponse:
    """
    Get the strengths of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got strengths request for profile: {uuid}")

    for profile in profiles:
        if uuid == profile["uuid"] and tenant_id == profile["tenant_id"]:
            return JSONResponse(content=profile["strengths"])
    return JSONResponse(content=[])


@v1_router.get("/profile/{tenant_id}/{uuid}/get-to-know", response_class=JSONResponse)
def get_profile_get_to_know(
    uuid: str,
    tenant_id: str,
) -> JSONResponse:
    """
    Get the 'get-to-know' information of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got get-to-know request for profile: {uuid}")

    for profile in profiles:
        if uuid == profile["uuid"] and tenant_id == profile["tenant_id"]:
            return JSONResponse(content=profile["get-to-know"])
    return JSONResponse(content={})


@v1_router.get("/profile/{tenant_id}/{uuid}/connections", response_class=JSONResponse)
def get_profile_connections(
    uuid: str,
    tenant_id: str,
) -> JSONResponse:
    """
    Get the connections of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got connections request for profile: {uuid}")

    for profile in profiles:
        if uuid == profile["uuid"] and tenant_id == profile["tenant_id"]:
            return JSONResponse(content=profile["good_to_know"]["connections"])
    return JSONResponse(content=[])


@v1_router.get("/profile/{tenant_id}/{uuid}/hobbies", response_class=JSONResponse)
def get_profile_hobbies(
    uuid: str,
    tenant_id: str,
) -> JSONResponse:
    """
    Get the hobbies of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got hobbies request for profile: {uuid}")

    for profile in profiles:
        if uuid == profile["uuid"] and tenant_id == profile["tenant_id"]:
            return JSONResponse(content=profile["good_to_know"]["hobbies"])
    return JSONResponse(content=[])


@v1_router.get("/profile/{tenant_id}/{uuid}/news", response_class=JSONResponse)
def get_profile_news(
    uuid: str,
    tenant_id: str,
) -> JSONResponse:
    """
    Get the news of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got news request for profile: {uuid}")

    for profile in profiles:
        if uuid == profile["uuid"] and tenant_id == profile["tenant_id"]:
            return JSONResponse(content=profile["good_to_know"]["news"])
    return JSONResponse(content=[])


@v1_router.get(
    "/profile/{tenant_id}/{uuid}/work-experience", response_class=JSONResponse
)
def get_profile_work_experience(
    uuid: str,
    tenant_id: str,
) -> JSONResponse:
    """
    Get the work experience of a profile - *Mock version*.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got work experience request for profile: {uuid}")

    for profile in profiles:
        if uuid == profile["uuid"] and tenant_id == profile["tenant_id"]:
            return JSONResponse(content=profile["work_experience"])
    return JSONResponse(content=[])


@v1_router.get("/meetings/{tenant_id}", response_class=JSONResponse)
def get_all_meetings(
    tenant_id: str,
) -> JSONResponse:
    """
    Get all meetings for a specific tenant - Mock version.

    - **tenant_id**: Tenant ID
    """
    logger.info(f"Got meetings request for tenant: {tenant_id}")
    if tenant_id != "abcde":
        return JSONResponse(content=[])
    return JSONResponse(content=meetings)
