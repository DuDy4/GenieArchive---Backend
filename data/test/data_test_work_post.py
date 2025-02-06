import asyncio

from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.data_transfer_objects.work_history_dto import WorkHistoryArtifact
from common.genie_logger import GenieLogger

logger = GenieLogger()
langsmith = Langsmith()


pdl_element = {
    "title": {
        "raw": [
            "CRO"
        ],
        "name": "CRO",
        "role": None,
        "levels": [
            "CXO"
        ],
        "sub_role": None
    },
    "company": {
        "id": "Nfa8yfffvedbil9w8taeaqcrxcim",
        "raw": [
            "Rogue Fractional"
        ],
        "name": "Rogue Fractional",
        "size": "1-10",
        "type": "Private",
        "ticker": None,
        "founded": None,
        "website": None,
        "industry": None,
        "location": None,
        "fuzzy_match": False,
        "linkedin_id": None,
        "twitter_url": None,
        "facebook_url": None,
        "linkedin_url": "Linkedin.com/company/roguefractional"
    },
    "summary": "I Have Helped Many Clients with the Challenging Task of Building for the Future. Clarity About Where and WHY Clients BUY is the First Step Into Building Your GTM Strategy, Including Playbooks, Compensation Plans, Pricing Models, Lead Generation, Account Acquisition, Expansion Opportunity, and Revenue Projections. Business History Books ARE Filled with Great Ideas That Have Failed for Trying to BE Cookie Cutter Using Someone Else's Methodologies. Don't LET Yours BE Next...go Rogue and Think Outside the Box!",
    "end_date": None,
    "last_seen": "2024-03-09",
    "first_seen": "2023-07-08",
    "is_primary": True,
    "start_date": "2019-01",
    "num_sources": 4,
    "location_names": []
}

artifact = WorkHistoryArtifact.from_pdl_element(pdl_element, "profile_uuid")

# result = asyncio.run(langsmith.get_work_history_post(artifact.to_dict()))

# logger.info(result)

apollo_element = {
    "id": "66e9a3afe261900001ae1452",
    "_id": "66e9a3afe261900001ae1452",
    "key": "66e9a3afe261900001ae1452",
    "kind": None,
    "major": None,
    "title": "Director of Sales - Enterprise",
    "degree": None,
    "emails": None,
    "current": True,
    "end_date": None,
    "created_at": None,
    "start_date": "2023-04-01",
    "updated_at": None,
    "description": None,
    "grade_level": None,
    "raw_address": None,
    "organization_id": "5a9f384ba6da98d94d951b09",
    "organization_name": "mabl"
}

profile_uuid = "some-profile-uuid"
artifact = WorkHistoryArtifact.from_apollo_element(apollo_element, profile_uuid)
result = asyncio.run(langsmith.get_work_history_post(artifact.to_dict()))

logger.info(result)
