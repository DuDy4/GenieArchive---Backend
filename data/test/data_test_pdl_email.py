import os
import json

from common.genie_logger import GenieLogger

logger = GenieLogger()

from common.utils import env_utils
from data.pdl_consumer import PDLClient
from data.data_common.dependencies.dependencies import personal_data_repository

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

pdl_key = env_utils.get("PDL_API_KEY")

event = GenieEvent(
    topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
    data=json.dumps(
        {
            "tenant_id": "d91b83dd-44bd-443d-8ed0-b41ba2779a30",
            "email": "sam@bilanc.co",
        }
    ),
    scope="public",
)
event.send()

# data = pdl_client.get_single_profile_from_email_address("danshevel@gmail.com")
# logger.info(f"Dan Shevel's data: {data}")

# data = pdl_client.get_single_profile_from_email_address("asaf.savich@kubiya.ai")
# logger.info(f"Asaf Savich's data: {data}")
