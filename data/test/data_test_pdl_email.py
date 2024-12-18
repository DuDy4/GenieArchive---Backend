import os
import json
import sys

# Get the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Append the parent directory (one level up)
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Append the grandparent directory (two levels up)
grandparent_dir = os.path.dirname(parent_dir)
sys.path.append(grandparent_dir)
from common.genie_logger import GenieLogger

logger = GenieLogger()

from common.utils import env_utils
from data.pdl_consumer import PDLClient
from data.data_common.dependencies.dependencies import personal_data_repository

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

# logger.bind_context()
# pdl_key = env_utils.get("PDL_API_KEY")
#
# event = GenieEvent(
#     topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
#     data={"tenant_id": "13eb2df3-0ae9-404c-b888-f20b1bf468b1", "email": "asaf@genieai.ai"},
# )
# event.send()

# data = pdl_client.get_single_profile_from_email_address("danshevel@gmail.com")
# logger.info(f"Dan Shevel's data: {data}")

# data = pdl_client.get_single_profile_from_email_address("asaf.savich@kubiya.ai")
# logger.info(f"Asaf Savich's data: {data}")
