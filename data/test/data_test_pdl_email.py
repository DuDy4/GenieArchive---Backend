import os

from loguru import logger

from data.pdl import PDLClient
from data.data_common.dependencies.dependencies import personal_data_repository

pdl_key = os.environ.get("PDL_API_KEY")

pdl_client = PDLClient(
    api_key=pdl_key, personal_data_repository=personal_data_repository()
)

data = pdl_client.get_single_profile_from_email_address("danshevel@gmail.com")
logger.info(f"Dan Shevel's data: {data}")


data = pdl_client.get_single_profile_from_email_address("asaf.savich@kubiya.ai")
logger.info(f"Asaf Savich's data: {data}")
