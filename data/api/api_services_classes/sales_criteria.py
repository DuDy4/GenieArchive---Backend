from common.utils import email_utils
from common.utils.job_utils import fix_and_sort_experience_from_pdl, fix_and_sort_experience_from_apollo
from data.api.base_models import *
from data.data_common.utils.persons_utils import determine_profile_category
from data.data_common.dependencies.dependencies import (
    profiles_repository,
    tenant_profiles_repository,
    ownerships_repository,
    meetings_repository,
    tenants_repository,
    persons_repository,
    personal_data_repository,
    companies_repository,
    hobbies_repository,
)
from fastapi import HTTPException

from common.genie_logger import GenieLogger
from data.data_common.utils.str_utils import to_custom_title_case

logger = GenieLogger()


class ProfilesApiService:
    def __init__(self):
        self.profiles_repository = profiles_repository()
        self.tenant_profiles_repository = tenant_profiles_repository()



