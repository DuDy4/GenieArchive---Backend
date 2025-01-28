from ..utils.postgres_connector import get_db_connection, connection_pool
from ..repositories.personal_data_repository import PersonalDataRepository
from ..repositories.persons_repository import PersonsRepository
from ..repositories.profiles_repository import ProfilesRepository
from ..repositories.tenant_profiles_repository import TenantProfilesRepository
from ..repositories.tenants_repository import TenantsRepository
from ..repositories.meetings_repository import MeetingsRepository
from ..repositories.google_creds_repository import GoogleCredsRepository
from ..repositories.ownerships_repository import OwnershipsRepository
from ..repositories.hobbies_repository import HobbiesRepository
from ..repositories.companies_repository import CompaniesRepository
from ..repositories.stats_repository import StatsRepository
from ..repositories.badges_repository import BadgesRepository
from ..repositories.file_upload_repository import FileUploadRepository
from ..repositories.deals_repository import DealsRepository
from ..repositories.statuses_repository import StatusesRepository
from ..repositories.artifacts_repository import ArtifactsRepository
from ..repositories.artifact_scores_repository import ArtifactScoresRepository
from common.genie_logger import GenieLogger

logger = GenieLogger()

t_repository = TenantsRepository()
pd_repository = PersonalDataRepository()
p_repository = PersonsRepository()
pr_repository = ProfilesRepository()
tp_repository = TenantProfilesRepository()
m_repository = MeetingsRepository()
gc_repository = GoogleCredsRepository()
o_repository = OwnershipsRepository()
h_repository = HobbiesRepository()
c_repository = CompaniesRepository()
s_repository = StatsRepository()
b_repository = BadgesRepository()
f_repository = FileUploadRepository()
d_repository = DealsRepository()
st_repository = StatusesRepository()
a_repository = ArtifactsRepository()
as_repository = ArtifactScoresRepository()


def artifacts_repository() -> ArtifactsRepository:
    return a_repository

def artifact_scores_repository() -> ArtifactScoresRepository:
    return as_repository

def tenants_repository() -> TenantsRepository:
    return t_repository


def stats_repository() -> StatsRepository:
    return s_repository

def badges_repository() -> BadgesRepository:
    return b_repository


def companies_repository() -> CompaniesRepository:
    return c_repository


def deals_repository() -> DealsRepository:
    return d_repository


def personal_data_repository() -> PersonalDataRepository:
    return pd_repository


def profiles_repository() -> ProfilesRepository:
    return pr_repository


def tenant_profiles_repository() -> TenantProfilesRepository:
    return tp_repository


def persons_repository() -> PersonsRepository:
    return p_repository


def meetings_repository() -> MeetingsRepository:
        return m_repository


def google_creds_repository() -> GoogleCredsRepository:
    return gc_repository


def ownerships_repository() -> OwnershipsRepository:
    return o_repository


def hobbies_repository() -> HobbiesRepository:
    return h_repository


def file_upload_repository() -> FileUploadRepository:
    return f_repository

def statuses_repository() -> StatusesRepository:
    return st_repository

