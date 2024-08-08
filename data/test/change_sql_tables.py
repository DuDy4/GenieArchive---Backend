import os
import sys
from loguru import logger
import traceback

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.data_transfer_objects.company_dto import CompanyDTO, NewsData
from data.data_common.dependencies.dependencies import (
    get_db_connection,
)

companies_repository = CompaniesRepository(conn=get_db_connection())
profiles_repository = ProfilesRepository(conn=get_db_connection())
companies_repository.create_table_if_not_exists()
profiles_repository.create_table_if_not_exists()


def update_profiles_repository_columns():
    query = """
    ALTER TABLE public.profiles DROP COLUMN news;
    """
    try:
        with profiles_repository.conn.cursor() as cursor:
            cursor.execute(query)
            profiles_repository.conn.commit()
    except Exception as error:
        logger.error(f"Error creating table: {error}")
        traceback.print_exc()


def update_companies_repository_columns():
    query = """
    ALTER TABLE public.companies ADD COLUMN news jsonb;
    """
    try:
        with companies_repository.conn.cursor() as cursor:
            cursor.execute(query)
            companies_repository.conn.commit()
    except Exception as error:
        logger.error(f"Error creating table: {error}")
        traceback.print_exc()


update_profiles_repository_columns()
update_companies_repository_columns()
