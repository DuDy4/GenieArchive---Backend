import json
import traceback
from typing import Optional, Union, List
import psycopg2
from loguru import logger

from common.utils.str_utils import get_uuid4

from data.data_common.data_transfer_objects.company_dto import CompanyDTO


class CompaniesRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            domain VARCHAR,
            size VARCHAR,
            description TEXT,
            overview TEXT,
            challenges JSONB,
            technologies JSONB,
            employees JSONB,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error(f"Error creating table: {error}")

    def insert(self, company_dto: CompanyDTO) -> Optional[int]:
        insert_query = """
        INSERT INTO companies (
            uuid, name, domain, size, description, overview, challenges, technologies, employees
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert company: {company_dto}")
        company_values = (
            company_dto.uuid,
            company_dto.name,
            company_dto.domain,
            company_dto.size,
            company_dto.description,
            company_dto.overview,
            json.dumps(company_dto.challenges),
            json.dumps(company_dto.technologies),
            json.dumps(company_dto.employees),
        )

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, company_values)
                self.conn.commit()
                company_id = cursor.fetchone()[0]
                logger.info(f"Inserted company into database. Company id: {company_id}")
                return company_id
        except psycopg2.Error as error:
            logger.error(f"Error inserting company: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error inserting company, because: {error.pgerror}")

    def exists(self, domain: str) -> bool:
        logger.info(f"About to check if company exists with domain: {domain}")
        exists_query = "SELECT 1 FROM companies WHERE domain = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (domain,))
                result = cursor.fetchone() is not None
                logger.info(f"{domain} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of uuid {domain}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def get_company(self, uuid: str) -> Optional[CompanyDTO]:
        select_query = """
        SELECT uuid, name, domain, size, description, overview, challenges, technologies, employees
        FROM companies WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                company = cursor.fetchone()
                if company:
                    logger.info(f"Got company with uuid {uuid}")
                    return CompanyDTO.from_tuple(company)
                logger.info(f"Company with uuid {uuid} does not exist")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting company: {error}")
            traceback.print_exc()
            return None

    def get_company_from_domain(self, email_domain: str) -> Optional[CompanyDTO]:
        select_query = """
        SELECT uuid, name, domain, size, description, overview, challenges, technologies, employees
        FROM companies WHERE domain = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email_domain,))
                company = cursor.fetchone()
                if company:
                    logger.info(f"Got company with domain {email_domain}")
                    return CompanyDTO.from_tuple(company)
                logger.info(f"Company with domain {email_domain} does not exist")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting company: {error}")
            traceback.print_exc()
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def update(self, company_dto: CompanyDTO):
        update_query = """
        UPDATE companies
        SET name = %s, domain = %s, size = %s, description = %s, overview = %s, challenges = %s, technologies = %s, employees = %s, last_updated = CURRENT_TIMESTAMP
        WHERE uuid = %s
        """
        company_values = (
            company_dto.name,
            company_dto.domain,
            company_dto.size,
            company_dto.description,
            company_dto.overview,
            json.dumps(company_dto.challenges),
            json.dumps(company_dto.technologies),
            json.dumps(company_dto.employees),
            company_dto.uuid,
        )
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, company_values)
                self.conn.commit()
                logger.info(f"Updated company in database")
        except psycopg2.Error as error:
            raise Exception(f"Error updating company, because: {error.pgerror}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def save_company(self, company: CompanyDTO):
        self.create_table_if_not_exists()
        if not company.uuid:
            company.uuid = get_uuid4()
        if self.exists(company.domain):
            self.update(company)
            return company.uuid
        else:
            return self.insert(company)

    def process_employee_data(self, employees: Union[str, List[dict]]) -> List[dict]:
        result_employee_data = []
        logger.info(f"Processing employees: {employees}")
        if not employees:
            return []
        for employee in employees:
            if not employee.get("first_name"):
                if employee.get("name"):
                    result_employee_data.append(employee)
                logger.info(f"Skipping employee: {employee}")
                continue
            name = employee.get("first_name") + " " + employee.get("last_name")
            email = employee.get("value")
            position = employee.get("position")
            linkedin = employee.get("linkedin")
            department = employee.get("department")
            result_employee_data.append(
                {
                    "name": name,
                    "email": email,
                    "position": position,
                    "linkedin": linkedin,
                    "department": department,
                }
            )
        logger.info(f"Processed employees: {result_employee_data}")
        return result_employee_data
