import json
import traceback
from typing import Optional, Union, List
import psycopg2
from loguru import logger

from common.utils.str_utils import get_uuid4


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
            description TEXT,
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

    def insert(self, company_data: dict) -> Optional[int]:
        insert_query = """
        INSERT INTO companies (
            uuid, name, domain, description, technologies, employees
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert company: {company_data}")
        company_values = (
            company_data.get("uuid"),
            company_data.get("organization"),
            company_data.get("domain"),
            company_data.get("description"),
            json.dumps(company_data.get("technologies")),
            json.dumps(company_data.get("employees")),
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

    def get_company(self, uuid: str) -> Optional[dict]:
        select_query = """
        SELECT * FROM companies WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                company = cursor.fetchone()
                if company:
                    logger.info(f"Got company with uuid {uuid}")
                    return {
                        "id": company[0],
                        "uuid": company[1],
                        "name": company[2],
                        "domain": company[3],
                        "description": company[4],
                        "technologies": company[5],
                        "employees": company[6],
                    }
                logger.info(f"Company with uuid {uuid} does not exist")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting company: {error}")
            traceback.print_exc()
            return None

    def get_company_from_domain(self, email_domain):
        select_query = """
        SELECT * FROM companies WHERE domain = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email_domain,))
                company = cursor.fetchone()
                if company:
                    logger.info(f"Got company with domain {email_domain}")
                    return {
                        "id": company[0],
                        "uuid": company[1],
                        "name": company[2],
                        "domain": company[3],
                        "description": company[4],
                        "technologies": company[5],
                        "employees": company[6],
                    }
                logger.info(f"Company with domain {email_domain} does not exist")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting company: {error}")
            traceback.print_exc()
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def update(self, company_data: dict):
        update_query = """
        UPDATE companies
        SET name = %s, domain = %s, description = %s, technologies = %s, employees = %s
        WHERE uuid = %s
        """
        company_values = (
            company_data.get("name"),
            company_data.get("domain"),
            company_data.get("description"),
            json.dumps(company_data.get("technologies")),
            json.dumps(company_data.get("employees")),
            company_data.get("uuid"),
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

    def save_company(self, company_data: dict):
        self.create_table_if_not_exists()
        logger.info(f"About to save company: {company_data}")
        if not company_data.get("uuid"):
            company_data["uuid"] = get_uuid4()
        if company_data.get("emails"):
            company_data["employees"] = self.process_employee_data(
                company_data.get("emails")
            )
            company_data.pop("emails")
        if self.exists(company_data.get("domain")):
            self.update(company_data)
            return company_data.get("uuid")
        else:
            return self.insert(company_data)

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
