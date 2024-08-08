import json
import traceback
from datetime import date, datetime
from typing import Optional, Union, List
import psycopg2
from loguru import logger
from pydantic import AnyUrl, ValidationError

from common.utils.str_utils import get_uuid4

from data.data_common.data_transfer_objects.company_dto import CompanyDTO, NewsData


def _process_employee_data(employees: Union[str, List[dict]]) -> List[dict]:
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
            news JSONB,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error(f"Error creating table: {error}")

    def get_company(self, uuid: str) -> Optional[CompanyDTO]:
        select_query = """
        SELECT uuid, name, domain, size, description, overview, challenges, technologies, employees, news
        FROM companies WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                company = cursor.fetchone()
                logger.debug(f"Company: {company}")
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
        SELECT uuid, name, domain, size, description, overview, challenges, technologies, employees, news
        FROM companies WHERE domain = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email_domain,))
                company = cursor.fetchone()
                if company:
                    logger.info(f"Got company with domain {email_domain}")
                    news = company[9]
                    if not news:
                        logger.info(
                            f"No news data for company with domain {email_domain}"
                        )
                        news = []
                        company = company[:9] + ([],)
                    logger.debug(f"News data: {news}")
                    valid_news = []
                    for news_item in news:
                        try:
                            if isinstance(news_item, dict):
                                news_item = NewsData.from_dict(news_item)
                                logger.debug(f"Deserialized news: {news_item}")
                                valid_news.append(news_item)
                        except ValidationError:
                            logger.error(f"Invalid news item: {news_item}")
                    logger.debug(f"Valid news: {valid_news}")
                    company = company[:9] + (valid_news,)
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

    def get_news(self, company_uuid):
        select_query = """
        SELECT news FROM companies WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (company_uuid,))
                news = cursor.fetchone()
                logger.debug(f"News data by email: {news}")
                news = news[0]  # news is a tuple containing the news data
                if len(news) > 2:
                    news = news[:2]
                res_news = self.process_news(news)
                if not res_news:
                    logger.warning(
                        f"No news data for company with domain {company_domain}"
                    )
                    return []
                return res_news
        except psycopg2.Error as error:
            logger.error(f"Error getting news data: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return

    def get_news_data_by_email(self, email):
        if "@" not in email:
            logger.error(f"Invalid email: {email}")
            return None
        company_domain = email.split("@")[1]
        logger.info(f"Company domain: {company_domain}, email: {email}")
        query = """
        SELECT news FROM companies WHERE domain = %s;
        """
        try:
            with (self.conn.cursor() as cursor):
                cursor.execute(query, (company_domain,))
                news = cursor.fetchone()
                logger.debug(f"News data by email: {news}")
                news = news[0]  # news is a tuple containing the news data
                if len(news) > 2:
                    news = news[:2]
                res_news = self.process_news(news)
                if not res_news:
                    logger.warning(
                        f"No news data for company with domain {company_domain}"
                    )
                    return []
                return res_news
        except psycopg2.Error as error:
            logger.error(f"Error getting news data by email: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def save_company(self, company: CompanyDTO):
        self.create_table_if_not_exists()
        if not company.uuid:
            company.uuid = get_uuid4()
        if self.exists_domain(company.domain):
            self._update(company)
            return company.uuid
        else:
            return self._insert(company)

    def save_news(self, uuid, news: Union[List[NewsData], List[dict]]):
        self.create_table_if_not_exists()
        self.validate_news(news)
        if not news:
            logger.error(f"Invalid news data: {news}, skip saving news")
            return None
        news_dicts = [n.to_dict() if isinstance(n, NewsData) else n for n in news]
        update_query = """
        UPDATE companies
        SET news = %s, last_updated = CURRENT_TIMESTAMP
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (json.dumps(news_dicts), uuid))
                self.conn.commit()
                logger.info(f"Updated news in database")
        except psycopg2.Error as error:
            raise Exception(f"Error updating news, because: {error.pgerror}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def save_news_by_email(self, email, news):
        if "@" not in email:
            logger.error(f"Invalid email: {email}")
            return None
        company_domain = email.split("@")[1]
        self.validate_news(news)

        query = """
        UPDATE companies
        SET news = %s, last_updated = CURRENT_TIMESTAMP
        WHERE domain = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (json.dumps(news), company_domain))
                self.conn.commit()
                logger.info(f"Updated news by email: {email}")
        except psycopg2.Error as error:
            logger.error(f"Error updating news by email: {error}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if company exists with uuid: {uuid}")
        exists_query = "SELECT 1 FROM companies WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (uuid,))
                result = cursor.fetchone() is not None
                logger.info(f"{uuid} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of company {uuid}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def exists_domain(self, domain: str) -> bool:
        logger.info(f"About to check if company exists with domain: {domain}")
        exists_query = "SELECT 1 FROM companies WHERE domain = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (domain,))
                result = cursor.fetchone() is not None
                logger.info(f"{domain} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of domain {domain}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def delete_company(self, uuid):
        delete_query = """
        DELETE FROM companies WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (uuid,))
                self.conn.commit()
                logger.info(f"Deleted company with uuid: {uuid}")
        except psycopg2.Error as error:
            logger.error(f"Error deleting company: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def _insert(self, company_dto: CompanyDTO) -> Optional[int]:
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

    def _update(self, company_dto: CompanyDTO):
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

    @staticmethod
    def serialize_news(news: Union[NewsData, dict]) -> dict[str]:
        if isinstance(news, NewsData):
            news = news.to_dict()
        if news.get("date") and isinstance(news["date"], date):
            news["date"] = news["date"].isoformat()
        if news.get("link") and isinstance(news["link"], AnyUrl):
            news["link"] = str(news["link"])
        return news

    # @staticmethod
    # def deserialize_news(news: dict) -> NewsData | None:
    #     try:
    #         if news.get("date"):
    #             logger.debug(f"Date: {news['date']}, type: {type(news['date'])}")
    #             if isinstance(news["date"], str):
    #                 try:
    #                     logger.debug(f"Attempting to convert date string: {news['date']}")
    #                     news["date"] = date.fromisoformat(news["date"])  # Convert string back to date
    #                     logger.debug(f"Converted date: {news['date']}, type: {type(news['date'])}")
    #                 except ValueError as ve:
    #                     logger.error(f"ValueError in fromisoformat: {ve}")
    #                     return None
    #             else:
    #                 logger.error(f"Date field is not a string: {news['date']}")
    #         if news.get("link"):
    #             logger.debug(f"Link: {news['link']}, type: {type(news['link'])}")
    #             try:
    #                 news["link"] = AnyUrl(news["link"])
    #                 logger.debug(f"Converted link: {news['link']}, type: {type(news['link'])}")
    #             except ValidationError as ve:
    #                 logger.error(f"ValidationError for link: {ve}")
    #                 return None
    #     except Exception as e:
    #         logger.error(f"Error deserializing news: {e}")
    #         return None
    #     logger.debug(f"Deserialized news: {news}")
    #     return NewsData.from_dict(news)

    @staticmethod
    def process_news(news: List[dict]) -> List[NewsData]:
        logger.debug(f"News data: {news}")
        res_news = []
        if news:
            for item in news:
                logger.debug(f"Item: {item}")
                try:
                    deserialized_news = NewsData.from_dict(item)
                    logger.debug(f"Deserialized news: {deserialized_news}")
                    if deserialized_news:
                        res_news.append(deserialized_news)
                    logger.debug(f"Processed news: {res_news}")
                except Exception as e:
                    logger.error(
                        f"Error deserializing news: {e}. Skipping this news item"
                    )
        logger.debug(f"News data: {res_news}")
        return res_news

    @staticmethod
    def validate_news(news):
        if not news:
            return []
        logger.debug(f"Validating news: {news}")
        i = 0
        while i < len(news):
            if isinstance(news[i], dict):
                try:
                    news[i] = NewsData.from_dict(news[i])
                except Exception as e:
                    logger.error(f"Error converting news to NewsData: {e}")
                    news.pop(i)
                    i -= 1
                finally:
                    i += 1
            elif isinstance(news[i], NewsData):
                i += 1
        return news

    @staticmethod
    def json_serializer(obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, AnyUrl):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")
