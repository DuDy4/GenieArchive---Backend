import json
import traceback
from datetime import date, datetime
from typing import Optional, Union, List
import psycopg2
from common.genie_logger import GenieLogger
from common.utils.json_utils import clean_json
from pydantic import AnyUrl, ValidationError
from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.company_dto import CompanyDTO, NewsData

logger = GenieLogger()


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
            address VARCHAR,
            country VARCHAR,
            logo VARCHAR,
            founded_year INT,
            size VARCHAR,
            industry VARCHAR,
            description TEXT,
            overview TEXT,
            challenges JSONB,
            technologies JSONB,
            employees JSONB,
            social_links JSONB,
            annual_revenue VARCHAR,
            total_funding VARCHAR,
            funding_rounds JSONB,
            news JSONB,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            news_last_updated TIMESTAMP
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
        SELECT uuid, name, domain, address, country, logo, founded_year, size, industry, description, overview, challenges, technologies, employees, social_links, annual_revenue, total_funding, funding_rounds, news
        FROM companies WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                company = cursor.fetchone()
                if company:
                    logger.info(f"Got company with uuid {uuid} and name {company[1]}")
                    return CompanyDTO.from_tuple(company)
                logger.info(f"Company with uuid {uuid} does not exist")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting company: {error}")
            traceback.print_exc()
            return None

    def get_company_from_domain(self, email_domain: str) -> Optional[CompanyDTO]:
        select_query = """
        SELECT uuid, name, domain, address, country, logo, founded_year, size, industry, description, overview, challenges, technologies, employees, social_links, annual_revenue, total_funding, funding_rounds, news
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
            traceback.print_exc()
            return None

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
                if news is None:
                    logger.error(f"No news data for email: {email}, and news is null instead of empty list")
                    return []
                logger.debug(f"News data by email: {news}")
                if not news:
                    news = []  # In case news is null
                else:
                    news = news[0]  # news is a tuple containing the news data
                if len(news) > 2:
                    news = news[:2]
                res_news = self.process_news(news)
                if not res_news:
                    logger.warning(f"No news data for company with domain {company_domain}")
                    return []
                return res_news
        except psycopg2.Error as error:
            logger.error(f"Error getting news data by email: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def save_company_without_news(self, company: CompanyDTO):
        self.create_table_if_not_exists()
        if not company.uuid:
            company.uuid = get_uuid4()
        if self.exists_domain(company.domain):
            self._update(company)
            return company.uuid
        else:
            return self._insert(company)

    def get_news_last_updated(self, company_uuid):
        select_query = """
        SELECT news_last_updated FROM companies WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (company_uuid,))
                last_updated = cursor.fetchone()
                if last_updated:
                    return last_updated[0]
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting news last updated: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def get_all_companies(self):
        select_query = """
        SELECT uuid, name, domain, address, country, logo, founded_year, size, industry, description, overview, challenges, technologies, employees, social_links, annual_revenue, total_funding, funding_rounds, news
        FROM companies;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                companies = cursor.fetchall()
                if companies:
                    logger.debug(f"Got {len(companies)} companies: {companies}")
                    companies = [CompanyDTO.from_tuple(company) for company in companies]
                    logger.debug(f"Companies: {companies}")
                    return companies
                logger.info(f"No companies found")
                return []
        except psycopg2.Error as error:
            logger.error(f"Error getting companies: {error}")
            traceback.print_exc()
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return []

    def save_news(self, uuid, news: Union[List[NewsData], List[dict]]):
        self.create_table_if_not_exists()
        self.validate_news(news)
        if not news:
            logger.error(f"Invalid news data: {news}, skip saving news")
            return None
        news_dicts = [n.to_dict() if isinstance(n, NewsData) else n for n in news]
        update_query = """
        UPDATE companies
        SET news = %s, news_last_updated = CURRENT_TIMESTAMP
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (clean_json(json.dumps(news_dicts)), uuid))
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
        if news is None:
            news = []

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
                uuid, name, domain, address, country, logo, founded_year, size, industry, description, overview,
                 challenges, technologies, employees, social_links, annual_revenue, total_funding, funding_rounds
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """
        logger.info(f"About to insert company: {company_dto}")
        company_values = (
            company_dto.uuid,
            company_dto.name,
            company_dto.domain,
            company_dto.address,
            company_dto.country,
            company_dto.logo,
            company_dto.founded_year,
            company_dto.size,
            company_dto.industry,
            company_dto.description,
            company_dto.overview,
            json.dumps(company_dto.challenges),
            json.dumps(company_dto.technologies),
            json.dumps(company_dto.employees),
            json.dumps(
                [link.to_dict() for link in company_dto.social_links if not isinstance(link, dict)]
                if company_dto.social_links
                else None
            ),
            company_dto.annual_revenue,
            company_dto.total_funding,
            json.dumps(
                [round.to_dict() for round in company_dto.funding_rounds]
                if company_dto.funding_rounds
                else None
            ),
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
        if not company_dto:
            logger.error(f"Invalid company data: {company_dto}")
            return None

        fields = []
        values = []

        company_dict = company_dto.to_dict()

        # Iterate over all attributes of the CompanyDTO
        for key, value in company_dict.items():
            # Skip UUID since it's used in the WHERE clause
            if key == "uuid":
                continue

            # If the value is not None, add it to the fields and values
            if value:
                if isinstance(value, list) or isinstance(value, dict):
                    value = json.dumps(value)  # Convert lists and dicts to JSON strings
                fields.append(f"{key} = %s")
                values.append(value)

        # Add the last_updated timestamp
        fields.append("last_updated = CURRENT_TIMESTAMP")

        # Add the UUID for the WHERE clause
        logger.debug(f"Company UUID: {company_dto.uuid}")
        values.append(company_dto.uuid)

        # Construct the SQL update query dynamically
        update_query = f"""
        UPDATE companies
        SET {', '.join(fields)}
        WHERE uuid = %s
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, tuple(values))
                self.conn.commit()
                logger.info(f"Updated company in database")
            return True
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
                    logger.error(f"Error deserializing news: {e}. Skipping this news item")
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
