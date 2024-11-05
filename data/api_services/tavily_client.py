import os
import sys
from tavily import TavilyClient
from dotenv import load_dotenv
from datetime import date, datetime


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()
from data.data_common.data_transfer_objects.company_dto import NewsData

from common.genie_logger import GenieLogger
logger = GenieLogger()

tavily_client = TavilyClient(api_key=os.getenv("TAVILY_API_TOKEN","tvly-YOUR_API").strip())


class Tavily:
    def query(self, query):
        if tavily_client is None:
            logger.error("Tavily client is not initialized")
            return
        response = tavily_client.search(query)
        return response
    
    def get_news(self, topic):
        if tavily_client is None:
            logger.error("Tavily client is not initialized")
            return
        if not topic:
            logger.error("Topic is missing")
            return
        query = f"What are the lates updates about the company with domain {topic}? The domain MUST be included in the news. Only return answers with a score of 85% and above that are directly involve the company."
        # response = tavily_client.search(query, topic="news", max_results=5)
        response = tavily_client.search(query, max_results=5)

        news_list = []
        results = response.get("results", [])
        for news in results:
            if not news or not news.get("title") or not news.get("url"):
                if news:
                    logger.info(f"Missing data in news: {news}")
                continue
            logger.info(f"Found news: {news}")
            datetime_obj = datetime.strptime(news.get('published_date'), "%a, %d %b %Y %H:%M:%S %Z") if news.get('published_date') else None
            news_data = {
                "title": news.get("title"),
                "summary": news.get("content"),
                "media": "Web",
                "link": news.get("url"),
                "date": str(datetime_obj.date()) if datetime_obj else str(date.today()),
            }
            news_list.append(NewsData.from_dict(news_data))
        if not news_list:
            logger.info(f"No news found for {topic}")
        return news_list


# from data.data_common.dependencies.dependencies import companies_repository

# if __name__ == "__main__":
#     company_repository = companies_repository()
#     tavily = Tavily()
#     companies = company_repository.get_all_companies()
#     for company in companies:
#         logger.info(f"Getting news for company: {company.name}")
#         news = tavily.get_news(company.domain)
#         if news:
#             logger.info(f"Fetched News for company {company.name}")
#             # company_repository.save_news(company.uuid, news)
#         else:
#             logger.info(f"No news found for company {company.name}")
#     # print(tavily.get_news("Databricks"))