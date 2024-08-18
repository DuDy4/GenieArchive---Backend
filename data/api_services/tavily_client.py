import os
import sys
from tavily import TavilyClient
from dotenv import load_dotenv
from datetime import date


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
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
        query = f"What are the lates updates about {topic}? Only return answers with a score of 0.8 and above
"
        response = tavily_client.search(query)

        news_list = []
        results = response.get("results", [])
        for news in results:
            if not news or not news.get("title") or not news.get("url"):
                if news:
                    logger.info(f"Missing data in news: {news}")
                continue
            news_data = {
                "title": news.get("title"),
                "summary": news.get("content"),
                "media": "Web",
                "link": news.get("url"),
                "date": str(date.today()),
            }
            news_list.append(NewsData.from_dict(news_data))
        if not news_list:
            logger.info(f"No news found for {topic}")
    return news_list


if __name__ == "__main__":
    tavily = Tavily()
    print(tavily.get_news("Databricks"))