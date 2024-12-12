from common.genie_logger import GenieLogger

logger = GenieLogger()


def filter_not_reshared_social_media_news(news: list, linkedin_url):
    if "https://www." not in linkedin_url:
        linkedin_url = f"https://www.{linkedin_url}"
        logger.info(f"Linkedin url not in correct format. Formatted to {linkedin_url}")
    not_reshared_news = [new for new in news if new.reshared == linkedin_url]
    logger.info(f"Filtered {len(news) - len(not_reshared_news)} reshared news")
    return not_reshared_news
    # reshared = [news.reshared for news in news if news.reshared != linkedin_url]
    # reshared = []
    # for news in news:
    #     logger.info(f"News reshared: {news.reshared} and linkedin_url: {linkedin_url}")
    #     if news.reshared != linkedin_url:
    #         reshared.append(news.reshared)
    # return reshared