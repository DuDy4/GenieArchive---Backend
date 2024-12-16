from common.genie_logger import GenieLogger

logger = GenieLogger()


def filter_not_reshared_social_media_news(news: list, linkedin_url):
    not_reshared_news = [new for new in news if (new.reshared and linkedin_url in new.reshared)]
    return not_reshared_news
