import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.dependencies.dependencies import companies_repository


def data_test_new_company_news():
    event = GenieEvent(
        topic=Topic.COMPANY_NEWS_UPDATED,
        data='{"company_uuid": "68167b0c-557f-45d8-b19b-a37b15ac32a1"}',
    )
    assert event
    event.send()
    print("News updated test passed")


def data_test_company_up_to_date(company_uuid):
    event = GenieEvent(topic=Topic.COMPANY_NEWS_UP_TO_DATE, data={"company_uuid": company_uuid})
    assert event
    event.send()
    print("News up to date test passed")


data_test_new_company_news()
data_test_company_up_to_date()
