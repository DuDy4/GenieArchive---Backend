import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_news_consumer():
    event = GenieEvent(
        topic=Topic.NEW_COMPANY_DATA,
        data='{"company_uuid": "e2b828f3-d39d-4473-94dc-6efc11f4add6"}',
        scope="public",
    )
    assert event
    event.send()
    print("News consumer test passed")


test_news_consumer()
