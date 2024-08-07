import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_news_consumer():
    event = GenieEvent(
        topic=Topic.NEW_COMPANY_DATA,
        data='{"company_uuid": "140ed066-5a07-4191-9b82-e0ba983806d5"}',
        scope="public",
    )
    assert event
    event.send()
    print("News consumer test passed")


test_news_consumer()
