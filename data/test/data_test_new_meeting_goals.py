import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def data_test_new_goals():
    event = GenieEvent(
        topic=Topic.NEW_MEETING_GOALS,
        data='{"meeting_uuid": "f7566f30-a294-4018-9371-5ec03e15c70d"}',
    )
    assert event
    event.send()
    print("News updated test passed")


def data_test_new_goals2():
    event = GenieEvent(
        topic=Topic.NEW_MEETING_GOALS,
        data='{"meeting_uuid": "52b18895-6ade-4a3b-a8da-18bb39b88329"}',
    )
    assert event
    event.send()
    print("News up to date test passed")


# data_test_new_goals()
data_test_new_goals2()
