import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_regoals_event():
    data_to_send = {
        "tenant_id": "org_N1U4UsHtTfESJPYB",
    }

    event = GenieEvent(topic=Topic.NEW_EMBEDDED_DOCUMENT, data=data_to_send)
    event.send()


test_regoals_event()
