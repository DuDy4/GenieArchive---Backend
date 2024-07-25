import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_new_contacts(email_address: str):

    test_data = {"email": email_address}

    event = GenieEvent(
        topic=Topic.FAILED_TO_ENRICH_EMAIL, data=json.dumps(test_data), scope="public"
    )
    event.send()


test_new_contacts("hello@frontegg.com")
test_new_contacts("hasan.cakmak@couchbase.com")
