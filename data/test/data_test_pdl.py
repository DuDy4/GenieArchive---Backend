import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_new_contacts():

    test_data = """{"uuid": "592ff140-91c2-475e-8bc2-1ce23328896a", "name": "", "company": "", "email": "asaf@trywonder.ai", "linkedin": "", "position": "", "timezone": ""}"""

    event = GenieEvent(topic=Topic.PDL_NEW_PERSON_TO_ENRICH, data=test_data)
    event.send()


test_new_contacts()
