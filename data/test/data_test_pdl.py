import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_new_contacts():

    test_data = """{"uuid": "592ff140-91c2-475e-8bc2-1ce23328896a", "name": "Asaf Savich", "company": "GenieAI", "email": "asaf@trywonder.ai", "linkedin": "https://www.linkedin.com/in/asaf-savich/", "position": "CTO", "timezone": ""}"""

    event = GenieEvent(
        topic=Topic.NEW_CONTACT_TO_ENRICH, data=test_data, scope="public"
    )
    event.send()


test_new_contacts()
