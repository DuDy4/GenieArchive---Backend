import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.events.genie_event import GenieEvent
from common.events.topics import Topic


def test_new_contacts():

    test_data = """{"uuid": "e58a0044-c276-4c1c-a63e-5a57d79ccb8b", "name": "Asaf Savich", "company": "GenieAI", "email": "asaf@trywonder.ai", "linkedin": "https://www.linkedin.com/in/asaf-savich/", "position": "CTO", "timezone": ""}"""

    event = GenieEvent(
        topic=Topic.NEW_CONTACT_TO_ENRICH, data=test_data, scope="public"
    )
    event.send()


test_new_contacts()
