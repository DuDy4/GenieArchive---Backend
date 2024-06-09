import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.events.genie_event import GenieEvent
from common.events.topics import Topic


def test_new_contacts():

    test_data = """[PersonDTO(uuid='b6a763c7-1ccc-4d19-b2f5-c3348744bde7', name='Rose Gonzalez', company='Edge
    Communications', email='rose@edge.com', linkedin='', position='SVP, Procurement', timezone=''), PersonDTO(
    uuid='f4f10170-188d-4e09-bccb-0bcf8853fd73', name='Jake Llorrac', company='sForce', email='', linkedin='',
    position='', timezone=''), PersonDTO(uuid='e58a0044-c276-4c1c-a63e-5a57d79ccb8b', name='Asaf Savich',
    company='GenieAI', email='asaf@trywonder.ai', linkedin='https://www.linkedin.com/in/asaf-savich/',
    position='CTO', timezone='')]"""

    event = GenieEvent(
        topic=Topic.NEW_CONTACTS_TO_CHECK, data=test_data, scope="public"
    )
    event.send()


test_new_contacts()
