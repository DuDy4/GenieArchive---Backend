import json

from app_common.data_transfer_objects.person_dto import PersonDTO
from app_common.events.topics import Topic
from app_common.events.genie_event import GenieEvent
from loguru import logger


def handle_new_contacts_event(new_contacts: list[PersonDTO | dict]):
    logger.info(f"Topic: {Topic.NEW_CONTACT}")
    try:
        for i in range(0, len(new_contacts)):
            if isinstance(new_contacts[i], PersonDTO):
                contact = new_contacts[i].to_json()
            if isinstance(new_contacts[i], dict):
                contact = json.dumps(new_contacts[i])
            event = GenieEvent(
                Topic.NEW_CONTACT,
                contact,
                "public",
            )
            event.send()
        return {"status": "success", "message": "Event processed"}
    except Exception as e:
        logger.error(f"Error handling new contacts event: {e}")
