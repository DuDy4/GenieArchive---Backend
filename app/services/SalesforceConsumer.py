from loguru import logger

from app.app_common.dependencies.dependencies import contacts_repository
from common.events.genie_consumer import GenieConsumer
from common.events.genie_event import GenieEvent
from common.events.topics import Topic
from app.app_common.data_transfer_objects.person import PersonDTO


class SalesforceConsumer(GenieConsumer):
    """
    Class for producing events to the Salesforce event queue.
    """

    def __init__(self):
        super().__init__(topics=[Topic.NEW_CONTACTS_TO_CHECK])
        self.contacts_repository = contacts_repository()

    def process_event(self, event):
        """
        Produces an event to the Salesforce event queue.

        Args:
            event: The event to produce.
        """
        logger.info(
            f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}"
        )

        # for each contact - check if exists in our db
        contacts = event.body_as_str
        logger.debug(f"Contacts: {contacts} as type: {type(contacts)}")
        for contact in contacts:
            person = PersonDTO.from_sf_contact(contact)
            if self.contacts_repository.search_contact(
                person.name, person.company, person.email
            ):
                logger.info(f"Contact {person.name} already exists in our db")
                continue
            else:
                new_contact_event = GenieEvent(Topic.NEW_CONTACT, contact, "public")
                new_contact_event.send()
                logger.info(
                    f"Contact {person.name} - sent new contact event with linkedin"
                )
                self.contacts_repository.insert_contact(person)
                logger.info(f"Contact {person.name} inserted to our db")

        # if not - insert to db and send new contact event

        self.redis_client.publish("salesforce", event)

    def get_linkedin(self, contact):
        contact_linkedin = contact.get("LinkedInUrl")
        if not contact_linkedin:
            for key in contact.keys():
                if "linkedin" in key.lower():
                    contact_linkedin = contact.get(key)
                    return contact_linkedin
        return contact_linkedin
