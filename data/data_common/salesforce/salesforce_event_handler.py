from data.data_common.repositories.contacts_repository import ContactsRepository
from ..data_transfer_objects.person_dto import PersonDTO

from ..events.genie_event import GenieEvent
from ..events.topics import Topic

from common.genie_logger import GenieLogger
logger = GenieLogger()

class SalesforceEventHandler:
    def __init__(self, contacts_repository: ContactsRepository):
        self.contacts_repository = contacts_repository

    def handle_event(self, event):
        try:
            # Parse the incoming event
            logger.info(f"Received event data: {event}")
            salesforce_contact_id = event.get("ContactId__c")
            change_type = event.get("ChangeType__c")
            self.contacts_repository.create_table_if_not_exists()

            if not salesforce_contact_id or not change_type:
                logger.error("Invalid event data: missing ContactId or ChangeType")
                return

            if change_type == "Insert":
                if self.contacts_repository.exists_salesforce_id(salesforce_contact_id):
                    logger.warning(
                        f"Contact {salesforce_contact_id} already exists in the repository"
                    )
                    return
                contact_data = event.get("ContactData__c")
                if contact_data:
                    # Convert contact data to PersonDTO
                    person = PersonDTO.from_sf_contact(contact_data)
                    # Insert new contact into the repository
                    row_id = self.contacts_repository.insert_contact(
                        person, salesforce_contact_id
                    )
                    if row_id:
                        logger.info(f"Inserted new contact: {person}")
                        event = GenieEvent(
                            Topic.NEW_CONTACT,
                            person.to_json(),
                            "public",
                        )
                        event.send()
                        return True
                    else:
                        logger.error(f"Failed to insert new contact: {person}")
                        return False

            elif change_type == "Update":
                # Check if the contact exists in the repository, and get the uuid if it does
                uuid = self.contacts_repository.exists_salesforce_id(
                    salesforce_contact_id
                )
                if not uuid:
                    logger.warning(
                        f"Contact {salesforce_contact_id} does not exists in the repository"
                    )
                    return
                contact_data = event.get("ContactData__c")
                if contact_data:
                    person = PersonDTO.from_sf_contact(contact_data)
                    logger.debug(f"Updating contact: {person}")
                    if uuid:
                        person.uuid = uuid
                    result = self.contacts_repository.update_contact(person)
                    if result:
                        logger.info(
                            f"Updated contact {salesforce_contact_id}, uuid: {uuid}"
                        )
                        event = GenieEvent(
                            Topic.NEW_CONTACT,
                            person.to_json(),
                            "public",
                        )
                        event.send()
                        return True
                    else:
                        logger.error(f"Failed to update contact: {person}")
                        return False

            elif change_type == "Delete":
                if not self.contacts_repository.exists_salesforce_id(
                    salesforce_contact_id
                ):
                    logger.warning(
                        f"Contact {salesforce_contact_id} does not exists in the repository"
                    )
                    return
                # Delete contact from the repository
                row_id = self.contacts_repository.get_contact_id_by_salesforce_id(
                    salesforce_contact_id
                )
                if not row_id:
                    logger.error(
                        f"Failed to get contact id for {salesforce_contact_id}"
                    )
                    return False
                self.contacts_repository.delete_contact(row_id)
                logger.info(f"Deleted contact: {salesforce_contact_id}")
                return True
            else:
                logger.error(f"Unknown change type: {change_type}")
                return False

            # Send an event after processing the data

        except Exception as e:
            logger.error(f"Error handling event: {e}")
