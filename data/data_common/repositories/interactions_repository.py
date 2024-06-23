from typing import Optional
import psycopg2
from loguru import logger

from data.data_common.data_transfer_objects.interaction import InteractionDTO


class InteractionsRepository:
    def __init__(self, conn):
        self.conn = conn

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS interactions (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            userUuid VARCHAR,
            userEmail VARCHAR,
            interaction_source VARCHAR,
            interaction_type VARCHAR,
            company VARCHAR,
            recipient_uuid VARCHAR,
            recipient_email VARCHAR,
            recipient_company VARCHAR,
            content TEXT,
            timestamp INT
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info(f"Created interactions table in database")
        except Exception as error:
            logger.error("Error creating table:", error)

    def insert(self, interaction: InteractionDTO) -> Optional[int]:
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO interactions (uuid, userUuid, userEmail, interaction_source, interaction_type, company,
        recipient_uuid, recipient_email, recipient_company, content, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert interaction: {interaction}")
        interaction_data = interaction.to_tuple()
        logger.debug(f"Interaction tuple: {interaction_data}")

        try:
            with self.conn.cursor() as cursor:
                logger.debug("About to execute sql command")
                cursor.execute(insert_query, interaction_data)
                logger.debug("About to commit the sql command")
                self.conn.commit()
                logger.info("Inserted new interaction")
                interaction_id = cursor.fetchone()[0]
                logger.info(
                    f"Inserted interaction to database. Interaction id: {interaction_id}"
                )
                return interaction_id
        except psycopg2.Error as error:
            logger.error("Error inserting interaction:", error)
            print(error)
            return None

    def exists(self, uuid: str) -> bool:
        logger.info(f"about to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM interactions WHERE uuid = %s;"

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (uuid,))
                result = cursor.fetchone() is not None
                logger.info(f"{uuid} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of uuid {uuid}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def get_interaction_id(self, uuid):
        select_query = "SELECT id FROM interactions WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    return row[0]
                else:
                    logger.error(f"Error with getting interaction id for {uuid}")

        except Exception as error:
            logger.error("Error fetching id by uuid:", error)
        return None

    def get_interaction_by_id(self, id: str) -> Optional[InteractionDTO]:
        select_query = "SELECT * FROM interactions WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (id,))
                row = cursor.fetchone()
                if row:
                    return InteractionDTO(*row[1:])
        except Exception as error:
            logger.error("Error fetching interaction by id:", error)
        return None

    def update_interaction(self, interaction: InteractionDTO):
        update_query = """
        UPDATE interactions
        SET userUuid = %s, userEmail = %s, interaction_source = %s, interaction_type = %s, company = %s,
        recipient_uuid = %s, recipient_email = %s, recipient_company = %s, content = %s, timestamp = %s
        WHERE uuid = %s;
        """
        interaction_data = interaction.to_tuple()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, interaction_data)
                self.conn.commit()
                logger.info(
                    f"Updated interaction with uuid {interaction.uuid} in database"
                )
        except Exception as error:
            logger.error("Error updating interaction:", error)

    def delete_interaction(self, id: str):
        delete_query = "DELETE FROM interactions WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (id,))
                self.conn.commit()
                logger.info(f"Deleted interaction with id {id} from database")
        except Exception as error:
            logger.error("Error deleting interaction:", error)

    def save_interaction(self, interaction: InteractionDTO):
        self.create_table_if_not_exists()
        if self.exists(interaction.uuid):
            self.update_interaction(interaction)
        else:
            self.insert(interaction)
