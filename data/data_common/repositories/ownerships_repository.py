import traceback
from typing import Optional
import psycopg2
from common.genie_logger import GenieLogger
logger = GenieLogger()
import json


class OwnershipsRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def get_all_persons_for_tenant(self, tenant_id):
        self.create_table_if_not_exists()
        select_query = """
        SELECT person_uuid FROM ownerships WHERE tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                logger.debug(f"About to get all ownerships for tenant {tenant_id}")
                cursor.execute(select_query, (tenant_id,))
                logger.debug(f"Executed query")
                ownerships = cursor.fetchall()
                logger.info(f"Got all ownerships for tenant {tenant_id}: {ownerships}")
                ownerships = [ownership[0] for ownership in ownerships]
                return ownerships
        except psycopg2.Error as error:
            logger.error(f"Error getting all ownerships: {error.pgerror}")
            traceback.print_exc()
            return []

    def save_ownership(self, uuid, tenant_id):
        self.create_table_if_not_exists()
        logger.info(f"About to save ownership: {uuid}, for tenant: {tenant_id}")
        if self.exists(uuid, tenant_id):
            return "Ownership already exists in database"
        self.insert(uuid, tenant_id)
        return "Ownership saved successfully"

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ownerships (
            id SERIAL PRIMARY KEY,
            person_uuid VARCHAR NOT NULL,
            tenant_id VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error: {error}")
            traceback.print_exc()
            self.conn.rollback()

    def insert(self, uuid, tenant_id):
        insert_query = """
        INSERT INTO ownerships (person_uuid, tenant_id)
        VALUES (%s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert ownership: {uuid}")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, (uuid, tenant_id))
                self.conn.commit()
                ownership_id = cursor.fetchone()[0]
                logger.info(
                    f"Inserted ownership to database. Ownership id: {ownership_id}"
                )
                return ownership_id
        except psycopg2.Error as error:
            logger.error(f"Error inserting ownership: {error.pgerror}")
            traceback.print_exc()

    def exists(self, uuid, tenant_id):
        select_query = """
        SELECT id FROM ownerships WHERE person_uuid = %s AND tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid, tenant_id))
                ownership = cursor.fetchone()
                if ownership:
                    return True
                return False
        except psycopg2.Error as error:
            logger.error(f"Error checking ownership existence: {error.pgerror}")
            traceback.print_exc()
            return False

    def check_ownership(self, tenant_id, uuid):
        """
        Check if the ownership exists in the database
        """
        select_query = """
        SELECT id FROM ownerships WHERE person_uuid = %s AND tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid, tenant_id))
                ownership = cursor.fetchone()
                if ownership:
                    return True
                return False
        except psycopg2.Error as error:
            logger.error(f"Error checking ownership existence: {error.pgerror}")
            traceback.print_exc()
            return False

    def delete_ownership(self, tenant_id, uuid):
        """
        Delete ownership from the database
        """
        delete_query = """
        DELETE FROM ownerships WHERE person_uuid = %s AND tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (uuid, tenant_id))
                self.conn.commit()
                logger.info(f"Deleted ownership from database")
                return True
        except psycopg2.Error as error:
            logger.error(f"Error deleting ownership: {error.pgerror}")
            traceback.print_exc()
            return False
