import traceback
from typing import Optional
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()
import json


class OwnershipsRepository:
    def __init__(self):
        self.create_table_if_not_exists()


    def update_tenant_id(self, new_tenant_id, old_tenant_id):
        update_query = """
        UPDATE ownerships SET tenant_id = %s WHERE tenant_id = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (new_tenant_id, old_tenant_id))
                    conn.commit()
                    logger.info(f"Updated tenant_id from {old_tenant_id} to {new_tenant_id}")
                    return True
            except psycopg2.Error as error:
                logger.error(f"Error updating tenant_id: {error.pgerror}")
                traceback.print_exc()
                return False

    def get_all_persons_for_tenant(self, tenant_id):
        self.create_table_if_not_exists()
        select_query = """
        SELECT person_uuid FROM ownerships WHERE tenant_id = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (tenant_id,))
                    ownerships = cursor.fetchall()
                    logger.info(f"Got all ownerships for tenant {tenant_id}")
                    ownerships = [ownership[0] for ownership in ownerships]
                    return ownerships
            except psycopg2.Error as error:
                logger.error(f"Error getting all ownerships: {error.pgerror}")
                traceback.print_exc()
                return []

    def get_tenants_for_person(self, uuid):
        self.create_table_if_not_exists()
        select_query = """
        SELECT tenant_id FROM ownerships WHERE person_uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    tenants = cursor.fetchall()
                    tenants = [tenant[0] for tenant in tenants]
                    return tenants
            except psycopg2.Error as error:
                logger.error(f"Error getting all tenants for person: {error.pgerror}")
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
            user_id VARCHAR,
            tenant_id VARCHAR
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(f"Error: {error}")
                traceback.print_exc()

    def insert(self, uuid, tenant_id):
        insert_query = """
        INSERT INTO ownerships (person_uuid, tenant_id)
        VALUES (%s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert ownership: {uuid}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, (uuid, tenant_id))
                    conn.commit()
                    ownership_id = cursor.fetchone()[0]
                    logger.info(f"Inserted ownership to database. Ownership id: {ownership_id}")
                    return ownership_id
            except psycopg2.Error as error:
                logger.error(f"Error inserting ownership: {error.pgerror}")
                traceback.print_exc()

    def exists(self, uuid, tenant_id):
        select_query = """
        SELECT id FROM ownerships WHERE person_uuid = %s AND tenant_id = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query, (uuid, tenant_id))
                    conn.commit()
                    logger.info(f"Deleted ownership from database")
                    return True
            except psycopg2.Error as error:
                logger.error(f"Error deleting ownership: {error.pgerror}")
                traceback.print_exc()
                return False
