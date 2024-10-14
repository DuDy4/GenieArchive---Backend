import traceback
import psycopg2
import json
from common.genie_logger import GenieLogger
from typing import List, Optional
from data.data_common.data_transfer_objects.badges_dto import BadgeDTO, UserBadgeDTO, UserBadgeProgressDTO, DetailedUserBadgeProgressDTO

logger = GenieLogger()

class BadgesRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_tables_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_tables_if_not_exists(self):
        badge_table_query = """
        CREATE TABLE IF NOT EXISTS badges (
            badge_id UUID PRIMARY KEY,
            name VARCHAR NOT NULL,
            description TEXT,
            criteria JSONB NOT NULL,
            icon_url VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        user_badge_table_query = """
        CREATE TABLE IF NOT EXISTS user_badges (
            user_badge_id UUID PRIMARY KEY,
            email VARCHAR NOT NULL,
            badge_id UUID NOT NULL,
            earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        user_badge_progress_table_query = """
        CREATE TABLE IF NOT EXISTS user_badge_progress (
            email VARCHAR NOT NULL,
            badge_id UUID NOT NULL,
            progress JSONB NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (email, badge_id)
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(badge_table_query)
                cursor.execute(user_badge_table_query)
                cursor.execute(user_badge_progress_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error(f"Error creating tables: {error}")
            traceback.print_exc()

    # Badge Methods
    def insert_badge(self, badge: BadgeDTO) -> Optional[str]:
        insert_query = """
        INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING badge_id;
        """
        badge_data = badge.to_tuple()

        logger.info(f"About to insert badge data: {badge_data}")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, badge_data)
                self.conn.commit()
                badge_id = cursor.fetchone()[0]
                logger.info(f"Inserted badge into database. Badge ID: {badge_id}")
                return badge_id
        except psycopg2.Error as error:
            logger.error(f"Error inserting badge: {error.pgerror}")
            traceback.print_exc()
            return None

    def get_all_badges(self) -> List[BadgeDTO]:
        select_query = "SELECT * FROM badges;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                badges = cursor.fetchall()
                return [BadgeDTO.from_tuple(badge) for badge in badges]
        except psycopg2.Error as error:
            logger.error(f"Error retrieving badges: {error}")
            traceback.print_exc()
            return []
            return None

    def get_all_badges_by_type(self, type: str) -> List[BadgeDTO]:
        select_query = """
            SELECT * FROM badges 
            WHERE criteria->>'type' = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (type,))
                badges = cursor.fetchall()
                return [BadgeDTO.from_tuple(badge) for badge in badges]
        except psycopg2.Error as error:
            logger.error(f"Error retrieving badges: {error}")
            traceback.print_exc()
            return []

    # User Badge Methods
    def insert_user_badge(self, user_badge: UserBadgeDTO) -> Optional[str]:
        insert_query = """
        INSERT INTO user_badges (user_badge_id, email, badge_id, earned_at)
        VALUES (%s, %s, %s, %s)
        RETURNING user_badge_id;
        """
        user_badge_data = user_badge.to_tuple()

        logger.info(f"About to insert user badge data: {user_badge_data}")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, user_badge_data)
                self.conn.commit()
                user_badge_id = cursor.fetchone()[0]
                logger.info(f"Inserted user badge into database. User Badge ID: {user_badge_id}")
                return user_badge_id
        except psycopg2.Error as error:
            logger.error(f"Error inserting user badge: {error.pgerror}")
            traceback.print_exc()
            return None

    def get_user_badges(self, email: str) -> List[UserBadgeDTO]:
        select_query = "SELECT * FROM user_badges WHERE email = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                user_badges = cursor.fetchall()
                return [UserBadgeDTO.from_tuple(badge) for badge in user_badges]
        except psycopg2.Error as error:
            logger.error(f"Error retrieving user badges: {error}")
            traceback.print_exc()
            return []

    # User Badge Progress Methods
    def update_user_badge_progress(self, user_badge_progress: UserBadgeProgressDTO) -> bool:
        """
        Insert or update the user's badge progress in the database.
        
        :param email: The user's ID.
        :param badge_id: The badge's ID.
        :param new_progress: A dictionary representing the updated progress.
        :return: True if the update was successful, False otherwise.
        """
        update_query = """
        INSERT INTO user_badge_progress (email, badge_id, progress, last_updated)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (email, badge_id) DO UPDATE
        SET progress = EXCLUDED.progress,
            last_updated = EXCLUDED.last_updated;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (user_badge_progress.email, str(user_badge_progress.badge_id), json.dumps(user_badge_progress.progress)))
                self.conn.commit()
                return True
        except psycopg2.Error as error:
            logger.error(f"Error updating user badge progress: {error.pgerror}")
            traceback.print_exc()
            return False

    def get_user_badge_progress(self, email: str, badge_id: str) -> Optional[UserBadgeProgressDTO]:
        """
        Retrieve the user's progress for a specific badge. If no progress exists, assume an empty progress.

        :param email: The user's ID.
        :param badge_id: The badge's ID.
        :return: A dictionary representing the user's progress (defaults to empty if not found).
        """
        select_query = "SELECT progress, last_updated FROM user_badge_progress WHERE email = %s AND badge_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email, badge_id))
                result = cursor.fetchone()
                if result:
                    return result  # Return the progress JSON
                else:
                    # If no progress exists, return an empty JSON object representing 0 progress
                    return {}, None
        except psycopg2.Error as error:
            logger.error(f"Error retrieving user badge progress: {error}")
            traceback.print_exc()
            return {}

    def get_user_all_current_badges_progress(self, email: str) -> list[DetailedUserBadgeProgressDTO]:
        """
        Retrieve the user's progress for all badges. If no progress exists, assume an empty progress.

        :param email: The user's ID.
        :param badge_id: The badge's ID.
        :return: A dictionary representing the user's progress (defaults to empty if not found).
        """
        select_query = """
        SELECT ubp.email, b.badge_id, ubp.progress, ubp.last_updated, b.name, b.description, b.icon_url, b.criteria
        FROM badges b
        LEFT JOIN user_badge_progress ubp ON ubp.badge_id = b.badge_id
                AND email = 'asaf@genieai.ai'; 
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email, ))
                result = cursor.fetchall()
                if result:
                    formatted_results = []
                    for bage_progress in result:
                        if not bage_progress[0]:
                            bage_progress_list = list(bage_progress)
                            bage_progress_list[0] = email
                            bage_progress = tuple(bage_progress_list)
                        formatted_results.append(DetailedUserBadgeProgressDTO.from_tuple(bage_progress))
                    return formatted_results
                else:
                    return []
        except psycopg2.Error as error:
            logger.error(f"Error retrieving user badge progress: {error}")
            traceback.print_exc()
            return []
