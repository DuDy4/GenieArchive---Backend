import datetime
from data.data_common.data_transfer_objects.stats_dto import StatsDTO
from data.data_common.dependencies.dependencies import badges_repository, stats_repository
from data.data_common.data_transfer_objects.badges_dto import (
    DetailedUserBadgeProgressDTO,
    UserBadgeProgressDTO,
    UserBadgeDTO,
    BadgesEventTypes,
)
from common.utils.str_utils import get_uuid4
from common.genie_logger import GenieLogger
from data.data_common.repositories.users_repository import UsersRepository

logger = GenieLogger()


class BadgesApiService:
    def __init__(self):
        self.badges_repository = badges_repository()
        self.stats_repository = stats_repository()
        self.users_repository = UsersRepository()

    def get_user_badges_status(self, user_id: str) -> list[DetailedUserBadgeProgressDTO]:
        """
        Get all badges for a user.

        :param user_id: The ID of the tenant/user.
        :return: A list of badge DTOs.
        """
        email = self.users_repository.get_email_by_user_id(user_id)
        badges_progress = self.badges_repository.get_user_all_current_badges_progress(email)
        logger.info(f"User {email} has {badges_progress}")
        formatted_badges = []
        for badge in badges_progress:
            progress, last_updated = self.check_progress_by_frequency(badge.progress, badge.criteria.get("frequency"), badge.last_updated)
            formatted_badges.append(
                {
                    "badge_id": str(badge.badge_id),
                    "name": badge.badge_name,
                    "description": badge.badge_description,
                    "frequency": badge.criteria["frequency"],
                    "progress": {
                        "type": badge.criteria["type"],
                        "count": progress.get("count") if progress else 0,
                        "goal": badge.criteria["count"],
                    },
                    "icon_url": badge.badge_icon_url,
                    "seen": badge.seen,
                }
            )
        logger.info(f"User {email} has {len(formatted_badges)} badges")
        return formatted_badges
    
    def mark_badges_as_seen(self, user_id: str):
        """
        Marks a badge as seen by the user.

        :param user_id: The ID of the tenant/user.
        :param badge_id: The ID of the badge to mark as seen.
        """
        email = self.users_repository.get_email_by_user_id(user_id)
        if self.badges_repository.mark_badges_as_seen(email):
            logger.info(f"Marked badges as seen for user {email}")


    def get_unseen_badges(self, user_id: str) -> list[str]:
        """
        Returns any unseen badges for a user.

        :param user_id: The ID of the tenant/user.
        :return: list of unseen badges
        """
        email = self.users_repository.get_email_by_user_id(user_id)
        return self.badges_repository.get_unseen_badges(email)

    def handle_event(self, email: str, action: str, entity: str, entity_id: str):
        """
        Handles various user events (e.g., profile view, meeting view, login) and updates badge progress.

        :param email: The email of the tenant/user.
        :param action: The type of event (e.g., "VIEW", "LOGIN").
        :param entity: The entity associated with the event (e.g., "PROFILE", "MEETING").
        :param entity_id: The ID of the entity associated with the event.
        """
        if not email or not action or not entity or not entity_id:
            logger.info(f"Invalid badge event data: {email}, {action}, {entity}, {entity_id}")
            return
        event_type = action + "_" + entity
        if event_type not in BadgesEventTypes.__members__:
            logger.info(f"Invalid badge event type {event_type}")
            return
        stats_dto = StatsDTO(entity_id=entity_id, email=email, action=action, entity=entity)
        start_of_day = datetime.datetime.utcnow().replace(hour=3, minute=0, second=0, microsecond=0)
        num_events_today = self.stats_repository.count_events_from_date(stats_dto, start_of_day)
        if not num_events_today:
            logger.info(f"Error counting events for user {email}")
            return
        if num_events_today > 1 and entity != BadgesEventTypes.UPLOAD_FILE_CATEGORY.value:
            logger.info(
                f"Skipping badge calc. Event {event_type} for entity {entity_id} already exists for user {email}"
            )
            return

        badges = self.badges_repository.get_all_badges_by_type(event_type)
        if not badges:
            logger.info(f"No badges found for event type {event_type}")
            return
        for badge in badges:
            self.update_badge_progress(email, badge, event_type)

    def update_badge_progress(self, email: str, badge, event_type: str):
        """
        Updates the badge progress for a user based on the given event.

        :param email: The email of the tenant/user.
        :param badge: The badge DTO to update.
        :param event_type: The type of event (e.g., "VIEW_PROFILE", "VIEW_MEETING").
        """
        # Get the current progress for this badge
        current_progress, last_updated = self.badges_repository.get_user_badge_progress(email, str(badge.badge_id)) or {}


        now = datetime.datetime.utcnow()

        # Reset progress if it's a daily badge and the date has passed 3 AM UTC
        # if badge.criteria["frequency"] == "daily":
        #     if last_updated:
        #         # Check if the current time is beyond the 3 AM reset point
        #         if not self.is_within_daily_window(last_updated, now):
        #             current_progress = {"count": 0}
        #
        # # Reset progress if it's a weekly badge and the date has passed Sunday 3 AM UTC
        # elif badge.criteria["frequency"] == "weekly":
        #     if last_updated:
        #         # Check if the current time is beyond the weekly reset point
        #         if not self.is_within_weekly_window(last_updated, now):
        #             current_progress = {"count": 0}

        current_progress, last_updated = self.check_progress_by_frequency(current_progress, badge.criteria.get("frequency"), last_updated)

        # Update progress based on the event type and badge criteria
        if badge.criteria.get("type") and badge.criteria.get("type") == event_type:
            count = current_progress.get("count", 0) + 1
            new_progress = {"count": count}
            if event_type == BadgesEventTypes.UPLOAD_FILE_CATEGORY.value:
                tenant_categories = self.stats_repository.get_file_categories_stats(email)
                tenant_categories = [category for category in tenant_categories if category != "OTHER"] # Exclude "OTHER" category
                new_progress = {"count": len(tenant_categories)}
            # Check if the new progress meets the badge criteria
            if count >= badge.criteria["count"]:
                self.award_badge(email, str(badge.badge_id), badge.criteria["frequency"])

            # Update progress in the database
            progress_dto = UserBadgeProgressDTO(
                email=email, badge_id=str(badge.badge_id), progress=new_progress, last_updated=now
            )
            self.badges_repository.update_user_badge_progress(progress_dto)

    def check_progress_by_frequency(self, progress: dict, frequency: str, last_updated: datetime):
        """
        Handles the badge frequency logic for daily and weekly badges.

        :param progress: The current badge progress.
        :param frequency: The frequency of the badge (e.g., "daily", "weekly").
        :param last_updated: The timestamp of the last update.
        """
        if not progress or not last_updated:
            logger.error("Invalid progress or last_updated")
            return progress, last_updated
        if frequency == "daily":
            if not self.is_within_daily_window(last_updated, datetime.datetime.utcnow()):
                progress["count"] = 0
                last_updated = datetime.datetime.utcnow()
        elif frequency == "weekly":
            if not self.is_within_weekly_window(last_updated, datetime.datetime.utcnow()):
                progress["count"] = 0
                last_updated = datetime.datetime.utcnow()
        return progress, last_updated

    def is_within_daily_window(self, last_updated, current_time):
        """
        Checks if the current time is within the 3 AM UTC to 3 AM UTC of the next day window.

        :param last_updated: The timestamp of the last update.
        :param current_time: The current time in UTC.
        :return: True if within the daily window, False otherwise.
        """
        # Calculate the 3 AM UTC reset point for today
        reset_time = current_time.replace(hour=3, minute=0, second=0, microsecond=0)
        if current_time.hour < 3:
            reset_time -= datetime.timedelta(days=1)
        return last_updated >= reset_time

    def is_within_weekly_window(self, last_updated, current_time):
        """
        Checks if the current time is within the weekly window starting from Sunday 3 AM UTC.

        :param last_updated: The timestamp of the last update.
        :param current_time: The current time in UTC.
        :return: True if within the weekly window, False otherwise.
        """
        # Calculate the Sunday 3 AM UTC reset point for this week
        reset_time = current_time - datetime.timedelta(days=current_time.weekday() + 1)
        reset_time = reset_time.replace(hour=3, minute=0, second=0, microsecond=0)
        if current_time.weekday() == 6 and current_time.hour < 3:
            reset_time -= datetime.timedelta(days=7)
        return last_updated >= reset_time

    def award_badge(self, email: str, badge_id: str, frequency: str):
        """
        Awards a badge to the user if the progress criteria are met.

        :param email: The email of the tenant/user.
        :param badge_id: The ID of the badge to award.
        :param frequency: The frequency of the badge (e.g., "daily", "weekly").
        """
        # Check if the user already has this badge
        # user_badges = self.badges_repository.get_user_badges(email)
        # if badge_id in [badge.badge_id for badge in user_badges]:
        #     logger.info(f"User {email} already has badge {badge_id}")
        #     return


        user_badge_in_db = self.badges_repository.get_user_badge(email, badge_id)

        if user_badge_in_db and user_badge_in_db.last_earned_at:
            if frequency == "daily" and self.is_within_daily_window(user_badge_in_db.last_earned_at, datetime.datetime.utcnow()):
                logger.info(f"User {email} already has badge {badge_id}")
                return
            elif frequency == "weekly" and self.is_within_weekly_window(user_badge_in_db.last_earned_at, datetime.datetime.utcnow()):
                logger.info(f"User {email} already has badge {badge_id}")
                return
            elif frequency == "alltime":
                logger.info(f"User {email} already has badge {badge_id}")
                return

        # Award the badge
        user_badge_dto = UserBadgeDTO(
            user_badge_id=get_uuid4(),
            email=email,
            badge_id=badge_id,
            first_earned_at=datetime.datetime.utcnow(),
            last_earned_at=datetime.datetime.utcnow(),
        )
        logger.info(f"Awarding badge: {user_badge_dto}")
        # self.badges_repository.insert_user_badge(user_badge_dto)
        self.badges_repository.save_user_badge(user_badge_dto)
        logger.info(f"Awarded badge {badge_id} to user {email}")
