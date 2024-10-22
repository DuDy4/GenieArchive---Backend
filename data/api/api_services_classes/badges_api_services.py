import datetime
from data.data_common.data_transfer_objects.stats_dto import StatsDTO
from data.data_common.dependencies.dependencies import badges_repository, stats_repository, tenants_repository
from data.data_common.data_transfer_objects.badges_dto import (
    DetailedUserBadgeProgressDTO,
    UserBadgeProgressDTO,
    UserBadgeDTO,
    BadgesEventTypes,
)
from common.utils.str_utils import get_uuid4
from common.genie_logger import GenieLogger

logger = GenieLogger()


class BadgesApiService:
    def __init__(self):
        self.badges_repository = badges_repository()
        self.stats_repository = stats_repository()
        self.tenants_repository = tenants_repository()

    def get_user_badges_status(self, tenant_id: str) -> list[DetailedUserBadgeProgressDTO]:
        """
        Get all badges for a user.

        :param tenant_id: The ID of the tenant/user.
        :return: A list of badge DTOs.
        """
        email = self.tenants_repository.get_tenant_email(tenant_id)
        badges_progress = self.badges_repository.get_user_all_current_badges_progress(email)
        formatted_badges = []
        for badge in badges_progress:
            formatted_badges.append(
                {
                    "badge_id": str(badge.badge_id),
                    "name": badge.badge_name,
                    "description": badge.badge_description,
                    "frequency": badge.criteria["frequency"],
                    "progress": {
                        "type": badge.criteria["type"],
                        "count": badge.progress["count"] if badge.progress else 0,
                        "goal": badge.criteria["count"],
                    },
                    "icon_url": badge.badge_icon_url,
                }
            )
        logger.info(f"User {email} has {len(formatted_badges)} badges")
        return formatted_badges

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
        if num_events_today > 1:
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
        current_progress, last_updated = (
            self.badges_repository.get_user_badge_progress(email, str(badge.badge_id)) or {}
        )
        now = datetime.datetime.utcnow()

        # Reset progress if it's a daily badge and the date has passed 3 AM UTC
        if badge.criteria["frequency"] == "daily":
            if last_updated:
                # Check if the current time is beyond the 3 AM reset point
                if not self.is_within_daily_window(last_updated, now):
                    current_progress = {"count": 0}

        # Reset progress if it's a weekly badge and the date has passed Sunday 3 AM UTC
        elif badge.criteria["frequency"] == "weekly":
            if last_updated:
                # Check if the current time is beyond the weekly reset point
                if not self.is_within_weekly_window(last_updated, now):
                    current_progress = {"count": 0}

        # Update progress based on the event type and badge criteria
        if badge.criteria["type"] == event_type:
            count = current_progress.get("count", 0) + 1
            new_progress = {"count": count}

            # Check if the new progress meets the badge criteria
            if count >= badge.criteria["count"]:
                self.award_badge(email, badge.badge_id)

            # Update progress in the database
            progress_dto = UserBadgeProgressDTO(
                email=email, badge_id=str(badge.badge_id), progress=new_progress, last_updated=now
            )
            self.badges_repository.update_user_badge_progress(progress_dto)

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

    def award_badge(self, email: str, badge_id: str):
        """
        Awards a badge to the user if the progress criteria are met.

        :param email: The email of the tenant/user.
        :param badge_id: The ID of the badge to award.
        """
        # Check if the user already has this badge
        user_badges = self.badges_repository.get_user_badges(email)
        if badge_id in [badge.badge_id for badge in user_badges]:
            return  # User already has this badge

        # Award the badge
        user_badge_dto = UserBadgeDTO(
            user_badge_id=get_uuid4(), email=email, badge_id=badge_id, earned_at=datetime.datetime.utcnow()
        )
        self.badges_repository.insert_user_badge(user_badge_dto)
        logger.info(f"Awarded badge {badge_id} to user {email}")
