from datetime import datetime, timezone, timedelta
from uuid import UUID

from data.data_common.repositories.statuses_repository import StatusesRepository
from data.data_common.data_transfer_objects.status_dto import StatusDTO, StatusEnum
from common.genie_logger import GenieLogger

logger = GenieLogger()

statuses_repo = StatusesRepository()
statuses_repo.create_table_if_not_exists()
def test_start_status():

    statuses_repo.start_status("ctx_id", "123e4567-e89b-12d3-a456-426614174000", "tenant_id", "previous_event", "event_topic")
    try:
        status = statuses_repo.get_status("ctx_id","123e4567-e89b-12d3-a456-426614174000", "tenant_id", "event_topic")
        assert status.ctx_id == "ctx_id"
        assert status.object_uuid == UUID("123e4567-e89b-12d3-a456-426614174000")
        assert status.tenant_id == "tenant_id"
        assert status.event_topic == "event_topic"
        assert status.previous_event_topic == "previous_event"
        assert abs(status.current_event_start_time - datetime.now(timezone.utc)) < timedelta(seconds=4)
        assert status.status == StatusEnum.STARTED
    finally:
        statuses_repo.delete_status("ctx_id", "123e4567-e89b-12d3-a456-426614174000", "tenant_id", "event_topic")
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174000", "tenant_id", "event_topic")
        assert status is None

def test_update_status():
    statuses_repo.start_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "previous_event", "event_topic")
    try:
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
        assert status.ctx_id == "ctx_id"
        assert status.object_uuid == UUID("123e4567-e89b-12d3-a456-426614174001")
        assert status.tenant_id == "tenant_id"
        assert status.event_topic == "event_topic"
        assert status.previous_event_topic == "previous_event"
        assert abs(status.current_event_start_time - datetime.now(timezone.utc)) < timedelta(seconds=1)
        assert status.status == StatusEnum.STARTED

        statuses_repo.update_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic", StatusEnum.PROCESSING)
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
        assert status.status == StatusEnum.PROCESSING

        statuses_repo.update_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic", StatusEnum.COMPLETED)
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
        assert status.status == StatusEnum.COMPLETED
    finally:
        statuses_repo.delete_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
        assert status is None


def test_failed_status_with_error():
    statuses_repo.start_status("ctx_id", "123e4567-e89b-12d3-a456-426614174002", "tenant_id", "previous_event", "event_topic")
    try:
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
        assert status.ctx_id == "ctx_id"
        assert status.object_uuid == UUID("123e4567-e89b-12d3-a456-426614174002")
        assert status.tenant_id == "tenant_id"
        assert status.event_topic == "event_topic"
        assert status.previous_event_topic == "previous_event"
        assert abs(status.current_event_start_time - datetime.now(timezone.utc)) < timedelta(seconds=1)
        assert status.status == StatusEnum.STARTED

        statuses_repo.update_status("ctx_id", "123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic", StatusEnum.FAILED, "Error message")
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
        assert status.status == StatusEnum.FAILED
        error_message = statuses_repo.get_error_message("ctx_id", "123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
        assert error_message == "Error message"

    finally:
        statuses_repo.delete_status("ctx_id", "123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
        status = statuses_repo.get_status("ctx_id", "123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
        assert status is None
