from datetime import datetime, timezone
from uuid import UUID

from data.data_common.repositories.statuses_repository import StatusesRepository
from data.data_common.data_transfer_objects.status_dto import StatusDTO, StatusEnum
from common.genie_logger import GenieLogger

logger = GenieLogger()

statuses_repo = StatusesRepository()

def test_insert_status():
    status_dto = StatusDTO(
        person_uuid=UUID("123e4567-e89b-12d3-a456-426614174000"),
        tenant_id="tenant_id",
        event_topic="event_topic",
        current_event_start_time=datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
        status=StatusEnum.PROCESSING,
    )
    statuses_repo.insert_status(status_dto)
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174000", "tenant_id", "event_topic")
    assert status.person_uuid == UUID("123e4567-e89b-12d3-a456-426614174000")
    assert status.tenant_id == "tenant_id"
    assert status.event_topic == "event_topic"
    assert status.current_event_start_time == datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert status.status == StatusEnum.PROCESSING
    statuses_repo.delete_status("123e4567-e89b-12d3-a456-426614174000", "tenant_id", "event_topic")
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174000", "tenant_id", "event_topic")
    assert status is None

def test_update_status():
    status_dto = StatusDTO(
        person_uuid=UUID("123e4567-e89b-12d3-a456-426614174001"),
        tenant_id="tenant_id",
        event_topic="event_topic",
        current_event_start_time=datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
        status=StatusEnum.PROCESSING,
    )
    statuses_repo.insert_status(status_dto)
    statuses_repo.update_status(
        "123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic", StatusEnum.COMPLETED
    )
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
    assert status.person_uuid == UUID("123e4567-e89b-12d3-a456-426614174001")
    assert status.tenant_id == "tenant_id"
    assert status.event_topic == "event_topic"
    assert status.current_event_start_time.tzinfo is not None and status.current_event_start_time.tzinfo.utcoffset(
        status.current_event_start_time
    ) == timezone.utc.utcoffset(None)
    assert status.status == StatusEnum.COMPLETED
    statuses_repo.delete_status("123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174001", "tenant_id", "event_topic")
    assert status is None


def test_save_status():
    statuses_repo.save_status("123e4567-e89b-12d3-a456-426614174003", "tenant_id", "event_topic", StatusEnum.PROCESSING)
    statuses_repo.save_status(
        "123e4567-e89b-12d3-a456-426614174003", "tenant_id", "new_event", StatusEnum.COMPLETED
    )
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174003", "tenant_id", "event_topic")
    assert status.person_uuid == UUID("123e4567-e89b-12d3-a456-426614174003")
    assert status.tenant_id == "tenant_id"
    assert status.event_topic == "event_topic"
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174003", "tenant_id", "new_event")
    assert status.person_uuid == UUID("123e4567-e89b-12d3-a456-426614174003")
    assert status.tenant_id == "tenant_id"
    assert status.event_topic == "new_event"
    assert status.current_event_start_time.tzinfo is not None and status.current_event_start_time.tzinfo.utcoffset(
        status.current_event_start_time
    ) == timezone.utc.utcoffset(None)
    assert status.status == StatusEnum.COMPLETED
    statuses_repo.delete_status("123e4567-e89b-12d3-a456-426614174003", "tenant_id", "event_topic")
    statuses_repo.delete_status("123e4567-e89b-12d3-a456-426614174003", "tenant_id", "new_event")
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174003", "tenant_id", "event_topic")
    assert status is None
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174003", "tenant_id", "new_event")
    assert status is None


def test_get_status():
    status_dto = StatusDTO(
        person_uuid=UUID("123e4567-e89b-12d3-a456-426614174002"),
        tenant_id="tenant_id",
        event_topic="event_topic",
        current_event_start_time=datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
        status=StatusEnum.PROCESSING,
    )
    statuses_repo.insert_status(status_dto)
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
    assert status.person_uuid == UUID("123e4567-e89b-12d3-a456-426614174002")
    assert status.tenant_id == "tenant_id"
    assert status.event_topic == "event_topic"
    assert status.current_event_start_time == datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert status.status == StatusEnum.PROCESSING
    statuses_repo.delete_status("123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
    status = statuses_repo.get_status("123e4567-e89b-12d3-a456-426614174002", "tenant_id", "event_topic")
    assert status is None
