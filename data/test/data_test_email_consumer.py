import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


event = GenieEvent(Topic.NEW_UPCOMING_MEETING, {"meeting_uuid": "ec5592b3-bf2e-4784-9823-5c7e0300ec9b"})
event.send()
