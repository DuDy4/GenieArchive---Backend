import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


event = GenieEvent(Topic.NEW_UPCOMING_MEETING, {"meeting_uuid": "09a4d389-c9e8-4221-9a1a-4babab30b743"})
event.send()
