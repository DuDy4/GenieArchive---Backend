import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


event = GenieEvent(Topic.NEW_UPCOMING_MEETING, {"meeting_uuid": "0a313a8b-1c26-425f-b490-8cbebb6eb4bf"})
event.send()
