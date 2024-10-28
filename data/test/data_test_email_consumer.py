import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


event = GenieEvent(Topic.NEW_UPCOMING_MEETING, {"meeting_uuid": "d70bed58-7e4e-457a-898d-83db742640ab"})
event.send()
