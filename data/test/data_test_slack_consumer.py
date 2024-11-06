from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

genie = GenieEvent(
    Topic.APOLLO_FAILED_TO_ENRICH_PERSON,
    {"person":
         {"uuid": "6cbcaba1-9521-464b-a6b7-5b4c00525347", "name": "", "company": "",
                "email": "dan.shevel@trywonder.ai", "linkedin": "", "position": "", "timezone": ""},
     "email": "dan.shevel@trywonder.ai"}
)
genie.send()
