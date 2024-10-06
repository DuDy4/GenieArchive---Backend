import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

def test_new_contacts():
    # Test data includes a UUID, LinkedIn URL, and other fields
    test_data = """{
        "uuid": "592ff140-91c2-475e-8bc2-1ce23328896c", 
        "name": "", 
        "company": "", 
        "email": "gal.a@alignedup.com", 
        "linkedin": "https://www.linkedin.com/in/galaga/", 
        "position": "", 
        "timezone": ""
    }"""

    # Simulate the event with FETCH_NEWS topic and test data
    event = GenieEvent(topic=Topic.FETCH_NEWS, data=test_data)
    
    # Send the event for processing
    event.send()

# Run the test
test_new_contacts()
