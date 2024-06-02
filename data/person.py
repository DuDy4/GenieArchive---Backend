import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ai.langsmith.langsmith_loader import Langsmith
from common.utils.json_utils import json_to_python
from common.events.topics import Topic
from common.events.genie_consumer import GenieConsumer

class Person(GenieConsumer):
    def __init__(self):
        super().__init__(topics=[Topic.NEW_CONTACT])
        self.langsmith = Langsmith()

    async def process_event(self, event):
        print(f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}")
        response = self.langsmith.run_prompt_test(event.body_as_str())
        print(f"Response: {response}")
        strength = json_to_python(response)
        print(f"Strength: {strength}")
        return response

if __name__ == "__main__":
    person = Person()
    person.run()