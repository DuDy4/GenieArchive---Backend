import datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

class CalendarInsight:
    SCOPES = ['https://www.googleapis.com/auth/calendar']

    def __init__(self, calendar_id):
        self.calendar_id = calendar_id


    def get_upcoming_events(self, creds):
        # Build the Google Calendar API service
        service = build('calendar', 'v3', credentials=creds)

        # Get upcoming events
        now = datetime.datetime.utcnow().isoformat() + 'Z'  # 'Z' indicates UTC time
        events_result = service.events().list(calendarId=self.calendar_id, timeMin=now,
                                              maxResults=10, singleEvents=True,
                                              orderBy='startTime').execute()
        events = events_result.get('items', [])
        print(events)

        # Print event details
        for event in events:
            start = event['start'].get('dateTime', event['start'].get('date'))
            print(f"{start} - {event['summary']}")
            for attendee in event.get('attendees', []):
                print(f" - {attendee['email']}")
