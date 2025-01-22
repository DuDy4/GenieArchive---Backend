import sys
import os

from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.profile_dto import Strength
from data.data_common.events.topics import Topic
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.dependencies.dependencies import (
    meetings_repository,
)

from common.genie_logger import GenieLogger
logger = GenieLogger()

def create_fake_data():
    person = {'uuid': '00a64c11-da7d-45dc-bde5-dd6e30e5f0d2', 'name': 'John Sva', 'company': 'Microsoft', 'email': 'john.sva@microsoft.com', 'linkedin': 'linkedin.com/in/johnsva72', 'position': 'Chief Technology Officer', 'timezone': ''}
    strengths = [{'strength_name': 'Strategic', 'score': 95, 'reasoning': "John's role as a Chief Technology Officer requires strategic thinking to align technology initiatives with business goals. His experience in various leadership roles at Microsoft and Samsung demonstrates his ability to foresee potential challenges and opportunities, making strategic decisions that benefit the organization."}, {'strength_name': 'Learner', 'score': 90, 'reasoning': "John's educational background, including a Master's degree in Business Administration and a Bachelor's in Computer Science, along with his teaching role at Tel Aviv University, indicates a strong inclination towards learning and disseminating knowledge. His continuous engagement in emerging technologies showcases his passion for learning."}, {'strength_name': 'Achiever', 'score': 92, 'reasoning': "John's career trajectory, from software engineer to CTO, evidences a drive to achieve. His involvement in significant projects and leadership roles at tech giants like Microsoft and Samsung further reflects his determination and commitment to accomplishing goals."}, {'strength_name': 'Activator', 'score': 88, 'reasoning': 'As a co-founder of Stepitapp and a leader in innovation at multiple firms, John has shown the ability to turn ideas into action. His role in product management and rapid prototyping underscores his capability to initiate projects and lead them to fruition.'}, {'strength_name': 'Futuristic', 'score': 91, 'reasoning': "John's involvement in emerging technologies and his role as a Global Emerging Technologies Lead suggest a forward-thinking mindset. His work in virtual and augmented reality, as well as artificial intelligence, highlights his vision for the future and his ability to inspire others with this vision."}]
    avoid = [{'phrase_text': "Let's focus solely on AI and disregard cloud services.", 'confidence_score': 90, 'reasoning': "Given Microsoft's strategic interest in both AI and cloud services as critical areas, disregarding one would neglect key aspects of their competitive strategy."}, {'phrase_text': "We don't need to consider cybersecurity threats much.", 'confidence_score': 85, 'reasoning': 'As Microsoft is a prime target for cyber-attacks, undermining the importance of cybersecurity could pose significant risks.'}, {'phrase_text': "Let's not worry about regulatory compliance.", 'confidence_score': 80, 'reasoning': 'Navigating global regulations, especially related to data privacy, is crucial for Microsoft, and dismissing it could lead to significant legal challenges.'}]
    best_practices = [{'phrase_text': 'Align our solution with your strategic goals in AI and cloud.', 'confidence_score': 95, 'reasoning': "John's strategic role at Microsoft emphasizes aligning technology initiatives with business goals, particularly in AI and cloud services, which are critical to Microsoft's competitive strategy."}, {'phrase_text': 'Emphasize our integration capabilities for emerging technologies.', 'confidence_score': 90, 'reasoning': "John's role as a Global Emerging Technologies Lead highlights the importance of integrating new technologies seamlessly, a key factor in Microsoft's innovation strategy."}, {'phrase_text': 'Focus on enhancing cybersecurity measures in our offerings.', 'confidence_score': 88, 'reasoning': "Given Microsoft's emphasis on robust security measures due to their vulnerability to cyber-attacks, focusing on cybersecurity enhancements would align with their strategic priorities."}]
    phrases_to_use = [{'phrase_text': 'Our solution enhances both AI and cloud functionalities.', 'confidence_score': 93, 'reasoning': 'As Microsoft aims to remain competitive in both AI and cloud services, a solution that enhances both would be highly relevant to their strategic interests.'}, {'phrase_text': 'We offer tailored integration with emerging tech for startups.', 'confidence_score': 92, 'reasoning': "John's involvement with Microsoft for Startups and emerging technologies suggests a focus on tailored solutions that integrate new tech effectively."}, {'phrase_text': 'Our services prioritize top-tier cybersecurity standards.', 'confidence_score': 89, 'reasoning': "Given Microsoft's focus on cybersecurity due to being a prime target for attacks, emphasizing top-tier standards would resonate with their priorities."}]
    get_to_know = {
        'avoid': avoid,
        'best_practices': best_practices,
        'phrases_to_use': phrases_to_use
    }
    profile = {
        # 'strengths': [Strength.from_dict(strength) for strength in strengths],
        'strengths': strengths,
        'get_to_know': get_to_know
    }
    data_to_send = {
        'person': person,
        'profile': profile
    }

    logger.set_tenant_id('org_RPLWQRTI8t7EWU1L')
    logger.set_user_id('google-oauth2|102736324632194671211')

    return data_to_send



def test_base_profile(data_to_send):
    event = GenieEvent(Topic.NEW_BASE_PROFILE, data_to_send, "public")
    event.send()

data_to_send = create_fake_data()
test_base_profile(data_to_send)


