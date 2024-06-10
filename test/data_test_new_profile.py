import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.events.genie_event import GenieEvent
from common.events.topics import Topic


def test_new_contact():
    person = """{"uuid": "e58a0044-c276-4c1c-a63e-5a57d79ccb8b", "name": "Asaf Savich", "company": "GenieAI", "email": "asaf@trywonder.ai", "linkedin": "https://www.linkedin.com/in/asaf-savich/", "position": "CTO", "timezone": ""}"""
    profile = """{'new_strengths': [{'strengths_name': 'Leadership', 'score': 95, 'reasoning': "Asaf's role as Director of Engineering at multiple companies showcases his strong leadership abilities. His experience in leading teams and managing projects indicates his capability to inspire and guide others towards achieving common goals. For instance, his tenure at Kubiya.ai and Mend.io as a director demonstrates his proficiency in leadership."}, {'strengths_name': 'Technical Expertise', 'score': 90, 'reasoning': 'With a background in software development and a wide range of skills including Java, C#, Android Development, and more, Asaf possesses strong technical expertise. His ability to work on various technical projects and his proficiency in multiple programming languages highlight his deep understanding of the technical domain.'}, {'strengths_name': 'Strategic Thinking', 'score': 88, 'reasoning': "Asaf's positions, particularly as a director, require strategic planning and execution. His ability to oversee engineering operations and align them with business goals indicates a strong aptitude for strategic thinking. For example, his role in leading engineering teams at Kubiya.ai involves setting long-term objectives and finding innovative solutions."}, {'strengths_name': 'Problem Solving', 'score': 85, 'reasoning': "Asaf's extensive experience in software development and quality assurance suggests a strong problem-solving ability. His role in test automation and integration testing requires identifying issues and implementing effective solutions. His work in software design and quality assurance at various companies exemplifies his problem-solving skills."}, {'strengths_name': 'Team Collaboration', 'score': 82, 'reasoning': 'Working in different capacities, from a software engineer to a director, Asaf has demonstrated the ability to collaborate effectively with team members. His experience as a backend team leader and lead software engineer at Mend.io indicates his capability to work well within a team and drive collective success.'}, {'strengths_name': 'Adaptability', 'score': 80, 'reasoning': "Asaf's career shows a pattern of adapting to various roles and responsibilities across different industries, from computer software to medical devices. His ability to transition from software testing roles to leadership positions displays his adaptability and willingness to take on new challenges."}, {'strengths_name': 'Project Management', 'score': 78, 'reasoning': "Asaf's experience as a director involves managing complex projects and ensuring their successful completion. His role in overseeing engineering projects at Kubiya.ai and Mend.io demonstrates his project management skills, including planning, execution, and monitoring of project progress."}, {'strengths_name': 'Innovation', 'score': 75, 'reasoning': "Asaf's work in research and development, particularly at Mend.io and Safedk, highlights his innovative mindset. His involvement in developing new solutions and improving existing processes shows his ability to think creatively and contribute to technological advancements."}]}"""
    data_to_send = {"person": person, "profile": profile}
    event = GenieEvent(
        topic=Topic.NEW_PROCESSED_PROFILE, data=data_to_send, scope="public"
    )
    event.send()


test_new_contact()
