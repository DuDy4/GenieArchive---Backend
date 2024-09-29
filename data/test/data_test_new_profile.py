import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_new_profile():
    person = """{"uuid": "ThisIsATest", "name": "Asaf Savich", "company": "GenieAI", "email": "asaf@trywonder.ai", "linkedin": "https://www.linkedin.com/in/asaf-savich/", "position": "CTO", "timezone": ""}"""
    profile = """{"strengths": [
    {"strength_name": "Leadership", "score": 95, "reasoning": "Asaf's role as Director of Engineering at multiple companies showcases his strong leadership abilities. His experience in leading teams and managing projects indicates his capability to inspire and guide others towards achieving common goals. For instance, his tenure at Kubiya.ai and Mend.io as a director demonstrates his proficiency in leadership."}, {"strengths_name": "Technical Expertise", "score": 90, "reasoning": "With a background in software development and a wide range of skills including Java, C#, Android Development, and more, Asaf possesses strong technical expertise. His ability to work on various technical projects and his proficiency in multiple programming languages highlight his deep understanding of the technical domain."},
    {"strength_name": "Strategic Thinking", "score": 88, "reasoning": "Asaf's positions, particularly as a director, require strategic planning and execution. His ability to oversee engineering operations and align them with business goals indicates a strong aptitude for strategic thinking. For example, his role in leading engineering teams at Kubiya.ai involves setting long-term objectives and finding innovative solutions."}, {"strengths_name": "Problem Solving", "score": 85, "reasoning": "Asaf's extensive experience in software development and quality assurance suggests a strong problem-solving ability. His role in test automation and integration testing requires identifying issues and implementing effective solutions. His work in software design and quality assurance at various companies exemplifies his problem-solving skills."},
    {"strength_name": "Team Collaboration", "score": 82, "reasoning": "Working in different capacities, from a software engineer to a director, Asaf has demonstrated the ability to collaborate effectively with team members. His experience as a backend team leader and lead software engineer at Mend.io indicates his capability to work well within a team and drive collective success."}, {"strengths_name": "Adaptability", "score": 80, "reasoning": "Asaf's career shows a pattern of adapting to various roles and responsibilities across different industries, from computer software to medical devices. His ability to transition from software testing roles to leadership positions displays his adaptability and willingness to take on new challenges."},
    {"strength_name": "Project Management", "score": 78, "reasoning": "Asaf's experience as a director involves managing complex projects and ensuring their successful completion. His role in overseeing engineering projects at Kubiya.ai and Mend.io demonstrates his project management skills, including planning, execution, and monitoring of project progress."},
    {"strength_name": "Innovation", "score": 75, "reasoning": "Asaf's work in research and development, particularly at Mend.io and Safedk, highlights his innovative mindset. His involvement in developing new solutions and improving existing processes shows his ability to think creatively and contribute to technological advancements."}],
    "hobbies": ["a79d4460-8d40-4f30-88a1-b7d0eb3ff2ed", "3ea1cfbc-d2cd-43a8-841a-818e9ea779c5"],
    "picture_url": "https://cdn.theorg.com/53f0ea75-a81d-4165-9b25-3650bf46a7ab_thumb.jpg",
    "connections": ["de19a684-7ded-4de5-b88b-6bc712e3497d", "e5d5726a-4293-49c5-ae5b-4b146a539e8b"],
    "news": [
            {"news_title": "InnovateTech Launches New Product", "news_url": "https://www.techcrunch.com", "news_icon": "https://img.icons8.com/bubbles/50/news.png"},
            {"news_title": "Emma Johnson in Forbes 30 Under 30", "news_url": "https://www.forbes.com", "news_icon": "https://img.icons8.com/bubbles/50/news.png"}
        ],
    "summary": "Asaf is a seasoned software engineer with a strong background in software development and quality assurance. He has held various roles in the software industry, including software engineer, lead software engineer, and director of engineering. Asaf has experience working on a wide range of projects, from developing software solutions to managing engineering teams. His expertise in software development, quality assurance, and project management makes him a valuable asset to any organization.",
    "get_to_know": {
  "avoid": [
    {
      "reasoning": "Asaf’s experience is primarily with large corporations. A focus on startups won't align with his background and challenges at GenieAI.",
      "phrase_text": "Our platform is perfect for startups looking to quickly scale their testing efforts.",
      "confidence_score": 95
    },
    {
      "reasoning": "Given his role at a major corporation, Asaf likely deals with large-scale operations and complex projects, making this statement irrelevant.",
      "phrase_text": "Our tool is great for small development teams wanting to dip their toes into automated testing.",
      "confidence_score": 90
    },
    {
      "reasoning": "Asaf’s extensive experience in different sectors means he will value tailored, specific solutions over generic claims.",
      "phrase_text": "It's a one-size-fits-all solution for all your testing needs.",
      "confidence_score": 85
    },
    {
      "reasoning": "Asaf understands the intricacies of implementing new technologies and will know that true value requires some level of team involvement and effort.",
      "phrase_text": "You’ll see immediate results without any effort from your team.",
      "confidence_score": 80
    }
  ],
  "best_practices": [
    {
      "reasoning": "Asaf has strong experience with cloud-based business value analysis, making integrations highly relevant.",
      "what_to_do": "Emphasize the integration capabilities of GenieAI with existing CI/CD tools like Jenkins and GitHub.",
      "confidence_score": 100
    },
    {
      "reasoning": "This addresses his focus on efficiency and quality engineering, reducing time spent on manual tasks.",
      "what_to_do": "Highlight GenieAI’s ability to reduce manual test maintenance through AI-powered testing.",
      "confidence_score": 95
    },
    {
      "reasoning": "Asaf leads global teams and would appreciate tools that facilitate collaboration and transparency.",
      "what_to_do": "Discuss the collaborative features of GenieAI, emphasizing detailed reporting and easy sharing of test scenarios.",
      "confidence_score": 90
    },
    {
      "reasoning": "Concrete examples and success stories will resonate with his strategic and project management skills.",
      "what_to_do": "Present real-world examples of how GenieAI has helped other large organizations maintain high-quality standards through end-to-end testing.",
      "confidence_score": 85
    }
  ],
  "phrases_to_use": [
    {
      "reasoning": "Asaf has extensive experience in DevOps and cloud-based business value analysis, making the integration aspect highly relevant and appealing.",
      "phrase_text": "Our platform integrates seamlessly with tools like Jenkins and GitHub, which are likely part of your current development pipeline.",
      "confidence_score": 95
    },
    {
      "reasoning": "Given Asaf's background in quality engineering and managing large teams, emphasizing efficiency and reducing manual workload will resonate well with him.",
      "phrase_text": "GenieAI’s AI-powered capabilities can significantly reduce the manual effort required in maintaining tests, allowing your team to focus on higher-priority tasks.",
      "confidence_score": 90
    },
    {
      "reasoning": "Asaf has led global teams and is adept at de-mystifying technology. Highlighting collaboration and detailed reporting aligns with his strengths.",
      "phrase_text": "With GenieAI, you can maintain a high standard of quality within your distributed teams, as our platform supports collaboration and detailed reporting.",
      "confidence_score": 85
    },
    {
      "reasoning": "End-to-end testing is crucial for maintaining high standards, something Asaf has focused on throughout his career in quality engineering.",
      "phrase_text": "Our end-to-end testing support is designed to cover complex user journeys, which can enhance the comprehensive quality checks you are implementing at GenieAI.",
      "confidence_score": 80
    }
  ]
}"""
    data_to_send = {"person": person, "profile": profile}
    event = GenieEvent(topic=Topic.NEW_PROCESSED_PROFILE, data=data_to_send)
    event.send()


test_new_profile()
