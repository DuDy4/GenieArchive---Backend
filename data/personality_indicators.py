from typing import List, Union
from pydantic import BaseModel, Field, validator

class PersonalityIndicator(BaseModel):
    indicator: str
    rank: int
    sources: List[str]
    reasoning: str
    value: Union[str, int, float, List[str], None] = Field(None)

    class Config:
        allow_mutation = True

class PersonalityIndicators(BaseModel):
    work_history: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Work History",
            rank=5,
            sources=["LinkedIn"],
            reasoning="Roles and responsibilities strongly influence strengths."
        )
    )
    education_background: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Education Background",
            rank=4,
            sources=["LinkedIn", "Academic Transcripts"],
            reasoning="Type and level of education provide insight into natural strengths."
        )
    )
    public_speaking_engagements: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Public Speaking Engagements",
            rank=4,
            sources=["Event Websites", "YouTube"],
            reasoning="Participation in public speaking indicates communication and influence strengths."
        )
    )
    published_articles: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Published Articles or Blog Posts",
            rank=5,
            sources=["Personal Blog", "Medium"],
            reasoning="Writing reveals areas of expertise and strong interests."
        )
    )
    social_media_activity: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Social Media Activity",
            rank=4,
            sources=["Twitter", "LinkedIn"],
            reasoning="Engagement on social media provides insights into strengths and interests."
        )
    )
    linkedin_recommendations: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="LinkedIn Recommendations",
            rank=4,
            sources=["LinkedIn"],
            reasoning="Endorsements and recommendations highlight perceived strengths."
        )
    )
    awards_recognitions: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Awards and Recognitions",
            rank=4,
            sources=["Company Websites", "News Articles"],
            reasoning="Professional accolades indicate areas of excellence."
        )
    )
    professional_network: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Professional Network",
            rank=4,
            sources=["LinkedIn"],
            reasoning="A diverse professional network indicates relationship-building strengths."
        )
    )
    industry_conferences_attended: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Industry Conferences Attended",
            rank=3,
            sources=["Event Websites", "Professional Associations"],
            reasoning="Participation in industry events highlights areas of interest."
        )
    )
    books_papers_authored: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Books or Papers Authored",
            rank=5,
            sources=["Google Scholar", "Personal Blog"],
            reasoning="Authorship indicates deep expertise and analytical strength."
        )
    )
    professional_organizations: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Membership in Professional Organizations",
            rank=3,
            sources=["Professional Associations"],
            reasoning="Involvement in professional groups highlights commitment to learning."
        )
    )
    volunteer_work: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Volunteer Work and Community Involvement",
            rank=3,
            sources=["Volunteer Organizations", "Social Media"],
            reasoning="Volunteer activities indicate strengths related to service."
        )
    )
    career_goals: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Career Goals and Aspirations",
            rank=4,
            sources=["LinkedIn"],
            reasoning="Publicly shared career goals provide insights into motivations and strengths."
        )
    )
    webinars_online_courses: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Participation in Webinars or Online Courses",
            rank=3,
            sources=["Online Learning Platforms (Coursera, Udemy)"],
            reasoning="Involvement in continuous education highlights areas of interest."
        )
    )
    patents_inventions: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Patents or Inventions",
            rank=4,
            sources=["Patent Databases (Google Patents, USPTO)"],
            reasoning="Patents or inventions indicate creativity and innovation strengths."
        )
    )
    professional_certifications: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Professional Certifications and Licenses",
            rank=3,
            sources=["LinkedIn", "Certification Bodies"],
            reasoning="Certifications highlight expertise and strengths in specific areas."
        )
    )
    interview_transcripts: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Interview Transcripts or Videos",
            rank=4,
            sources=["YouTube", "Company Websites"],
            reasoning="Interviews provide direct insights into thoughts and strengths."
        )
    )
    conference_presentations: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Conference Presentations",
            rank=4,
            sources=["Event Websites", "YouTube"],
            reasoning="Conference presentations indicate public speaking and expertise."
        )
    )
    github_contributions: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="GitHub Contributions",
            rank=4,
            sources=["GitHub"],
            reasoning="Open-source contributions highlight technical skills and collaboration."
        )
    )
    personal_website_testimonials: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Testimonials on Personal Website",
            rank=3,
            sources=["Personal Website", "LinkedIn"],
            reasoning="Testimonials provide insights into perceived strengths."
        )
    )
    professional_portfolio: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Professional Portfolio",
            rank=4,
            sources=["Personal Website", "Behance"],
            reasoning="Portfolios highlight creativity, execution, and professional skills."
        )
    )
    podcasts_webinars_hosted: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Podcasts or Webinars Hosted",
            rank=4,
            sources=["Podcast Platforms", "YouTube"],
            reasoning="Hosting events indicates communication and leadership strengths."
        )
    )
    online_communities: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Participation in Online Communities",
            rank=3,
            sources=["Professional Forums (Stack Overflow, Reddit)"],
            reasoning="Participation in online communities highlights knowledge sharing."
        )
    )
    google_scholar_citations: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Google Scholar Citations",
            rank=3,
            sources=["Google Scholar"],
            reasoning="Citations indicate recognition and research strengths."
        )
    )
    competitions_hackathons: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Participation in Competitions or Hackathons",
            rank=3,
            sources=["Event Websites", "Hackathon Listings"],
            reasoning="Competitive events highlight problem-solving and innovation strengths."
        )
    )
    residence: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Residence",
            rank=2,
            sources=["Public Records"],
            reasoning="Location influences experiences and strengths."
        )
    )
    height: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Height",
            rank=1,
            sources=["N/A"],
            reasoning="Physical height has negligible impact on strengths."
        )
    )
    companies_worked_for: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Type of Companies Worked For",
            rank=3,
            sources=["LinkedIn", "Company Websites"],
            reasoning="Nature and culture of previous employers influence strengths development."
        )
    )
    age: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Age",
            rank=2,
            sources=["Public Records"],
            reasoning="Age provides context for experience level."
        )
    )
    gender: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Gender",
            rank=2,
            sources=["N/A"],
            reasoning="Gender provides some contextual information but minor impact."
        )
    )
    marital_status: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Marital Status",
            rank=1,
            sources=["Public Records"],
            reasoning="Marital status has negligible impact on strengths."
        )
    )
    industry_experience: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Industry Experience",
            rank=3,
            sources=["LinkedIn", "Company Websites"],
            reasoning="Experience in specific industries highlights relevant strengths."
        )
    )
    years_of_experience: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Years of Experience",
            rank=3,
            sources=["LinkedIn"],
            reasoning="Years of experience provide context but are not primary determinants."
        )
    )
    language_proficiency: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Language Proficiency",
            rank=3,
            sources=["LinkedIn", "Language Certification Bodies"],
            reasoning="Proficiency in multiple languages indicates communication strengths."
        )
    )
    certifications_licenses: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Certifications and Licenses",
            rank=4,
            sources=["LinkedIn", "Certification Bodies"],
            reasoning="Certifications indicate strengths in specific areas of expertise."
        )
    )
    professional_development_courses: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Professional Development Courses",
            rank=4,
            sources=["Online Learning Platforms"],
            reasoning="Participation in professional development courses highlights commitment to growth."
        )
    )
    mentorship_roles: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Mentorship Roles",
            rank=3,
            sources=["LinkedIn", "Company Websites"],
            reasoning="Mentorship roles indicate leadership and development strengths."
        )
    )
    company_size: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Company Size",
            rank=3,
            sources=["LinkedIn", "Company Websites"],
            reasoning="Company size influences experiences and strength development."
        )
    )
    job_titles_held: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Job Titles Held",
            rank=4,
            sources=["GitHub"],
            reasoning="Job titles held provide insight into career progression and strengths."
        )
    )
    open_source_contributions: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Open Source Contributions",
            rank=3,
            sources=["Client Websites", "Testimonials Sections"],
            reasoning="Open-source contributions indicate technical expertise and collaboration."
        )
    )
    client_testimonials: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Client Testimonials",
            rank=3,
            sources=["Company Websites", "LinkedIn"],
            reasoning="Client testimonials highlight strengths from external perspectives."
        )
    )
    board_memberships: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Board Memberships",
            rank=3,
            sources=["Community Organization Websites"],
            reasoning="Board memberships indicate leadership and strategic strengths."
        )
    )
    community_leadership_roles: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Community Leadership Roles",
            rank=4,
            sources=["LinkedIn"],
            reasoning="Community leadership roles highlight service and organizational strengths."
        )
    )
    peer_reviews: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Peer Reviews",
            rank=3,
            sources=["YouTube", "Company Websites"],
            reasoning="Peer reviews provide insights into perceived strengths by colleagues."
        )
    )
    interview_performance: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Interview Performance",
            rank=4,
            sources=["Company Websites", "Research Journals"],
            reasoning="Interview performance indicates communication and presentation strengths."
        )
    )
    published_case_studies: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Published Case Studies",
            rank=4,
            sources=["Personal Blog", "Medium"],
            reasoning="Published case studies highlight problem-solving and analytical strengths."
        )
    )
    professional_blog_posts: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Professional Blog Posts",
            rank=3,
            sources=["Research Journals", "Google Scholar"],
            reasoning="Professional blog posts reveal areas of expertise."
        )
    )
    research_contributions: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Research Contributions",
            rank=3,
            sources=["LinkedIn", "Company Websites"],
            reasoning="Research contributions indicate analytical and innovative strengths."
        )
    )
    collaborations_partnerships: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Collaborations and Partnerships",
            rank=4,
            sources=["Community Organization Websites"],
            reasoning="Collaborations and partnerships highlight teamwork and relationship strengths."
        )
    )
    cultural_activities_involvement: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Cultural Activities Involvement",
            rank=3,
            sources=["News Websites", "YouTube"],
            reasoning="Involvement in cultural activities indicates strengths in diversity and inclusion."
        )
    )
    media_appearances: PersonalityIndicator = Field(
        PersonalityIndicator(
            indicator="Media Appearances",
            rank=3,
            sources=["Community Organization Websites", "Social Media"],
            reasoning="Media appearances highlight public influence and communication strengths."
        )
    )

    @validator('*', pre=True, each_item=True)
    def set_indicator_value(cls, value, field):
        if isinstance(value, dict):
            return PersonalityIndicator(**value)
        return value

# Example usage
class Person(BaseModel):
    name: str
    personality_indicators: PersonalityIndicators

# Create a person with specific values for their personality indicators
person_a = Person(
    name="John Doe",
    personality_indicators={
        "work_history": {"value": "Senior Developer at TechCorp"},
        "education_background": {"value": "B.Sc. in Computer Science from MIT"},
        "age": {"value": 34}
        # Add values for other indicators as needed...
    }
)

print(person_a)
print(person_a.personality_indicators.age.value)  # Output: 34
