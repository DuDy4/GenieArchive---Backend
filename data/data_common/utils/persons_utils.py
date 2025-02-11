from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from data.data_common.data_transfer_objects.profile_category_dto import ProfileCategory, SalesCriteriaType, SalesCriteria

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from ai.train.profile_param_weights import ProfileParamWeights

personal_data_repository = PersonalDataRepository()
companies_repository = CompaniesRepository()
profile_param_wights = ProfileParamWeights()

logger = GenieLogger()

profiles = ["The Analytical", "The Social", "The Innovator", "The Thorough", "The Go-Getter", "The Intuitive"]

sales_criteria_mapping = {
    profiles[0]: [
        SalesCriteria(criteria=SalesCriteriaType.TECHNICAL_FIT, score=0, target_score=40),
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=40),
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=20),],
    profiles[1]: [
        SalesCriteria(criteria=SalesCriteriaType.TRUST, score=0, target_score=40),
        SalesCriteria(criteria=SalesCriteriaType.REPUTATION, score=0, target_score=20),
        SalesCriteria(criteria=SalesCriteriaType.LONG_TERM_PROFESSIONAL_ADVISOR, score=0, target_score=40),],
    profiles[2]: [
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=20),
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=30),
        SalesCriteria(criteria=SalesCriteriaType.INNOVATION, score=0, target_score=50),],
    profiles[3]: [
        SalesCriteria(criteria=SalesCriteriaType.TECHNICAL_FIT, score=0, target_score=30),
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=20),
        SalesCriteria(criteria=SalesCriteriaType.REPUTATION, score=0, target_score=50),],
    profiles[4]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=30),
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=40),
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=20),],
    profiles[5]: [
        SalesCriteria(criteria=SalesCriteriaType.TECHNICAL_FIT, score=0, target_score=25),
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=25),
        SalesCriteria(criteria=SalesCriteriaType.INNOVATION, score=0, target_score=50),],
}

criteria_icon_mapping = {
    SalesCriteriaType.BUDGET: "https://img.icons8.com/ios/50/money-box--v1.png",
    SalesCriteriaType.TRUST: "https://img.icons8.com/ios/50/handshake--v1.png",
    SalesCriteriaType.TECHNICAL_FIT: "https://img.icons8.com/ios/50/gears--v1.png",
    SalesCriteriaType.BUSINESS_FIT: "https://img.icons8.com/ios/50/puzzle-matching.png",
    SalesCriteriaType.VALUE_PROPOSITION: "https://img.icons8.com/ios/50/diamond-care-1.png",
    SalesCriteriaType.INNOVATION: "https://img.icons8.com/ios/50/light-on--v1.png",
    SalesCriteriaType.REPUTATION: "https://img.icons8.com/external-bearicons-detailed-outline-bearicons/64/external-reputation-reputation-bearicons-detailed-outline-bearicons.png",
    SalesCriteriaType.LONG_TERM_PROFESSIONAL_ADVISOR: "https://img.icons8.com/ios/50/education.png",
    SalesCriteriaType.RESPONSIVENESS: "https://img.icons8.com/dotty/80/running.png",
}

strengths_mapping = {
    # "Achiever": [4, 6, 1, 3, 7, 2, 5],
    "Achiever":            [7, 7, 7, 7, 7, 7],
    "Activator":           [5, 7, 3, 6, 1, 4],
    "Adaptability":        [6, 2, 3, 6, 3, 3],
    "Analytical":          [1, 10, 6, 2, 5, 3],
    "Arranger":            [5, 4, 7, 1, 6, 2],
    "Belief":              [7, 1, 3, 4, 2, 5],
    "Command":             [6, 3, 5, 7, 1, 4],
    "Communication":       [6, 1, 4, 10, 2, 3],
    "Competition":         [5, 7, 4, 10, 1, 3],
    "Connectedness":       [7, 1, 2, 6, 4, 3],
    "Consistency":         [2, 1, 10, 6, 4, 5],
    "Context":             [2, 10, 10, 3, 5, 4],
    "Deliberative":        [2, 6, 3, 1, 4, 5],
    "Developer":           [10, 1, 2, 3, 10, 10],
    "Discipline":          [4, 10, 6, 5, 1, 3],
    "Empathy":             [10, 1, 2, 10, 10, 10],
    "Focus":               [3, 10, 10, 5, 1, 4],
    "Futuristic":          [10, 3, 1, 10, 6, 2],
    "Harmony":             [10, 2, 10, 1, 10, 10],
    "Ideation":            [10, 10, 1, 10, 10, 2],
    "Includer":            [10, 1, 10, 10, 2, 10],
    "Individualization":   [3, 1, 10, 10, 5, 4],
    "Input":               [1, 7, 3, 4, 6, 2],
    "Intellection":        [4, 3, 2, 7, 6, 1],
    "Learner":             [5, 7, 4, 5, 6, 3],
    "Maximizer":           [4, 1, 10, 10, 2, 5],
    "Positivity":          [10, 1, 2, 10, 10, 10],
    "Relator":             [5, 1, 4, 3, 7, 2],
    "Responsibility":      [3, 6, 10, 4, 2, 5],
    "Restorative":         [5, 7, 2, 4, 6, 1],
    "Self-Assurance":      [3, 10, 10, 7, 1, 10],
    "Significance":        [6, 7, 3, 5, 1, 2],
    "Strategic":           [7, 7, 7, 7, 7, 7],
    "Woo":                 [5, 1, 3, 10, 2, 10],
}

profiles_description = {
    "The Analytical": "Seeks in-depth understanding and relies on data, facts, and logic. They value detailed explanations and proof over generalities.",
    "The Social": "Values personal connections and reliable relationships. They prioritize feeling comfortable and being heard, focusing on trust and empathy.",
    "The Innovator": "Innovation-driven and creative. They value novelty, inspiration, and products that bring unique advantages or change the market landscape.",
    "The Thorough": "Cautious and detail-oriented. They look for potential flaws, requiring proof, guarantees, and transparency to overcome their natural hesitation.",
    "The Go-Getter": "Results-oriented and decisive. They prefer clear, concise communication and focus on how the product solves immediate problems with measurable outcomes.",
    "The Intuitive": "Follows their gut feeling quite often. Enthusiastic about new ideas and products that resonate with their emotions and values.",
}

profiles_extended_description = {
    "The Analytical": (
        "An Analytical profile is characterized by a deep need for understanding, "
        "grounded in facts, data, and logic. Individuals with this profile are driven by "
        "precision and clarity, seeking in-depth, evidence-based insights to evaluate options "
        "and make decisions. They appreciate structured, well-supported arguments and take time "
        "to analyze all available information."
    ),
    "The Social": (
        "A Social profile is defined by a strong focus on building personal connections and "
        "fostering trust. These individuals value relationships and seek meaningful, empathetic "
        "interactions. They prefer working with those who take the time to understand their unique "
        "needs, creating a sense of comfort and confidence in the process."
    ),
    "The Innovator": (
        "An Innovator profile is marked by a fascination with creativity, unique solutions, and forward-thinking "
        "ideas. These individuals are drawn to products or concepts that stand out, offer added value, and push boundaries. "
        "They thrive on inspiration and enjoy exploring the transformative potential of innovative approaches."
    ),
    "The Thorough": (
        "A Thorough profile is defined by a cautious and discerning approach, emphasizing risk assessment and reliability. "
        "These individuals tend to question assumptions, look for flaws, and require reassurance before committing to decisions. "
        "They value transparency, realistic guarantees, and thorough validation of a product's capabilities."
    ),
    "The Go-Getter": (
        "A Go-Getter profile is characterized by a results-oriented mindset and a preference for decisive, "
        "quick actions. These individuals focus on achieving tangible outcomes and maintaining control "
        "throughout the process. They value efficiency, clarity, and directness, seeking clear paths to "
        "solve immediate problems and reach their goals."
    ),
    "The Intuitive": (
        "An Intuitive profile is defined by a strong reliance on feelings, intuition, and personal values. "
        "These individuals are driven by emotional resonance and seek products or ideas that align with their "
        "core beliefs and aspirations. They value authenticity, enthusiasm, and emotional connections in their interactions."
    ),
}

profiles_explanation = {
    "The Analytical": {
        "characteristics": "Seeks in-depth understanding of the product, looks for data, facts, proof, and logic",
        "needs": "Requires precise details and answers to technical questions.",
        "recommendations": "Provide presentations with numerical data, examples or proof of the product's success, detailed explanations, and structured answers to complex questions. This customer values time for thinking and drawing conclusions, and will not make decisions quickly.",
    },
    "The Social": {
        "characteristics": "Looks for personal connection and trustworthy relationships, wants to feel comfortable and heard.",
        "needs": "Requires personalized attention and trust throughout the process.",
        "recommendations": "Approach them personally, take time to understand their specific needs, emphasize post-sale support and service, and create a warm, empathetic conversation. This customer responds well to confidence and a sense of care."
    },
    "The Innovator": {
        "characteristics": "Values innovation, creative ideas, and products that offer added value and uniqueness.",
        "needs": "Craves inspiration, creative ideas, and excitement about the product.",
        "recommendations": "Share innovative ideas and the unique benefits of the product, demonstrate its ability to change the market, and provide examples of diverse uses. This customer will respond well to a captivating, inspiring presentation and the impact the product will have on the future."
    },
    "The Thorough": {
        "characteristics": "Tends to be cautious, looks for problems or flaws in the product, and needs reassurance.",
        "needs": "Requires proof and guarantees that the product will meet their expectations.",
        "recommendations": "Provide testimonials, reviews, previous successes, and demonstrations. Be prepared to answer numerous questions and allow for open, detailed conversations. This customer wants to ensure they’re not taking unnecessary risks and will need a realistic and transparent guarantee of the product’s value."
    },
    "The Go-Getter": {
        "characteristics": "Focused on results, aims to make decisions quickly, and prefers to maintain control in the process.",
        "needs": "Wants to see how the product solves immediate problems and seeks a clear business advantage.",
        "recommendations": "Be direct, clear, and to the point. Highlight the product’s value and results, showcase competitive advantages, and avoid unnecessary details. Allow them to feel in control, but provide a clear picture of how the product will help achieve their goals."
    },
    "The Intuitive": {
        "characteristics": "Driven by emotions, intuition, and personal values, seeks products that resonate with their beliefs.",
        "needs": "Requires a strong emotional connection to the product and values authenticity and enthusiasm.",
        "recommendations": "Appeal to their emotions, values, and personal beliefs. Share stories, testimonials, and examples that evoke positive feelings and demonstrate the product’s authenticity. This customer will respond well to a warm, enthusiastic approach and a genuine connection."
    },
}

profiles_colors = {
    "The Analytical": "#fed101",  # Yellow
    "The Social": "#2b75de",      # Blue
    "The Innovator": "#57cc99",   # Green
    "The Thorough": "#444c5f",     # Black
    "The Go-Getter": "#c1c6ff",   # Sky blue
    "The Intuitive": "#ffb67f"      # Orange
}

profile_font_color = {
    "The Analytical": "#333333",  # Very dark gray for yellow background
    "The Social": "#f0f0f0",      # Almost white for blue background
    "The Innovator": "#2f2f2f",   # Dark gray for green background
    "The Thorough": "#cfcfcf",     # Light gray for black background
    "The Go-Getter": "#212121",   # Darkest gray for sky blue background
    "The Intuitive": "#2a2a2a"      # Dark gray for orange background
}

weights = {  
    "The Analytical": {  
        "Logic/Analysis vs Feeling/Intuition": 0.4,  
        "Numbers": 0.3,  
        "Technical": 0.2,  
        "Perfectionist": 0.1,  
    },  
    "The Social": {  
        "Relator": 0.4,  
        "Social Influence": 0.3,  
        "Feeling": 0.2,  
        "Values": 0.1,  
    },  
    "The Innovator": {  
        "Ideation": 0.4,  
        "Risk Aversion vs Novelty": 0.3,  
        "Technical": 0.2,  
        "Associative vs Structured": 0.1,  
    },  
    "The Thorough": {  
        "Risk Aversion vs Novelty": 0.4,  
        "Perfectionist": 0.3,  
        "Security": 0.2,  
        "Values": 0.1,  
    },  
    "The Go-Getter": {  
        "Pace": 0.4,  
        "Achiever": 0.3,  
        "Command": 0.2,  
        "Responsive vs Disruptive": 0.1,  
    },  
    "The Emotional": {  
        "Feeling": 0.4,  
        "Emotional": 0.3,  
        "Relator": 0.2,  
        "Turbulent": 0.1,  
    },  
}  

def determine_profile_v2_category_v2(param_score):
    raw_scores = {}
    person_scores = profile_param_wights.normalize_param_scores(param_score)
    probabilities = profile_param_wights.profile_param_weights.predict_for_new_person(person_scores)
    best_profile = max(probabilities, key=probabilities.get)

    profile_category_dict = {
        "category": best_profile,
        "scores": raw_scores,
        "description": profiles_description.get(best_profile, ""),
        "extended_description": profiles_extended_description.get(best_profile, ""),
        "explanation": profiles_explanation.get(best_profile, {}),
        "color": profiles_colors.get(best_profile, ""),
        "font_color": profile_font_color.get(best_profile, ""),
    }
    return ProfileCategory.from_dict(profile_category_dict)

def determine_profile_v2_category(param_score):
    raw_scores = {} 
    # Calculate raw scores for each profile  
    for profile, params in weights.items():  
        raw_score = 0
        for param, weight in params.items():
            if param in param_score:
                raw_score += param_score[param] * weight
            else:
                logger.error(f"Parameter {param} not found in user param scores")
        raw_scores[profile] = raw_score  
    
    # Normalize the raw scores to calculate probabilities  
    total_raw_score = sum(raw_scores.values())  
    probabilities = {  
        profile: raw_score / total_raw_score for profile, raw_score in raw_scores.items()  
    }  
    
    best_profile = max(probabilities, key=probabilities.get) 
    
    print("\nProbabilities:")  
    for profile, probability in probabilities.items():  
        print(f"{profile}: {probability * 100:.2f}%")

    profile_category_dict = {
        "category": best_profile,
        "scores": raw_scores,
        "description": profiles_description.get(best_profile, ""),
        "extended_description": profiles_extended_description.get(best_profile, ""),
        "explanation": profiles_explanation.get(best_profile, {}),
        "color": profiles_colors.get(best_profile, ""),
        "font_color": profile_font_color.get(best_profile, ""),
    }
    return ProfileCategory.from_dict(profile_category_dict)
    

# Function to calculate the best profile
def determine_profile_category(strengths_scores):
    total_strength_score = 0
    for strength in strengths_scores:
        if isinstance(strength, dict):
            total_strength_score += strength.get("score", 0)
        else:
            total_strength_score += strength.score
        
    normalized_strengths = {}
    for strength in strengths_scores:
        score = strength.get("score", 0) if isinstance(strength, dict) else strength.score
        name = strength.get("strength_name", None) if isinstance(strength, dict) else strength.strength_name
        if name:
            normalized_strengths[name] = score / total_strength_score

    # Initialize profile scores
    profile_scores = {profile: 0 for profile in profiles}

    for strength, normalized_weight in normalized_strengths.items():
        if strength in strengths_mapping:
            for i, profile in enumerate(profiles):
                score = strengths_mapping[strength][i]
                if score != 10:  # Ignore irrelevant scores
                    profile_scores[profile] += normalized_weight * score

    # Select the profile with the lowest total score
    best_profile = min(profile_scores, key=profile_scores.get)
    profile_category_dict = {
        "category": best_profile,
        "scores": profile_scores,
        "description": profiles_description.get(best_profile, ""),
        "extended_description": profiles_extended_description.get(best_profile, ""),
        "explanation": profiles_explanation.get(best_profile, {}),
        "color": profiles_colors.get(best_profile, ""),
        "font_color": profile_font_color.get(best_profile, ""),
    }

    return ProfileCategory.from_dict(profile_category_dict)

def get_default_individual_sales_criteria(profile_category: ProfileCategory) -> list[SalesCriteria]:
    return sales_criteria_mapping[profile_category.category]


def fix_linkedin_url(linkedin_url: str) -> str:
    """
    Converts a full LinkedIn URL to a shortened URL.

    Args:
        linkedin_url (str): The full LinkedIn URL.

    Returns:
        str: The shortened URL.
    """

    if not linkedin_url:
        logger.error(f"Trying to fix Linkedin URL, but it is None or empty: {linkedin_url}")
        return ""

    linkedin_url = linkedin_url.replace("http://www.linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("https://www.linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("http://linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("https://linkedin.com/in/", "linkedin.com/in/")

    if linkedin_url and linkedin_url[-1] == "/":
        linkedin_url = linkedin_url[:-1:]
    return linkedin_url


def get_company_name_from_domain(email: str):
    company_domain = email.split("@")[1] if isinstance(email, str) and "@" in email else None
    if company_domain:
        company = companies_repository.get_company_from_domain(company_domain)
        if company:
            return company.name
    return None


def create_person_from_pdl_personal_data(person: PersonDTO):
    row_dict = personal_data_repository.get_personal_data_row(person.uuid)
    if not row_dict or row_dict.get("pdl_status") == personal_data_repository.TRIED_BUT_FAILED:
        logger.info(f"Personal data not found for {person.uuid}")
        return None
    personal_data = row_dict.get("pdl_personal_data")
    if not personal_data:
        logger.error(f"Personal data not found for {person.uuid}")
        return None
    personal_experience = personal_data.get("experience")
    position = ""
    company = get_company_name_from_domain(person.email)

    linkedin_url = row_dict.get("linkedin_url")
    if not linkedin_url:
        linkedin_url = personal_data.get("linkedin_url")
        if not linkedin_url:
            profiles = personal_data.get("profiles")
            for profile in profiles:
                if profile.get("network") == "linkedin":
                    linkedin_url = profile.get("url")
                    break
    linkedin_url = fix_linkedin_url(linkedin_url)
    pdl_company, pdl_position = get_company_and_position_from_pdl_experience(personal_experience)
    position = pdl_position
    if not company:
        company = pdl_company

    person_name = row_dict.get("name", "") or personal_data.get("full_name")
    logger.info(
        f"Position: {position}, Company: {company}, Person Name: {person_name}, Person Email: {person.email}"
    )

    person = PersonDTO(
        uuid=person.uuid,
        name=person.name or person_name,
        company=person.company or company,
        email=person.email,
        linkedin=person.linkedin or linkedin_url,
        position=person.position or position,
        timezone="",
    )
    logger.info(f"Person: {person}")
    return person


def get_company_and_position_from_apollo_personal_data(personal_data):
    position = personal_data.get("title", "")
    company = personal_data.get("organization", "")
    if company:
        company = company.get("name")
    experience_list = personal_data.get("employment_history")
    if experience_list and isinstance(experience_list, list):
        last_workplace = experience_list[0]
        if not position:
            position = last_workplace.get("title")
        if not company:
            company = last_workplace.get("organization_name")
    return company, position


def create_person_from_apollo_personal_data(person: PersonDTO):
    row_dict = personal_data_repository.get_personal_data_row(person.uuid)
    if not row_dict or row_dict.get("apollo_status") == personal_data_repository.TRIED_BUT_FAILED:
        return None
    personal_data = row_dict.get("apollo_personal_data")
    if not personal_data:
        logger.error(f"Personal data not found for {person.uuid}")
        return None
    apollo_company, position = get_company_and_position_from_apollo_personal_data(personal_data)
    company = get_company_name_from_domain(person.email)
    if not company:
        company = apollo_company
    linkedin_url = row_dict.get("linkedin_url")
    if not linkedin_url:
        logger.error(f"LinkedIn URL not found for {person.uuid}")
        linkedin_url = personal_data.get("linkedin_url")
        if not linkedin_url:
            logger.error(f"No LinkedIn URL found in apollo personal data for {person.uuid}")
        else:
            linkedin_url = fix_linkedin_url(linkedin_url)

    else:
        linkedin_url = fix_linkedin_url(linkedin_url)
    person_name = personal_data.get("name", "") or personal_data.get("first_name") + " " + personal_data.get(
        "last_name"
    )
    logger.info(
        f"Position: {position}, Company: {company}, Person Name: {person_name}, Person Email: {person.email}"
    )

    person = PersonDTO(
        uuid=person.uuid,
        name=person.name if (person.name and person.name != " ") else person_name,
        company=person.company if (person.company and person.company != " ") else company,
        email=person.email,
        linkedin=person.linkedin if (person.linkedin and person.linkedin != " ") else linkedin_url,
        position=person.position if (person.position and person.position != " ") else position,
        timezone="",
    )
    logger.info(f"Person: {person}")
    return person


def get_company_and_position_from_pdl_experience(personal_experience: dict) -> (str, str):
    if personal_experience and isinstance(personal_experience, list):
        personal_experience = personal_experience[0]
    company = ""
    position = ""

    if personal_experience and isinstance(personal_experience, dict):
        title_object = personal_experience.get("title")
        if title_object and isinstance(title_object, dict):
            position = title_object.get("name")
            company_object = personal_experience.get("company")
            if company_object and isinstance(company_object, dict):
                company = company_object.get("name")
    return company, position
