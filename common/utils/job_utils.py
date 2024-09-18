from common.genie_logger import GenieLogger
from data.data_common.utils.str_utils import to_custom_title_case
import traceback

logger = GenieLogger()


def fix_and_sort_experience_from_pdl(experience):
    try:
        for exp in experience:
            exp["end_date"] = exp.get("end_date") or "9999-12-31"  # Treat ongoing as future date
            exp["start_date"] = exp.get("start_date") or "0000-01-01"

            # Sort experience
        sorted_experience = sorted(experience, key=lambda x: (x["end_date"], x["start_date"]), reverse=True)
    except:
        logger.error(f"Error fixing and sorting experience: {experience}")
        sorted_experience = experience
    for exp in sorted_experience:
        try:
            if exp.get("end_date") == "9999-12-31":
                exp["end_date"] = None
            if exp.get("start_date") == "0000-01-01":
                exp["start_date"] = None
            title = exp.get("title")
            if title and isinstance(title, dict):
                name = title.get("name")
                titleize_name = to_custom_title_case(name)
                exp["title"]["name"] = titleize_name
            company = exp.get("company")
            if company and isinstance(company, dict):
                name = company.get("name")
                titleize_name = to_custom_title_case(name)
                exp["company"]["name"] = titleize_name
        except Exception as e:
            logger.error(f"Error: {e}")
            traceback.print_exc()
            continue
    return to_custom_title_case(sorted_experience)


def fix_and_sort_experience_from_apollo(apollo_data):
    experience = apollo_data.get("employment_history")
    if not experience:
        return None
    try:
        for exp in experience:
            exp["end_date"] = exp.get("end_date") or "9999-12-31"  # Treat ongoing as future date
            exp["start_date"] = exp.get("start_date") or "0000-01-01"

            # Sort experience
        sorted_experience = sorted(experience, key=lambda x: (x["end_date"], x["start_date"]), reverse=True)
    except:
        logger.error(f"Error fixing and sorting experience: {experience}")
        sorted_experience = experience
    for exp in sorted_experience:
        try:
            if exp.get("end_date") == "9999-12-31":
                exp["end_date"] = None
            if exp.get("start_date") == "0000-01-01":
                exp["start_date"] = None
            title = exp.get("title")
            if title and isinstance(title, dict):
                name = title.get("name")
            elif title:
                name = title
            titleize_name = to_custom_title_case(name)
            exp["title"] = {"name": titleize_name}
            # exp["title"]["name"] = titleize_name
            company = exp.get("organization_name")
            if company and isinstance(company, dict):
                name = company.get("name")
            elif company:
                name = company
            titleize_name = to_custom_title_case(name)
            exp["company"] = {"name": titleize_name}
            # exp["company"]["name"] = titleize_name
        except Exception as e:
            logger.error(f"Error: {e}")
            traceback.print_exc()
            continue
    return to_custom_title_case(sorted_experience)
