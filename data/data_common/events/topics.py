class Topic:
    # Events that notify about new objects in the system

    # New objects events
    NEW_MEETING = "new-meeting"
    NEW_COMPANY_DATA = "new-company-data"
    NEW_PERSON = "new-person"
    NEW_CONTACT = "new-contact"  # Also used for new person
    NEW_INTERACTION = "new-interaction"
    NEW_EMAIL_ADDRESS_TO_PROCESS = "new-email-address-to-process"  # Derived from new meetings

    # Events that require tasks

    # People enrichment
    NEW_EMAIL_ADDRESS_TO_ENRICH = "new-email-address-to-enrich"  # Call for people data labs
    NEW_CONTACT_TO_ENRICH = "new-contact-to-enrich"  # Call for people data labs
    UPDATED_ENRICHED_DATA = "got-updated-enriched-data"  # People data labs succeeded in enriching data
    NEW_PERSONAL_DATA = "new-personal-data-to-process"  # PersonManager calls for Langsmith to process enriched data
    NEW_PROCESSED_PROFILE = "new-processed-profile"  # Langsmith Succeeded in processing enriched data

    # Company enrichment
    NEW_EMAIL_TO_PROCESS_DOMAIN = "new-email-to-process-domain"  # Call for hunter.io

    # Events that stops the process

    # Events that notify that all is well (no action needed or end of successful process)
    UP_TO_DATE_ENRICHED_DATA = "up-to-date-enriched-data"  # People data labs already has the latest data
    COMPANY_NEWS_UP_TO_DATE = "company-news-up-to-date"

    ALREADY_FAILED_TO_ENRICH_EMAIL = "already-failed-to-enrich-email"  # People data labs already failed to enrich
    # email, and not enough time passed to retry
    COMPANY_NEWS_UPDATED = "company-news-updated"  # News scrapper succeeded in getting news
    FINISHED_NEW_PROFILE = "finished-new-profile"  # PersonManager succeeded working with people data labs + Langsmith

    # Events that notify about failures
    FAILED_TO_ENRICH_DATA = "failed-to-enrich-data"  # People data labs failed to enrich data
    FAILED_TO_ENRICH_EMAIL = "failed-to-enrich-email"  # People data labs failed to enrich email
    FAILED_TO_GET_DOMAIN_INFO = "failed-to-get-domain-info"  # Hunter.io failed to get domain info
    FAILED_TO_GET_LINKEDIN_URL = "failed-to-get-linkedin-url"  # NOT ACTIVE - Langsmith failed to get LinkedIn url
    FAILED_TO_GET_COMPANY_NEWS = "failed-to-get-company-news"  # News scrapper failed to get news
