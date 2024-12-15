class Topic:
    # Events that notify about new objects in the system

    PROFILE_CRITICAL_TOPICS = [
                
    ]

    # New objects events
    PERSONAL_NEWS_ARE_UP_TO_DATE = "personal-news-are-up-to-date"  # News scrapper already has the latest news
    BUG_IN_TENANT_ID = "bug-in-tenant-id"  # TenantManager found a bug in tenant_id
    NEW_TENANT_PROFILE = "new-tenant-profile"  # TenantManager saved a new tenant profile
    FAILED_TO_GET_PERSONAL_NEWS = "failed-to-get-personal-news"  # News scrapper failed to get news
    NEW_PERSONAL_NEWS = "new-personal-news"  # News scrapper saved news data
    EMAIL_SENDING_FAILED = "email-sending-failed"  # Email sending failed
    FAILED_TO_GET_PROFILE_PICTURE = "failed-to-get-profile-picture"  # PersonManager failed to get profile picture
    NEW_UPCOMING_MEETING = "new-upcoming-meeting"  # MeetingConsumer saved a new meeting
    NEW_EMBEDDED_DOCUMENT = "new-embedded-document"  # SalesMaterialConsumer embedded a document
    NEW_PERSON_CONTEXT = "new-person-context"
    NEW_MEETING_GOALS = (
        "new-meeting-goals"  # MeetingConsumer called for langsmith and saved goals in relevant meetings
    )
    NEW_MEETINGS_TO_PROCESS = (
        "new-meetings-to-process"  # api_manager calls to MeetingManager to process new meetings
    )
    NEW_MEETING = "new-meeting"
    NEW_COMPANY_DATA = "new-company-data"
    NEW_PERSON = "new-person"
    NEW_CONTACT = "new-contact"  # Also used for new person
    NEW_INTERACTION = "new-interaction"
    NEW_EMAIL_ADDRESS_TO_PROCESS = (
        "new-email-address-to-process"  # Derived from new meetings and listened to by PersonManager
    )

    # Events that require tasks

    # People enrichment
    PDL_NEW_EMAIL_ADDRESS_TO_ENRICH = "new-email-address-to-enrich"  # Call for people data labs
    PDL_NEW_PERSON_TO_ENRICH = "pdl-new-person-to-enrich"  # Call for people data labs to enrich a Person
    APOLLO_NEW_PERSON_TO_ENRICH = "apollo-new-person-to-enrich"  # Call for apollo to enrich a Person
    APOLLO_NEW_EMAIL_ADDRESS_TO_ENRICH = (
        "apollo-new-email-address-to-enrich"  # Call for apollo to enrich an email
    )
    PDL_UPDATED_ENRICHED_DATA = (
        "pdl-got-updated-enriched-data"  # People data labs succeeded in enriching data
    )
    APOLLO_UPDATED_ENRICHED_DATA = "apollo-updated-enriched-data"  # Apollo succeeded in enriching data

    UPDATED_AGENDA_FOR_MEETING = "updated-agenda-for-meeting"  # MeetingManager updated meeting agenda

    NEW_PERSONAL_DATA = (
        "new-personal-data-to-process"  # PersonManager calls for Langsmith to process enriched data
    )
    NEW_PROCESSED_PROFILE = "new-processed-profile"  # Langsmith Succeeded in processing enriched data
    NEW_BASE_PROFILE = "new-base-profile"  # Langsmith Succeeded in processing calculating strengths

    # Company enrichment
    NEW_EMAIL_TO_PROCESS_DOMAIN = "new-email-to-process-domain"  # Call for hunter.io

    # Events that stops the process

    # Events that notify that all is well (no action needed or end of successful process)
    PDL_UP_TO_DATE_ENRICHED_DATA = "up-to-date-enriched-data"  # People data labs already has the latest data
    APOLLO_UP_TO_DATE_ENRICHED_DATA = "apollo-up-to-date-personal-data"  # Apollo already has the latest data
    COMPANY_NEWS_UP_TO_DATE = "company-news-up-to-date"

    ALREADY_PDL_FAILED_TO_ENRICH_EMAIL = (
        "already-failed-to-enrich-email"  # People data labs already failed to enrich
    )
    ALREADY_PDL_FAILED_TO_ENRICH_PERSON = (
        "already-failed-to-enrich-person"  # People data labs already failed to enrich
    )
    # email, and not enough time passed to retry
    COMPANY_NEWS_UPDATED = "company-news-updated"  # News scrapper succeeded in getting news
    FINISHED_NEW_PROFILE = (
        "finished-new-profile"  # PersonManager succeeded working with people data labs + Langsmith
    )

    # Events that notify about failures
    PDL_FAILED_TO_ENRICH_PERSON = "pdl-failed-to-enrich-person"  # People data labs failed to enrich data
    PDL_FAILED_TO_ENRICH_EMAIL = "pdl-failed-to-enrich-email"  # People data labs failed to enrich email
    APOLLO_FAILED_TO_ENRICH_PERSON = (
        "apollo-failed-to-enrich-person"  # People data labs failed to enrich data
    )
    APOLLO_FAILED_TO_ENRICH_EMAIL = "apollo-failed-to-enrich-email"  # People data labs failed to enrich email
    FAILED_TO_ENRICH_EMAIL = (
        "failed-to-enrich-email"  # PersonManager notify (slack) that email enrichment failed
    )
    FAILED_TO_ENRICH_PERSON = (
        "failed-to-enrich-person"  # PersonManager notify (slack) that person enrichment failed
    )
    FAILED_TO_GET_COMPANY_DATA = "failed-to-get-company-data"  # CompanyConsumer failed to get company data
    FAILED_TO_GET_COMPANY_NEWS = "failed-to-get-company-news"  # News scrapper failed to get news

    FILE_UPLOADED = "file-uploaded"


