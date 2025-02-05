from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO
from data.data_common.data_transfer_objects.news_data_dto import SocialMediaPost
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

post = {
    "date": "2024-11-08",
    "link": "https://www.linkedin.com/feed/update/urn:li:activity:7260493919619067904/",
    "text": "The cloud is… complicated.\n\nBut why? After all the strides we’ve made in Infrastructure-as-Code, why are cloud configurations still so hard to manage? This video breaks it down.\n\nFirefly helps you understand and automate your cloud configurations, enabling you to eliminate drift and codify unmanaged assets instantly. Watch to learn more about how Firefly solves cloud complexity.\n\nTo learn more about how Firefly can help you manage cloud chaos check out 👉https://lnkd.in/gsikqG5P",
    "likes": 128,
    "media": "LinkedIn",
    "title": "The cloud is… complicated.\n\nBut why? After all the strides we’ve made in Infrastructure-as-Code, why",
    "images": [],
    "summary": None,
    "reshared": "https://www.linkedin.com/company/fireflyai"
}

social_media_post = SocialMediaPost.from_dict(post)

artifact = ArtifactDTO.from_social_media_post(social_media_post, "994a340c-6710-4826-9752-4e88fdceadfa")

# artifact = ArtifactDTO(
#     uuid="test_uuid",
#     artifact_type="post",
#     source="linkedin",
#     profile_uuid="994a340c-6710-4826-9752-4e88fdceadfa",
#     artifact_url="http://test.com",
#     text="test text",
#     summary="test summary",
#     published_date="2021-01-01T00:00:00",
#     metadata={"test": "test"}
# )

event = GenieEvent(
    topic=Topic.NEW_PERSON_ARTIFACT,
    data={"artifact": artifact.to_dict(), "profile_uuid": "994a340c-6710-4826-9752-4e88fdceadfa", "user_id": "google-oauth2|117881894742800328091", "tenant_id": "org_N1U4UsHtTfESJPYB"}
)
event.send()

