import streamlit as st

from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.data_transfer_objects.company_dto import NewsData
from data.data_common.dependencies.dependencies import (
    persons_repository,
    hobbies_repository,
    personal_data_repository,
    profiles_repository,
    interactions_repository,
    ownerships_repository,
    companies_repository,
)
from common.genie_logger import GenieLogger
logger = GenieLogger()

# Password protection
def check_password():
    password = st.text_input("Enter Password", type="password")
    if password != "123":
        st.error("Invalid password")
        st.stop()


check_password()

persons_repository = persons_repository()
profiles_repository = profiles_repository()
companies_repository = companies_repository()
hobbies_repository = hobbies_repository()

# Search for a person by email
st.title("Genie Profile Enrichment")
email = st.text_input("Search by Email")

if email:
    person_object = persons_repository.get_person_by_email(email)
    person = persons_repository.get_person_complete_data(email)

    if person:
        name = person["name"]
        st.write(f"Prospect found: {name}")
        company = person["company"]
        email = person["email"]
        linkedin = person["linkedin"]
        position = person["position"]
        existing_hobbies = profiles_repository.get_hobbies_by_email(email)
        logger.info(f"Hobbies: {existing_hobbies}")

        existing_news = companies_repository.get_news_data_by_email(email)
        logger.info(f"News: {existing_news}")

        existing_connections = profiles_repository.get_connections_by_email(email)
        logger.info(f"Connections: {existing_connections}")

        # Initialize dynamic fields lists
        if not hasattr(st, "hobby_inputs"):
            st.hobby_inputs = existing_hobbies if existing_hobbies else []
        if not hasattr(st, "news_inputs"):
            st.news_inputs = existing_news if existing_news else []
        if not hasattr(st, "connection_inputs"):
            st.connection_inputs = existing_connections if existing_connections else []

        with st.form("enrichment_form"):
            linkedin_url = st.text_input("LinkedIn URL", value=linkedin or "")
            if linkedin_url and linkedin_url != linkedin:
                logger.info(
                    f"Updating linkedin URL from [{linkedin}] to [{linkedin_url}]"
                )
                persons_repository.update_person_linkedin(email, linkedin_url)
            position = st.text_input("Position", value=position or "")

            st.subheader("Hobbies")
            hobby_inputs = []
            if st.hobby_inputs:
                logger.info(st.hobby_inputs)
                for i, hobby in enumerate(st.hobby_inputs):
                    hobby_name = st.text_input(
                        f"Hobby {i+1} Name", value=hobby.hobby_name
                    )
                    hobby_icon = st.text_input(
                        f"Hobby {i+1} Icon URL", value=hobby.icon_url
                    )
                    hobby_inputs.append((hobby_name, hobby_icon))

            st.subheader("Top News")
            news_inputs = []
            if st.news_inputs:
                logger.info(st.news_inputs)
                for i, news in enumerate(st.news_inputs):
                    logger.info(f"processing news {news} ")
                    news_headline = st.text_input(
                        f"News {i+1} Headline", value=news.title
                    )
                    news_url = st.text_input(f"News {i+1} URL", value=news.link)
                    news_source = st.text_input(f"News {i+1} Source", value=news.media)
                    news_inputs.append((news_headline, news_url, news_source))

            st.subheader("Relevant Connections")
            connection_inputs = []
            if st.connection_inputs:
                for i, connection in enumerate(st.connection_inputs):
                    connection_name = st.text_input(
                        f"Connection {i+1} Name", value=connection.name
                    )
                    connection_picture = st.text_input(
                        f"Connection {i+1} Image URL", value=connection.image_url
                    )
                    connection_linkedin = st.text_input(
                        f"Connection {i+1} LinkedIn URL", value=connection.linkedin_url
                    )
                    connection_inputs.append(
                        (connection_name, connection_picture, connection_linkedin)
                    )

            if st.form_submit_button("Save"):
                hobbies_list = [
                    {"hobby": h[0], "icon_url": h[1]}
                    for h in hobby_inputs
                    if h[0] and h[1]
                ]
                if existing_hobbies != hobbies_list:
                    logger.info(f"New hobbies: {hobbies_list}")
                    hobby_uuids = []
                    for hobby in hobbies_list:
                        hobby_uuid = hobbies_repository.find_or_create_hobby(
                            hobby["hobby"], hobby["icon_url"]
                        )
                        hobby_from_db = hobbies_repository.get_hobby(hobby_uuid)
                        if not hobby_from_db["icon_url"] == hobby["icon_url"]:
                            hobbies_repository.update_icon_url(
                                hobby_uuid, hobby["icon_url"]
                            )
                        hobby_uuids.append(hobby_uuid)
                    profiles_repository.update_hobbies_by_email(email, hobby_uuids)
                top_news_list = [
                    {"title": n[0], "link": n[1], "media": n[2]}
                    for n in news_inputs
                    if n[0] and n[1] and n[2]
                ]
                if existing_news != top_news_list:
                    logger.info(f"New news: {top_news_list}")
                    filtered_list = []
                    for news_articale in top_news_list:
                        if news.title and news.title != "My Post":
                            filtered_list.append(news_articale)
                    companies_repository.save_news_by_email(email, filtered_list)
                else:
                    logger.info("News list is the same")
                relevant_connections_list = [
                    {"name": c[0], "image_url": c[1], "linkedin_url": c[2]}
                    for c in connection_inputs
                    if c[0] and c[1] and c[2]
                ]
                if existing_connections != relevant_connections_list:
                    logger.info(f"New connections: {relevant_connections_list}")
                    profiles_repository.update_connections_by_email(
                        email, relevant_connections_list
                    )

                st.success("Prospect data updated successfully!")

        if st.button("Add Hobby"):
            st.hobby_inputs.append(({"hobby_name": "", "icon_url": ""}))
            st.rerun()

        if st.button("Add News"):
            st.news_inputs.append(
                NewsData(
                    link="https://linkedin.com/my-post",
                    title="My Post",
                    media="linkedin",
                )
            )
            st.rerun()

        if st.button("Add Connection"):
            st.connection_inputs.append(
                ({"name": "", "image_url": "", "linkedin_url": ""})
            )
            st.rerun()

    else:
        st.error("No prospect found with this email")
