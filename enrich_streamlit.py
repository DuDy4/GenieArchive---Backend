import streamlit as st
from loguru import logger
from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
    interactions_repository,
    ownerships_repository,
)

# Password protection
def check_password():
    password = st.text_input("Enter Password", type="password")
    if password != "123":
        st.error("Invalid password")
        st.stop()

check_password()

persons_repository = persons_repository()
profiles_repository = profiles_repository()

# Search for a person by email
st.title("Prospect Data Enrichment")
email = st.text_input("Search by Email")

if email:
    person = persons_repository.get_person_complete_data(email)

    if person:
        name = person['name']
        st.write(f"Prospect found: {name}")
        company = person['company']
        email = person['email']
        linkedin = person['linkedin']
        position = person['position']
        hobbies = profiles_repository.get_hobbies_by_email(email)
        logger.info(f"Hobbies: {hobbies}")

        top_news = profiles_repository.get_news_by_email(email)
        logger.info(f"News: {top_news}")

        connections = profiles_repository.get_connections_by_email(email)
        logger.info(f"Connections: {connections}")

        # Initialize dynamic fields lists
        if not hasattr(st, 'hobby_inputs'):
            st.hobby_inputs = hobbies if hobbies else [({"hobby_name" : "", "icon_url" : ""})]
        if not hasattr(st, 'news_inputs'):
            st.news_inputs = top_news if top_news else [({"title" : "", "link" : "", "source" : "linkedin"})]
        if not hasattr(st, 'connection_inputs'):
            st.connection_inputs = connections if connections else [({"name" : "", "image_url" : "", "linkedin_url" : ""})]

        with st.form("enrichment_form"):
            linkedin_url = st.text_input("LinkedIn URL", value=linkedin or "")
            position = st.text_input("Position", value=position or "")

            st.subheader("Hobbies")
            hobby_inputs = []
            if st.hobby_inputs:
                logger.info(st.hobby_inputs)
                for i, hobby in enumerate(st.hobby_inputs):
                    hobby_name = st.text_input(f"Hobby {i+1} Name", value=hobby['hobby_name'])
                    hobby_icon = st.text_input(f"Hobby {i+1} Icon URL", value=hobby['icon_url'])
                    hobby_inputs.append((hobby_name, hobby_icon))

            st.subheader("Top News")
            news_inputs = []
            if st.news_inputs:
                logger.info(st.news_inputs)
                for i, news in enumerate(st.news_inputs):
                    logger.info(f"processing news {news} ")
                    news_headline = st.text_input(f"News {i+1} Headline", value=news['title'])
                    news_url = st.text_input(f"News {i+1} URL", value=news['link'])
                    news_source = st.text_input(f"News {i+1} Source", value=news['source'])
                    news_inputs.append((news_headline, news_url, news_source))

            st.subheader("Relevant Connections")
            connection_inputs = []
            if st.connection_inputs:
                for i, connection in enumerate(st.connection_inputs):
                    connection_name = st.text_input(f"Connection {i+1} Name", value=connection['name'])
                    connection_picture = st.text_input(f"Connection {i+1} Image URL", value=connection['image_url'])
                    connection_linkedin = st.text_input(f"Connection {i+1} LinkedIn URL", value=connection['linkedin_url'])
                    connection_inputs.append((connection_name, connection_picture, connection_linkedin))

            if st.form_submit_button("Save"):
                hobbies_list = [ {"hobby": h[0], "icon_url": h[1]} for h in hobby_inputs if h[0] and h[1]]
                logger.info(f"Hobbies list to save: {hobbies_list}")
                top_news_list = [{"headline": n[0], "url": n[1], "source": n[2]} for n in news_inputs if n[0] and n[1] and n[2]]
                logger.info(f"News list to save: {top_news_list}")
                relevant_connections_list = [{"name": c[0], "picture_url": c[1], "linkedin_url": c[2]} for c in connection_inputs if c[0] and c[1] and c[2]]
                logger.info(f"Connections list to save: {relevant_connections_list}")

                # TODO - Implement the update query
                # update_query = prospects_table.update().where(prospects_table.c.email == email).values(
                #     linkedin_url=linkedin_url,
                #     position=position,
                #     twitter_url=twitter_url,
                #     hobbies=hobbies_list,
                #     top_news=top_news_list,
                #     relevant_connections=relevant_connections_list
                # )
                # session.execute(update_query)
                # session.commit()
                st.success("Prospect data updated successfully!")

        if st.button("Add Hobby"):
            st.hobby_inputs.append(({"hobby_name" : "", "icon_url" : ""}))
            st.experimental_rerun()

        if st.button("Add News"):
            st.news_inputs.append(({"title" : "", "link" : "", "source" : "linkedin"}))
            st.experimental_rerun()

        if st.button("Add Connection"):
            st.connection_inputs.append(({"name" : "", "image_url" : "", "linkedin_url" : ""}))
            st.experimental_rerun()

    else:
        st.error("No prospect found with this email")
