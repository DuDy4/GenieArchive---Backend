import streamlit as st
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
import json
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
    if password != "g3n13":
        st.error("Invalid password")
        st.stop()

check_password()

persons_repository = persons_repository() 


# Search for a person by email
st.title("Prospect Data Enrichment")
email = st.text_input("Search by Email")

if email:
    person = persons_repository.get_person_data(email)

    if person:
        st.write(f"Prospect found: {person.name}")
        name = person.name
        company = person.company
        email = person.email
        linkedin = person.linkedin
        position = person.position
        
        linkedin_url = st.text_input("LinkedIn URL", value=linkedin or "")
        position = st.text_input("Position", value=position or "")
        #twitter_url = st.text_input("Twitter URL", value=result.twitter_url or "")

        hobbies = st.text_area("Hobbies (Format: hobby,icon_url per line)", value="\n".join([f"{hobby['hobby']},{hobby['icon_url']}" for hobby in result.hobbies] if result.hobbies else []))
        top_news = st.text_area("Top News (Format: headline,url,source per line)", value="\n".join([f"{news['headline']},{news['url']},{news['source']}" for news in result.top_news] if result.top_news else []))
        relevant_connections = st.text_area("Relevant Connections (Format: name,picture_url,linkedin_url per line)", value="\n".join([f"{conn['name']},{conn['picture_url']},{conn['linkedin_url']}" for conn in result.relevant_connections] if result.relevant_connections else []))

        if st.button("Save"):
            hobbies_list = [dict(zip(["hobby", "icon_url"], h.split(','))) for h in hobbies.split('\n')]
            top_news_list = [dict(zip(["headline", "url", "source"], n.split(','))) for n in top_news.split('\n')]
            relevant_connections_list = [dict(zip(["name", "picture_url", "linkedin_url"], c.split(','))) for c in relevant_connections.split('\n')]
            
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
    else:
        st.error("No prospect found with this email")
