import os
import gspread
import matplotlib.pyplot as plt
import numpy as np
import json
import re
import requests
from openai import OpenAI
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import boto3
from botocore.exceptions import NoCredentialsError
from requests_oauthlib import OAuth1

from dotenv import load_dotenv

load_dotenv()

S3_REGION_NAME = os.environ.get("S3_REGION_NAME", "us-east-1")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")
NOUN_KEY = os.environ.get("NOUN_KEY")
NOUN_SECRET = os.environ.get("NOUN_SECRET")
noun_auth = OAuth1(NOUN_KEY, NOUN_SECRET)
noun_endpoint = "https://api.thenounproject.com/v2/icon?limit=1&query="

# AWS S3 configuration
s3_bucket_name = "genie-poc-images"

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MAIL_COLUMN = "Draft (Auto-Generated)"
STRENGTHS_COLUMN = "Stregnths (Auto-Generated)"
CHART_COLUMN = "Strengths Chart (Auto-Generated)"
IMAGE_LINK_COLUMN = "Image link"
HOBBY_URLS_COLUMN = "Hobby URLs"
NEWS_COLUMN = "News Data"
OpenAI.api_key = os.environ.get("OPENAI_API_KEY")

client = OpenAI()


def sent_to_gpt(chat_log, temperature):
    """
    Sends a chat log to GPT for generating a response based on the provided temperature setting.
    """
    # Ensure chat_log is in the correct format
    if not isinstance(chat_log, list) or not all(
        isinstance(item, dict) for item in chat_log
    ):
        raise ValueError("chat_log must be a list of dictionaries.")
    if any(
        not isinstance(item.get("role", ""), str)
        or not isinstance(item.get("content", ""), str)
        for item in chat_log
    ):
        raise ValueError(
            "Each item in chat_log must have 'role' and 'content' keys with string values."
        )

    try:
        response = client.chat.completions.create(
            model="gpt-4o", temperature=temperature, messages=chat_log
        )
        assistant_response = response.choices[0].message.content
        return assistant_response.replace(". ", ".\n")
    except Exception as e:
        print(f"An error occurred while sending to GPT: {e}")
        return ""


creds = Credentials.from_service_account_file(
    "./google_creds_vc_mailing.json", scopes=scope
)
client = gspread.authorize(creds)
spreadsheet = client.open("Funding")
sheet = spreadsheet.worksheet("VCs seed")


def get_sheet_records(id: int):
    all_members = sheet.get_all_records()
    vc_member = all_members[id - 2]
    return vc_member


def get_vc_member_data(name: str):
    name = name.replace("_", " ").strip()
    all_members = sheet.get_all_records()
    for index, row in enumerate(all_members):
        if row["Full name"].strip() == name:
            return row, index + 2
    return None


# Function to normalize names to URL format
def normalize_name(name):
    return name.lower().replace(" ", "-").strip()


# Function to generate URL from name
def generate_url(name):
    base_url = "https://signal.nfx.com/investors/"
    return base_url + normalize_name(name)


# Function to fetch image source URL
def fetch_image_src(name):
    url = generate_url(name)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 403:
        print(f"Access forbidden for URL: {url}")
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    # Find the image tag (you might need to adjust the tag and class based on the HTML structure)
    # image_tag = soup.find('img', class_='contact-card-img')  # Adjust the class name if necessary
    image_tag = soup.find(
        "img", class_="carousel-container-inner"
    )  # Adjust the class name if necessary
    if image_tag:
        return image_tag["src"]
    return None


def fetch_hobbies_images(hobbies):
    hobby_data = []
    for hobby in hobbies:
        icon_response = requests.get(noun_endpoint + hobby, auth=noun_auth)
        icon_body = icon_response.json()
        if len(icon_body["icons"]) > 0 and icon_body["icons"][0]["thumbnail_url"]:
            icon_url = icon_body["icons"][0]["thumbnail_url"]
            hobby_data.append({hobby: icon_url})
            # upload_to_s3(icon_url, f'{hobby}.png')
    return hobby_data


def get_strengths(vc_member_data):
    prompt = prompts.strengths_ideitifier(vc_member_data)
    chat_logs = [{"role": "user", "content": f"{prompt}"}]
    response = sent_to_gpt(chat_logs, 0.3)
    return response


def get_hobbies(vc_member_data):
    prompt = prompts.get_hobbies(vc_member_data)
    chat_logs = [{"role": "user", "content": f"{prompt}"}]
    response = sent_to_gpt(chat_logs, 0.3)
    return response


def get_news(vc_member_data):
    prompt = prompts.get_news(vc_member_data)
    chat_logs = [{"role": "user", "content": f"{prompt}"}]
    response = sent_to_gpt(chat_logs, 0.3)
    return response


def create_vc_draft_email(vc_member_data, strengths):
    prompt = prompts.vc_mail_generator(vc_member_data, strengths)
    chat_logs = [{"role": "user", "content": f"{prompt}"}]
    response = sent_to_gpt(chat_logs, 0.3)
    return response


def update_sheet(id, mail_draft, strengths, chart_url, hobby_data, news, image_url):
    sheet.update_cell(id, sheet.find(MAIL_COLUMN).col, mail_draft)
    sheet.update_cell(id, sheet.find(STRENGTHS_COLUMN).col, strengths)
    sheet.update_cell(id, sheet.find(CHART_COLUMN).col, f'=IMAGE("{chart_url}")')
    sheet.update_cell(id, sheet.find(IMAGE_LINK_COLUMN).col, image_url)
    if hobby_data:
        sheet.update_cell(id, sheet.find(HOBBY_URLS_COLUMN).col, str(hobby_data))
    else:
        sheet.update_cell(id, sheet.find(HOBBY_URLS_COLUMN).col, "[]")
    if news:
        sheet.update_cell(id, sheet.find(NEWS_COLUMN).col, str(news))
    else:
        sheet.update_cell(id, sheet.find(NEWS_COLUMN).col, "[]")


def get_strengths_chart(strengths, name_and_vc):
    spider_graph_name = create_spider_graph(strengths, name_and_vc)
    return spider_graph_name


def create_spider_graph(strengths_score, name_and_vc):
    traits = strengths_score
    labels = [item["strength"] for item in traits]
    values = [item["score"] for item in traits]

    # Number of variables we're plotting.
    num_vars = len(labels)

    # Compute angle each bar is centered on:
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()

    # Repeat the first value to close the circle
    values += values[:1]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))
    ax.fill(angles, values, color="red", alpha=0.25)
    ax.plot(angles, values, color="red", linewidth=2)  # Plot the data

    # Labels for each point
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels)

    # Title of the graph
    ax.set_title(f"{name_and_vc} Personality Chart", size=15, color="Black", y=1.1)
    graph_name = f"strengths_{name_and_vc}.png"
    plt.savefig(graph_name)
    # plt.show()
    return graph_name


def extract_first_json(string):
    # Regular expression to find potential JSON objects
    json_pattern = re.compile(r"\{.*\}", re.S)

    # Find all potential JSON objects
    matches = json_pattern.findall(string)

    for match in matches:
        try:
            # Try to parse the JSON
            json_obj = json.loads(match)
            return json_obj
        except json.JSONDecodeError:
            # If parsing fails, continue to the next match
            continue

    json_pattern = re.compile(r"\[.*\]", re.S)
    matches = json_pattern.findall(string)

    for match in matches:
        try:
            # Try to parse the JSON
            json_obj = json.loads(match)
            return json_obj
        except json.JSONDecodeError:
            # If parsing fails, continue to the next match
            continue

    # Return None if no valid JSON object is found
    return None


def upload_to_s3(file_name, object_name=None):
    if object_name is None:
        object_name = file_name

    # Initialize a session using Amazon S3
    s3_client = boto3.client(
        "s3",
        region_name=S3_REGION_NAME,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )

    try:
        # Upload the file to S3
        s3_client.upload_file(
            file_name, s3_bucket_name, object_name, ExtraArgs={"ACL": "public-read"}
        )
        # Construct the public URL
        url = (
            f"https://{s3_bucket_name}.s3.{S3_REGION_NAME}.amazonaws.com/{object_name}"
        )
        return url
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None
