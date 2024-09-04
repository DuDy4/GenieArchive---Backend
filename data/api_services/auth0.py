import requests
import json
import os
import random
import string
import re

# Replace these with your client ID, client secret, and Auth0 domain as secrets
from common.genie_logger import GenieLogger
from dotenv import load_dotenv
load_dotenv()

logger = GenieLogger()

auth0_domain = os.getenv("AUTH0_DOMAIN")
auth0_client_id = os.getenv("AUTH0_CLIENT_ID")
auth0_client_secret = os.getenv("AUTH0_CLIENT_SECRET")



def get_api_token():
    auth0_audience = auth0_domain + "/api/v2/"
    grant_type = "client_credentials"
    token_url = f"{auth0_domain}/oauth/token"
    data= {
        "grant_type": grant_type,
        "client_id": auth0_client_id,
        "client_secret": auth0_client_secret,
        "audience": auth0_audience
    }
    
    response = requests.post(
        token_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(data)
    )
    if response.status_code != 200:
        logger.error(f"Failed to get API token: {response.status_code} - {response.text}")
        return None
    json_response = response.json()
    access_token = json_response.get("access_token")
    logger.info(f"Auth0 API token: {access_token[:10]}")
    return access_token


# Function to fetch the Management API token
def get_management_api_token():
    try:
        token_response = requests.post(f"https://{auth0_domain}/oauth/token", json={
            "client_id": auth0_client_id,
            "client_secret": auth0_client_secret,
            "audience": f"https://{auth0_domain}/api/v2/",
            "grant_type": "client_credentials"
        })
        token_response.raise_for_status()
        return token_response.json()['access_token']
    except requests.RequestException as error:
        print(f"Error fetching Management API token: {str(error)}")
        raise Exception('Failed to fetch Management API token.')



def handle_auth0_user_signup(user_info):
    user_id = 'google-oauth2|117881894742800328091'
    base_org_id = 'org_IpehKwPW4fx2hXIl'
    user_email = 'test@test.com'
    # Fetch Management API token
    management_api_token = get_api_token()
    auth_headers = {
        'Authorization': f"Bearer {management_api_token}",
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    orgs = get_user_orgs(user_id, auth_headers)
    if len(orgs) == 1 and orgs[0]['id'] == base_org_id:
        # Create a readable organization name using the user's name or email
        base_org_name = user_email.split('@')[0]
        base_org_name = re.sub(r'[^a-z0-9-]', '', base_org_name.lower().replace(' ', '-'))
        
        # Add a unique suffix to ensure the organization name is unique
        unique_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        org_name = f"org-{base_org_name}-{unique_suffix}"
        org_display_name = f" {user_email} Organization"
        
        try:

            organization = create_auth0_org(org_name, org_display_name, auth_headers)
            # Adds user to the new organization
            add_user_as_member(user_id, organization['id'], auth_headers)
            # Removes user from the base organization
            remove_user_as_member(user_id, base_org_id, auth_headers)


            logger.info(f"Organization {organization['name']} created and user assigned successfully.")
        except requests.RequestException as error:
            logger.error(f"Full response error: {error.response.text if error.response else str(error)}")
            raise Exception('User signup FAILED due to organization creation error.')
    else:
        logger.info('User already has an assigned organization.')


def create_auth0_org(org_name, org_display_name, headers):
    # Create a new organization
    create_org_response = requests.post(
        f"https://{auth0_domain}/api/v2/organizations",
        json={
            "name": org_name,
            "display_name": org_display_name
        },
        headers=headers
    )
    create_org_response.raise_for_status()
    organization = create_org_response.json()
    return organization


def add_user_as_member(user_id, org_id, headers):
    # Adds user to organization
    add_member_response = requests.post(
        f"https://{auth0_domain}/api/v2/organizations/{org_id}/members",
        json={
            "members": [
                user_id
            ]
        },
        headers=headers
    )
    add_member_response.raise_for_status()


def remove_user_as_member(user_id, org_id, headers):
    # Removes user from organization
    remove_member_response = requests.delete(
        f"https://{auth0_domain}/api/v2/organizations/{org_id}/members",
        json={
            "members": [
                user_id
            ]
        },
        headers=headers
    )
    remove_member_response.raise_for_status()


def create_auth0_org(org_name, org_display_name, headers):
    # Create a new organization
    create_org_response = requests.post(
        f"https://{auth0_domain}/api/v2/organizations",
        json={
            "name": org_name,
            "display_name": org_display_name
        },
        headers=headers
    )
    create_org_response.raise_for_status()
    organization = create_org_response.json()
    return organization


def get_user_orgs(user_id, headers):
    # Create a new organization
    create_org_response = requests.post(
        f"{auth0_domain}/api/v2/users/{user_id}/organizations",
        headers=headers
    )
    create_org_response.raise_for_status()
    organization = create_org_response.json()
    return organization





def main():
    # Get the API token
    api_token = get_api_token()
    print(f"API Token: {api_token}")

    # Use the API token to get user info
    # user_info = get_user_info(api_token)
    # print("User Info:", user_info)

if __name__ == "__main__":
    main()

