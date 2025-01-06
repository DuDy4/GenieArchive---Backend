import requests
import base64
import json
from joserfc import jwt, jwk
from joserfc.jwk import JWKRegistry
from common.utils import env_utils


AUTH0_DOMAIN = env_utils.get("AUTH0_DOMAIN")
API_IDENTIFIER = AUTH0_DOMAIN + "/api/v2/"  # https://dev-ef3pwnhntlcnkc81.us.auth0.com/api/v2/
ALGORITHMS = ["RS256"]
CLAIM_NAMESPACE = "https://claims.genieai.ai/"
EMAIL_CLAIM = CLAIM_NAMESPACE + "user_email"
TENANT_ID_CLAIM = CLAIM_NAMESPACE + "tenant_id"
USER_ID_CLAIM = "sub"



jwks_url = f"{AUTH0_DOMAIN}/.well-known/jwks.json"
jwks_response = requests.get(jwks_url)
jwks = jwks_response.json()


def get_jwk_by_kid(kid, jwks):
    for jwk_data in jwks["keys"]:
        if jwk_data["kid"] == kid:
            return jwk_data
    return None

def decode_base64url(data):
    """Helper to decode base64url-encoded strings with proper padding."""
    padding_needed = 4 - (len(data) % 4)
    if padding_needed != 4:
        data += "=" * padding_needed  # Add the required padding
    return base64.urlsafe_b64decode(data)

def get_unverified_header(jwt_token):
    """Decode JWT header without verification."""
    header_b64 = jwt_token.split('.')[0]
    header_json = decode_base64url(header_b64).decode('utf-8')
    return json.loads(header_json)

def remove_bearer_prefix(token: str):
    """Remove 'Bearer' prefix if it exists."""
    if token.startswith("Bearer "):
        return token[len("Bearer "):]
    return token

def decode_jwt_token(access_token: str):
    access_token = remove_bearer_prefix(access_token)
    header = get_unverified_header(access_token)
    kid = header.get("kid")

    jwk_data = get_jwk_by_kid(kid, jwks)
    key = JWKRegistry.import_key(jwk_data)

    try:
        decoded_token = jwt.decode(access_token, key=key)
        return decoded_token
    except Exception as e:
        print(f"Failed to decode token: {e}")
        return None

def get_claims(payload: dict):
    return payload.claims

def get_user_email(payload: dict):
    if payload:
        return get_claims(payload).get(EMAIL_CLAIM)
    return None

def get_tenant_id(payload: dict):
    if payload:
        return get_claims(payload).get(TENANT_ID_CLAIM)
    return None

def get_user_id(payload: dict):
    if payload:
        return get_claims(payload).get(USER_ID_CLAIM)
    return None
