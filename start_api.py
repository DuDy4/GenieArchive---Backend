import os
import traceback
import signal
import sys
import uvicorn
import requests
import jwt
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from dotenv import load_dotenv

# Import OpenTelemetry for logging
from opentelemetry import trace
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs import LoggingHandler
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from common.genie_logger import GenieLogger

# Load environment variables and initialize logger
load_dotenv()
logger = GenieLogger()
# Set up Azure Monitor logging exporter
exporter = AzureMonitorLogExporter()

# Set up Logger Provider
logger_provider = LoggerProvider(resource=Resource.create({"service.name": "genie-api"}))
logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))

# Set up logging handler (if you want to use it with Python logging)

handler = LoggingHandler(logger_provider=logger_provider)

from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from common.utils import env_utils

# from jose import JWTError, jwt
from data.api.api_manager import v1_router

GENIE_CONTEXT_HEADER = "genie-context"
ALLOWED_ROUTES = ["/users/login-event"]
AUTH0_DOMAIN = env_utils.get("AUTH0_DOMAIN", "https://dev-ef3pwnhntlcnkc81.us.auth0.com")
API_IDENTIFIER = (
    (AUTH0_DOMAIN + "/api/v2/") if AUTH0_DOMAIN else "https://dev-ef3pwnhntlcnkc81.us.auth0.com/api/v2/"
)
ALGORITHMS = ["RS256"]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def get_auth0_public_key():
    jwks_url = f"{AUTH0_DOMAIN}/.well-known/jwks.json"
    jwks_response = requests.get(jwks_url)
    jwks = jwks_response.json()
    return jwks


def decode_auth0_token(token):
    jwks = get_auth0_public_key()

    # Get the header to determine which key to use
    unverified_header = jwt.get_unverified_header(token)

    rsa_key = {}
    for key in jwks["keys"]:
        if key["kid"] == unverified_header["kid"]:
            rsa_key = {"kty": key["kty"], "kid": key["kid"], "use": key["use"], "n": key["n"], "e": key["e"]}

    if rsa_key:
        try:
            payload = jwt.decode(
                token, rsa_key, algorithms=ALGORITHMS, audience=API_IDENTIFIER, issuer=f"{AUTH0_DOMAIN}"
            )
            return payload
        except jwt.ExpiredSignatureError:
            return "Token has expired"
        except jwt.JWTClaimsError:
            return "Incorrect claims, please check the audience and issuer"
        except Exception as e:
            return f"Unable to parse token: {str(e)}"
    return "Unable to find appropriate key"


# Optional JWT validation function
async def jwt_optional(token: str = Depends(oauth2_scheme)):
    if not token:
        return None

    try:
        # payload = jwt.decode(token, env_utils.get("JWT_SECRET_KEY"), algorithms=["HS256"])
        payload = decode_auth0_token(token)
        logger.info(f"Optional API JWT payload: {payload}")
        return payload
    except Exception as e:
        logger.error(f"Optional API JWT error: {traceback.format_exc()}")
        return None


async def jwt_mandatory(token: str = Depends(oauth2_scheme)):
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        # payload = jwt.decode(token, env_utils.get("JWT_SECRET_KEY"), algorithms=["RS256"])
        payload = decode_auth0_token(token)
        logger.info(f"API JWT payload: {payload}")
        return payload
    except Exception as e:
        logger.error(f"API JWT error: {traceback.format_exc()}")
        raise HTTPException(status_code=401, detail="Invalid token")


class JWTValidationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path not in ALLOWED_ROUTES:
            token = request.headers.get("Authorization")
            await jwt_mandatory(token)

        response = await call_next(request)
        return response


class GenieContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        genie_context = None
        if request.headers and GENIE_CONTEXT_HEADER in request.headers:
            genie_context = request.headers[GENIE_CONTEXT_HEADER]
        if not genie_context:
            logger.bind_context()
            logger.info("No Genie context found.")
        else:
            logger.info(f"Found Genie context: {genie_context}")
        if request.url and request.url.path:
            logger.set_endpoint(request.url.path)
            logger.info(f"Request to {request.url.path}")
        response = await call_next(request)
        return response


# Initialize FastAPI app and middleware
app = FastAPI(
    title="Profile Management API",
    description="This is the official Genie AI API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://localhost:5173", "https://alpha.genieai.ai"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(SessionMiddleware, secret_key=env_utils.get("APP_SECRET_KEY"), max_age=3600)
app.add_middleware(GenieContextMiddleware)
# app.add_middleware(JWTValidationMiddleware)


@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception):
    logger.error(f"Request failed: {exc}")
    traceback_str = "".join(traceback.format_tb(exc.__traceback__))
    logger.error(f"Traceback: {traceback_str}")
    return PlainTextResponse(str(exc), status_code=500)


@app.get("/", response_class=RedirectResponse)
def read_root(request: Request):
    base_url = request.url.scheme + "://" + request.url.netloc
    return RedirectResponse(url=base_url + "/docs")


app.include_router(v1_router)

PORT = int(env_utils.get("PERSON_PORT", 8000))
use_https = env_utils.get("USE_HTTPS", "false").lower() == "true"

# Function to handle signal and flush logs
def handle_shutdown_signal(signal, frame):
    print("Shutdown signal received, flushing logs...")
    try:
        logger_provider.force_flush(timeout_millis=10000)
    except Exception as e:
        print(f"Error during log flush: {e}")
    finally:
        sys.exit(0)


# Register the shutdown signal handler for KeyboardInterrupt (Ctrl+C)
signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)  # Optional for other termination signals

logger.info(f"Starting API on port {PORT} with HTTPS: {use_https}")

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        ssl_keyfile="key.pem" if use_https else None,
        ssl_certfile="cert.pem" if use_https else None,
    )
