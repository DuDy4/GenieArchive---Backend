import os
import traceback
import signal
import sys
import uvicorn
import requests
import base64
import json
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from fastapi.routing import APIRoute
from dotenv import load_dotenv

# Import OpenTelemetry for logging
from opentelemetry import trace
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs import LoggingHandler
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from azure.monitor.opentelemetry import configure_azure_monitor
from common.genie_logger import GenieLogger
from common.utils import jwt_utils
# Load environment variables and initialize logger
load_dotenv()
logger = GenieLogger()
# Set up Azure Monitor logging exporter

# exporter = AzureMonitorLogExporter()
configure_azure_monitor()
# # Set up Logger Provider
# logger_provider = LoggerProvider(resource=Resource.create({"service.name": "genie-api"}))
# logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))

# # Set up logging handler (if you want to use it with Python logging)

# handler = LoggingHandler(logger_provider=logger_provider)

from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from common.utils import env_utils
from data.api.api_manager import v1_router

GENIE_CONTEXT_HEADER = "genie-context"
GENIE_EMAIL_STATE = "user_email"
ALLOWED_ROUTES = ["/users/login-event"]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Optional JWT validation function
async def jwt_validation(token: str = Depends(oauth2_scheme), mandatory: bool = True):
    if not token:
        if mandatory:
            raise HTTPException(status_code=401, detail="Not authenticated")
        return None

    try:
        payload = jwt_utils.decode_jwt_token(token)
        logger.info(f"{'Mandatory' if mandatory else 'Optional'} API JWT payload: {payload}")
        return payload
    except Exception as e:
        logger.error(f"API JWT error: {traceback.format_exc()}")
        if mandatory:
            raise HTTPException(status_code=401, detail="Invalid token")
        logger.info("Optional JWT token not valid, but continuing")
        return None


class JWTValidationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path not in ALLOWED_ROUTES:
            token = request.headers.get("Authorization")
            payload = await jwt_validation(token, mandatory=False)
            if payload:
                request.state.user_email = jwt_utils.get_user_email(payload)
                request.state.tenant_id = jwt_utils.get_tenant_id(payload)
        
        response = await call_next(request)
        return response
    
async def genie_metrics(request: Request):
    if request.scope['endpoint']:
        function_name = request.scope['endpoint'].__name__
        logger.set_function(function_name)
    logger.info("START HANDLING API")
    return



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
        if request.state and hasattr(request.state, 'user_email'):
            logger.set_email(request.state.user_email)
        if request.state and hasattr(request.state, 'tenant_id'):
            logger.set_tenant_id(request.state.tenant_id)
        response = await call_next(request)
        logger.info(f"FINISH HANDLING API")
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
app.add_middleware(JWTValidationMiddleware)


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

app.include_router(v1_router, dependencies=[Depends(genie_metrics)])

PORT = int(env_utils.get("PERSON_PORT", 8000))
use_https = env_utils.get("USE_HTTPS", "false").lower() == "true"


def handle_shutdown_signal(signal, frame):
    print("Shutdown signal received, flushing logs...")
    try:
        # logger_provider.force_flush(timeout_millis=10000)
        logger.info("Flushed logs")
    except Exception as e:
        print(f"Error during log flush: {e}")
    finally:
        sys.exit(0)


# Register the shutdown signal handler for KeyboardInterrupt (Ctrl+C)
signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)  

logger.info(f"Starting API on port {PORT} with HTTPS: {use_https}")

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        ssl_keyfile="key.pem" if use_https else None,
        ssl_certfile="cert.pem" if use_https else None,
    )
