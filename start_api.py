import os
import traceback

import uvicorn
from fastapi import FastAPI, Request
from dotenv import load_dotenv

# from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from common.utils import env_utils

from data.api.api_manager import v1_router
from common.genie_logger import GenieLogger

logger = GenieLogger()
# configure_azure_monitor()

load_dotenv()

GENIE_CONTEXT_HEADER = "genie-context"


class GenieContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        genie_context = None
        if request.headers and GENIE_CONTEXT_HEADER in request.headers:
            genie_context = request.headers[GENIE_CONTEXT_HEADER]
        if not genie_context:
            logger.bind_context()
        else:
            logger.info(f"Found Genie context")
        if request.url and request.url.path:
            logger.set_endpoint(request.url.path)
        response = await call_next(request)
        return response


app = FastAPI(
    title="Profile Management API",
    description="This is an API for managing users, contacts and profiles.",
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


@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception):
    logger.error(f"Request failed {exc}")
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
logger.info(f"Starting API on port {PORT} with HTTPS: {use_https}")
if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        ssl_keyfile="key.pem" if use_https else None,
        ssl_certfile="cert.pem" if use_https else None,
    )
