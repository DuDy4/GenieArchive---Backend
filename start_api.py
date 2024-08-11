import os
import traceback

import uvicorn
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from loguru import logger
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse, RedirectResponse
from common.utils import env_utils

# from starlette_context import middleware, context, plugins
# from starlette_context.middleware import ContextMiddleware

from data.api.api_manager import v1_router

load_dotenv()
app = FastAPI(
    title="Profile Management API",
    description="This is an API for managing users, contacts and profiles.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:1234","http://localhost:5173","https://app.genieai.ai", "https://smashcode-genie-ai.netlify.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    SessionMiddleware, secret_key=env_utils.get("APP_SECRET_KEY"), max_age=3600
)


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

if __name__ == "__main__":
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=PORT, 
        ssl_keyfile="key.pem" if use_https else None, 
        ssl_certfile="cert.pem" if use_https else None
    )
