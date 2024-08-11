import os
import traceback

import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse, RedirectResponse
from fastapi import Depends, FastAPI, Request, HTTPException


# from starlette_context import middleware, context, plugins
# from starlette_context.middleware import ContextMiddleware

from common.utils import env_utils
from data.api.mock_api import v1_router
from common.genie_logger import GenieLogger
logger = GenieLogger()

load_dotenv()
app = FastAPI(
    title="Profile Management API",
    description="This is a mock API for managing profiles.",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:1234"],
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
def read_root():
    return RedirectResponse(url="/docs")


app.include_router(v1_router)

MOCK_PORT = int(env_utils.get("MOCK_PORT", 8001))


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=MOCK_PORT,
        ssl_keyfile="./key.pem",
        ssl_certfile="./cert.pem",
    )
