import os

import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv
from starlette.middleware.sessions import SessionMiddleware
from starlette_context import middleware, context, plugins
from starlette_context.middleware import ContextMiddleware
from fastapi.middleware.cors import CORSMiddleware
import common.utils.env_utils as env_utils


from api_gateway.api_manager import v1_router

load_dotenv()
app = FastAPI()

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
app.add_middleware(
    ContextMiddleware,
    plugins=(plugins.RequestIdPlugin(), plugins.CorrelationIdPlugin()),
)


# app.add_middleware(CustomSessionMiddleware)

app.include_router(v1_router)

PORT = int(env_utils.get("APP_PORT", 3000))

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        ssl_keyfile="../key.pem",
        ssl_certfile="../cert.pem",
    )
