import os

import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv
from starlette.middleware.sessions import SessionMiddleware

from api_gateway.api_manager import v1_router

load_dotenv()
app = FastAPI()
app.add_middleware(
    SessionMiddleware, secret_key=os.environ.get("APP_SECRET_KEY"), max_age=3600
)

app.include_router(v1_router)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        #port=8444,
        #ssl_keyfile="../key.pem",
        #ssl_certfile="../cert.pem",
    )
