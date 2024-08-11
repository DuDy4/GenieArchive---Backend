import os
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def get(key: str, default: str = None):
    env_variable = os.getenv(key, default)
    if env_variable is None:
        logger.error(f"Environment variable {key} not found")
    if isinstance(env_variable, str):
        return env_variable.strip() if env_variable != " " else " "
    return env_variable
