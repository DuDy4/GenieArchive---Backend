import os
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
logger = GenieLogger()
load_dotenv()

def get(key: str, default: str = None):
    env_variable = os.getenv(key, default)
    if (env_variable is None):
        logger.warning(f"Environment variable {key} not found")
    if (isinstance(env_variable, str)):
        return env_variable.strip() 
    return env_variable