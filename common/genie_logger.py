from loguru import logger
import uuid
from contextvars import ContextVar

context_id = ContextVar("context_id", default=None)

class GenieLogger:
    def __init__(self):
        self.logger = logger

    def get_ctx_id(self):
        return context_id.get()

    def generate_short_context_id(self):
        full_uuid = str(uuid.uuid4())
        short_uuid = full_uuid.replace("-", "")[:12]  # Currently using the first 12 characters of the UUID for readability of the logs
        return short_uuid

    def bind_context(self, ctx_id=None):
        if ctx_id is None:
            ctx_id = self.generate_short_context_id() 
        context_id.set(ctx_id)
        return ctx_id

    def log(self, level, message, *args, **kwargs):
        ctx_id = context_id.get()
        if ctx_id:
            message = f"[CTX={ctx_id}] {message}"
        self.logger.opt(depth=2).log(level, message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        self.log("INFO", message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self.log("WARNING", message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.log("ERROR", message, *args, **kwargs)

    def debug(self, message, *args, **kwargs):
        self.log("DEBUG", message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        self.log("CRITICAL", message, *args, **kwargs)