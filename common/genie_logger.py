import logging

logger = logging.getLogger("genie_logger")
logger.setLevel(logging.INFO)
# Suppress azure logs by setting their log level to WARNING
logging.getLogger("azure.core").setLevel(logging.WARNING)
logging.getLogger("azure.monitor").setLevel(logging.WARNING)
logging.getLogger("opentelemetry.attributes").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("asyncio.default_exception_handler").setLevel(logging.ERROR)
logging.getLogger("azure.eventhub").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)


# log_format = "{asctime} [{levelname}] - {name}.{funcName}: {message}"
log_format = "{asctime} [{levelname}] - {name}.{funcName} - {filename}:{lineno}: {message}"
logging.basicConfig(level=logging.INFO, format=log_format, style="{")
# logger.propagate = False
import uuid
from contextvars import ContextVar

context_id = ContextVar("context_id", default=None)
topic = ContextVar("topic", default=None)
endpoint = ContextVar("endpoint", default=None)
email = ContextVar("email", default=None)
function = ContextVar("function_name", default=None)
tenant_id = ContextVar("tenant_id", default=None)

class GenieLogger:
    def __init__(self):
        self.logger = logger

    def get_logger(self):
        return self.logger

    def get_ctx_id(self):
        return context_id.get()

    def get_topic(self):
        return topic.get()

    def get_endpoint(self):
        return endpoint.get()

    def get_email(self):
        return email.get()
    
    def get_function(self):
        return function.get()
    
    def get_tenant_id(self):
        return tenant_id.get()

    def generate_short_context_id(self):
        full_uuid = str(uuid.uuid4())
        short_uuid = full_uuid.replace("-", "")[
            :12
        ]  # Currently using the first 12 characters of the UUID for readability of the logs
        return short_uuid

    def get_extra(self):
        extra_object = {}
        if self.get_ctx_id():
            extra_object["ctx_id"] = self.get_ctx_id()
        if self.get_topic():
            extra_object["topic"] = self.get_topic()
        if self.get_endpoint():
            extra_object["endpoint"] = self.get_endpoint()
        if self.get_email():
            extra_object["email"] = self.get_email()
        if self.get_function():
            extra_object["function"] = self.get_function()
        if self.get_tenant_id():
            extra_object["tenant_id"] = self.get_tenant_id()
        return extra_object

    def set_topic(self, topic_name):
        if topic_name:
            topic.set(topic_name)

    def set_endpoint(self, endpoint_name):
        if endpoint_name:
            endpoint.set(endpoint_name)

    def set_email(self, email_address):
        if email_address:
            email.set(email_address)

    def set_function(self, function_name):
        if function_name:
            function.set(function_name)

    def set_tenant_id(self, tenant_id_value):
        if tenant_id_value:
            tenant_id.set(tenant_id_value)

    def bind_context(self, ctx_id=None):
        if ctx_id is None:
            ctx_id = self.generate_short_context_id()
        context_id.set(ctx_id)
        return ctx_id

    def log(self, level, message, *args, **kwargs):
        ctx_id = context_id.get()
        if ctx_id:
            message = f"[CTX={ctx_id}] {message}"
        if args:
            message = message % args if "%" in message else f"{message} {' '.join(map(str, args))}"

        self.logger.log(level, message, stacklevel=3, extra=self.get_extra(), **kwargs)

    def info(self, message, *args, **kwargs):
        self.log(logging.INFO, message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self.log(logging.WARN, message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.log(logging.ERROR, message, *args, **kwargs)

    def debug(self, message, *args, **kwargs):
        self.log(logging.DEBUG, message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        self.log(logging.CRITICAL, message, *args, **kwargs)
