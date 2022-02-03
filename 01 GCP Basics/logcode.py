from google.cloud import logging
logging_client = logging.Client()
log_name = "msn"
logger = logging_client.logger(log_name)
text = "first log"
logger.log_text(text)
print(f"Logged: {text}")