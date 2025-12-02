# bot/utils/constants.py
from config import task_queue_config

# Конфигурация очереди задач
MAX_CONCURRENT_TASKS = task_queue_config["worker_count"]
TASK_QUEUE_MAX_SIZE = task_queue_config["queue_max_size"]
TASK_MAX_RETRIES = task_queue_config["max_retries"]
TASK_RETRY_BASE_DELAY = task_queue_config["retry_base_delay"]
TASK_RETRY_BACKOFF = task_queue_config["retry_backoff"]
TASK_RETRY_MAX_DELAY = task_queue_config["retry_max_delay"]
