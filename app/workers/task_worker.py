"""
Модуль очереди задач и воркеров.
"""

import asyncio
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Set, Tuple

from telegram.ext import Application

from app.config import TASK_QUEUE_CONFIG
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

# Конфигурация из app/config.py
MAX_CONCURRENT_TASKS = TASK_QUEUE_CONFIG["worker_count"]
TASK_QUEUE_MAX_SIZE = TASK_QUEUE_CONFIG["queue_max_size"]
TASK_MAX_RETRIES = TASK_QUEUE_CONFIG["max_retries"]
TASK_RETRY_BASE_DELAY = TASK_QUEUE_CONFIG["retry_base_delay"]
TASK_RETRY_BACKOFF = TASK_QUEUE_CONFIG["retry_backoff"]
TASK_RETRY_MAX_DELAY = TASK_QUEUE_CONFIG["retry_max_delay"]

# Глобальные переменные очереди
task_queue: "asyncio.Queue[Optional[Dict[str, Any]]]" = asyncio.Queue(
    maxsize=TASK_QUEUE_MAX_SIZE
)
task_registry: Dict[str, Dict[str, Any]] = {}
_retry_tasks: Set[asyncio.Task] = set()
_worker_lock = asyncio.Lock()


class TaskStatus(str, Enum):
    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    RETRY_SCHEDULED = "retry_scheduled"
    COMPLETED = "completed"
    FAILED = "failed"


class QueueFullError(Exception):
    """Ошибка переполнения очереди задач."""
    def __init__(self, message: str):
        super().__init__(message)
        self.user_message = message


async def start_workers(application: Application):
    """
    Запускает воркеры очереди. Идемпотентен — повторные вызовы ничего не делают.
    """
    async with _worker_lock:
        # Используем bot_data для хранения ссылок на воркеры
        existing = application.bot_data.get("_task_queue_workers")
        if existing:
            return

        workers = []
        for i in range(MAX_CONCURRENT_TASKS):
            task = asyncio.create_task(
                worker(task_queue, application), name=f"report-worker-{i}"
            )
            workers.append(task)
        application.bot_data["_task_queue_workers"] = workers
        logger.info(f"Запущено {len(workers)} воркеров очереди задач.")


async def stop_workers(application: Application):
    """
    Корректно останавливает воркеров (graceful shutdown).
    """
    workers = application.bot_data.get("_task_queue_workers")
    if not workers:
        return

    for _ in workers:
        await task_queue.put(None)

    await asyncio.gather(*workers, return_exceptions=True)
    application.bot_data["_task_queue_workers"] = []
    logger.info("Воркеры очереди задач остановлены.")


def _track_retry_task(coro):
    task = asyncio.create_task(coro)
    _retry_tasks.add(task)
    task.add_done_callback(_retry_tasks.discard)


def _update_task_state(task_id: str, **fields: Any) -> None:
    entry = task_registry.get(task_id)
    if not entry:
        entry = {"task_id": task_id, "created_at": datetime.utcnow()}
        task_registry[task_id] = entry
    entry.update(fields)
    entry["updated_at"] = datetime.utcnow()


async def _schedule_retry(queue: asyncio.Queue, task: Dict[str, Any], delay: float):
    await asyncio.sleep(delay)
    _update_task_state(task["task_id"], status=TaskStatus.QUEUED.value, next_retry_in=0)
    await queue.put(task)
    logger.info(
        "Задача %s повторно добавлена в очередь (попытка #%s).",
        task["task_id"],
        task["attempts"] + 1,
    )


def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает состояние задачи по task_id (для команды /task_status).
    """
    entry = task_registry.get(task_id)
    return dict(entry) if entry else None


async def worker(queue: asyncio.Queue, application: Application):
    while True:
        task = await queue.get()
        if task is None:
            queue.task_done()
            break

        task_id = task["task_id"]
        user_id = task["user_id"]
        # report_type = task["report_type"] # Пока не используется
        period = task["period"]
        chat_id = task["chat_id"]
        date_range = task["date_range"]

        _update_task_state(
            task_id,
            status=TaskStatus.IN_PROGRESS.value,
            attempts=task.get("attempts", 0),
        )

        try:
            # Получаем db_manager и report_service из bot_data
            db_manager = application.bot_data.get("db_manager")
            report_service = application.bot_data.get("report_service")

            if not db_manager:
                raise RuntimeError("DatabaseManager not found in application.bot_data")
            if not report_service:
                raise RuntimeError("ReportService not found in application.bot_data")

            async with db_manager.acquire() as connection:
                report = await report_service.generate_report(
                    user_id=user_id, period=period, date_range=date_range
                )

            if chat_id is not None:
                if report and not report.startswith("Ошибка:"):
                    # Отправляем сообщение
                    # Разбиваем длинные сообщения
                    if len(report) > 4096:
                        for x in range(0, len(report), 4096):
                            await application.bot.send_message(chat_id, report[x:x+4096])
                    else:
                        await application.bot.send_message(chat_id, report)
                        
                    logger.info(
                        "Отчёт для user_id=%s (task_id=%s) отправлен.", user_id, task_id
                    )
                else:
                    logger.info(
                        "Отчёт для user_id=%s (task_id=%s) с ошибкой или пустой.",
                        user_id,
                        task_id,
                    )
            else:
                logger.debug(
                    "chat_id=None, оператор %s. Отчёт сгенерирован без отправки. task_id=%s",
                    user_id,
                    task_id,
                )

            _update_task_state(
                task_id,
                status=TaskStatus.COMPLETED.value,
                finished_at=datetime.utcnow(),
            )
        except Exception as e:
            logger.error(
                "Ошибка при обработке задачи %s для user_id=%s: %s",
                task_id,
                user_id,
                e,
                exc_info=True,
            )
            task["attempts"] = task.get("attempts", 0) + 1
            _update_task_state(
                task_id,
                status=TaskStatus.FAILED.value,
                error=str(e),
                attempts=task["attempts"],
            )

            if task["attempts"] < TASK_MAX_RETRIES:
                delay = min(
                    TASK_RETRY_BASE_DELAY
                    * (TASK_RETRY_BACKOFF ** max(task["attempts"] - 1, 0)),
                    TASK_RETRY_MAX_DELAY,
                )
                _update_task_state(
                    task_id,
                    status=TaskStatus.RETRY_SCHEDULED.value,
                    next_retry_in=delay,
                )
                _track_retry_task(_schedule_retry(queue, task, delay))
                logger.warning(
                    "Задача %s будет повторена через %s секунд (попытка %s/%s).",
                    task_id,
                    delay,
                    task["attempts"],
                    TASK_MAX_RETRIES,
                )
            else:
                if chat_id:
                    try:
                        await application.bot.send_message(
                            chat_id=chat_id,
                            text="Ошибка при генерации отчёта после нескольких попыток. Попробуйте позже.",
                        )
                    except Exception as send_err:
                        logger.error(f"Не удалось отправить уведомление об ошибке: {send_err}")

        finally:
            queue.task_done()


async def add_task(
    application: Application, user_id, report_type, period, chat_id=None, date_range=None
) -> Tuple[str, TaskStatus]:
    """
    Добавляет задачу в очередь и возвращает её идентификатор и статус постановки.
    """
    await start_workers(application)

    if task_queue.full():
        logger.warning(
            "Очередь задач переполнена (max=%s). user_id=%s, report_type=%s",
            TASK_QUEUE_MAX_SIZE,
            user_id,
            report_type,
        )
        raise QueueFullError(
            "Очередь генерации отчётов переполнена. Попробуйте позже."
        )

    task_id = uuid.uuid4().hex
    task = {
        "task_id": task_id,
        "user_id": user_id,
        "report_type": report_type,
        "period": period,
        "chat_id": chat_id,
        "date_range": date_range,
        "attempts": 0,
    }

    _update_task_state(
        task_id,
        status=TaskStatus.QUEUED.value,
        attempts=0,
        user_id=user_id,
        report_type=report_type,
        period=period,
        chat_id=chat_id,
        date_range=date_range,
    )

    await task_queue.put(task)
    logger.info(
        "Задача %s добавлена в очередь: user_id=%s, report_type=%s, period=%s.",
        task_id,
        user_id,
        report_type,
        period,
    )

    if isinstance(chat_id, int):
        logger.debug("Можно отправить уведомление пользователю %s о постановке.", chat_id)
    else:
        logger.debug("chat_id отсутствует для user_id=%s, уведомление не требуется.", user_id)

    return task_id, TaskStatus.QUEUED
