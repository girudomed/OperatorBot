# Файл: app/telegram/handlers/system_menu.py

"""
Обработчики кнопок главного меню: «⚙️ Система» и «ℹ️ Помощь».

Позволяет запускать базовые диагностические действия прямо из Telegram.
"""

from __future__ import annotations

import html
from collections import deque
from datetime import datetime, timedelta
import re
from functools import partial
from pathlib import Path
from typing import Optional, Iterable, Deque
from io import BytesIO
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.db.manager import DatabaseManager
from app.db.repositories.roles import RolesRepository
from app.db.utils_schema import clear_schema_cache
from app.logging_config import get_watchdog_logger
from app.services.admin_logger import AdminActionLogger
from app.telegram.handlers.auth import help_command
from app.telegram.utils.logging import describe_user
from app.utils.error_handlers import log_async_exceptions
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.keyboards.inline_system import build_system_menu
from watch_dog.config import LOG_DIR, MAIN_LOG_FILE, ERROR_LOG_FILE

logger = get_watchdog_logger(__name__)


class SystemMenuHandler:
    """Отвечает за вывод системного меню и обработку его действий."""

    LOG_PATHS = [
        Path(LOG_DIR) / MAIN_LOG_FILE,
        Path(LOG_DIR) / ERROR_LOG_FILE,
        Path("logs/operabot.log"),
        Path("logs/errors.log"),
        Path("logs/app.log"),
        Path("logs/logs.log"),
    ]
    TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
    ALLOWED_ROLES = {"founder", "head_of_registry"}
    MAX_LOG_LINES = 40
    MAX_LOG_BYTES = 5 * 1024 * 1024
    ERROR_LOOKBACK_DAYS = 7
    TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        db_manager: DatabaseManager,
        permissions: PermissionsManager,
    ):
        self.db_manager = db_manager
        self.permissions = permissions
        self.roles_repo = RolesRepository(db_manager)
        self.action_logger = AdminActionLogger(db_manager)

    @log_async_exceptions
    async def handle_system_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Открывает системное меню по команде или кнопке."""
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        if not await self._can_use_system(user.id, user.username):
            await message.reply_text(
                "❌ У вас нет доступа к системным действиям. Обратитесь к разработчику."
            )
            return
        logger.info(
            "[SYSTEM_MENU] Пользователь %s открыл системное меню",
            describe_user(user),
        )

        include_cache_reset = self.permissions.is_dev_admin(user.id, user.username)

        await message.reply_text(
            "⚙️ <b>Системные функции</b>\nВыберите действие:",
            parse_mode="HTML",
            reply_markup=build_system_menu(include_cache_reset),
        )

    @log_async_exceptions
    async def handle_system_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Обработка callback-кнопок системного меню."""
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        try:
            await query.answer()
        except BadRequest:
            pass

        if not await self._can_use_system(user.id, user.username):
            try:
                await query.answer("Недостаточно прав", show_alert=True)
            except BadRequest:
                pass
            return

        action = (query.data or "").replace("system_", "", 1)
        include_cache_reset = self.permissions.is_dev_admin(user.id, user.username)

        simple_reply_actions = {"status", "errors", "logs", "clear_cache"}

        try:
            if action == "status":
                text = await self._collect_status()
            elif action == "errors":
                text = await self._collect_recent_errors()
            elif action == "check":
                text = await self._run_integrity_checks()
            elif action == "logs":
                text = await self._send_logs(query)
            elif action == "clear_cache":
                if not include_cache_reset:
                    text = "❌ Доступ к очистке кеша разрешён только Dev Admin."
                else:
                    text = await self._clear_caches()
            elif action == "back":
                text = "⚙️ <b>Системные функции</b>\nВыберите действие:"
            else:
                text = "Неизвестное действие."
            await self._log_system_action(user.id, action, text)
        except Exception as exc:
            logger.exception("system_%s failed for user %s", action, user.id)
            text = f"❌ Ошибка при выполнении действия: {exc}"

        try:
            if action in simple_reply_actions:
                await query.message.reply_text(text, parse_mode="HTML")
            else:
                await query.edit_message_text(
                    text=text,
                    parse_mode="HTML",
                    reply_markup=build_system_menu(include_cache_reset),
                )
        except Exception:
            logger.debug("Не удалось обновить сообщение системного меню", exc_info=True)

    async def _can_use_system(self, user_id: int, username: Optional[str]) -> bool:
        """Проверяет, доступно ли системное меню пользователю."""
        if self.permissions.is_supreme_admin(user_id, username):
            return True
        if self.permissions.is_dev_admin(user_id, username):
            return True
        role = await self.permissions.get_effective_role(user_id, username)
        return role in self.ALLOWED_ROLES

    async def _collect_status(self) -> str:
        lines = ["⚙️ <b>Состояние системы</b>"]
        try:
            row = await self.db_manager.execute_with_retry(
                "SELECT VERSION() as ver", fetchone=True
            )
            version = row.get("ver") if row else "—"
            lines.append(f"✅ Подключение к БД активно (MySQL {version})")
        except Exception as exc:
            logger.error("Не удалось получить статус БД: %s", exc, exc_info=True)
            lines.append(f"❌ БД недоступна: {exc}")

        pool = getattr(self.db_manager, "pool", None)
        if pool:
            maxsize = getattr(pool, "maxsize", "?")
            minsize = getattr(pool, "minsize", "?")
            lines.append(f"ℹ️ Пул соединений: min={minsize}, max={maxsize}")
        else:
            lines.append("ℹ️ Пул соединений ещё не инициализирован.")

        return "\n".join(lines)

    async def _collect_recent_errors(self) -> str:
        errors = self._grep_logs(
            paths=self.LOG_PATHS,
            limit=10,
        )
        if not errors:
            return "✅ В логе нет ошибок за последнюю сессию."
        return "❌ <b>Последние ошибки</b>:\n" + "\n".join(errors)

    async def _send_logs(self, query) -> str:
        log_path = None
        # Дедупим пути (часто один и тот же файл доступен по двум путям)
        seen = set()
        candidates = []
        for path in self.LOG_PATHS:
            if not path.exists():
                continue
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path
            if resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(path)
        if not candidates:
            return "📄 Логи недоступны (файлы не найдены)."

        candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        main_candidate = None
        error_candidates = []
        for candidate in candidates:
            name = candidate.name.lower()
            if "error" in name:
                error_candidates.append(candidate)
            elif main_candidate is None:
                main_candidate = candidate

        sent_files = 0
        # Считаем дубликаты имён, чтобы корректно назвать файлы в Telegram
        name_counts = {}
        for candidate in candidates:
            name_counts[candidate.name] = name_counts.get(candidate.name, 0) + 1

        if main_candidate:
            try:
                raw_text = self._read_log_tail_text(main_candidate, self.MAX_LOG_BYTES)
                tail_text = self._filter_recent_log_text(raw_text, self.ERROR_LOOKBACK_DAYS)
                if not tail_text.strip():
                    logger.info("Лог %s пустой за последние %s дней, отправляем хвост без фильтра", main_candidate, self.ERROR_LOOKBACK_DAYS)
                    tail_text = raw_text
                if not tail_text.strip():
                    logger.info("Лог %s полностью пустой, пропускаем", main_candidate)
                    tail_text = None
                log_path = main_candidate
                filename_override = None
                if name_counts.get(log_path.name, 0) > 1:
                    filename_override = f"{log_path.parent.name}_{log_path.name}"
                if tail_text:
                    await self._send_logs_file(query, tail_text, log_path, filename_override)
                    sent_files += 1
            except Exception as exc:
                logger.warning("Не удалось прочитать лог %s: %s", main_candidate, exc)

        for err_path in error_candidates:
            try:
                raw_text = self._read_log_tail_text(err_path, self.MAX_LOG_BYTES)
                tail_text = raw_text
                if not tail_text.strip():
                    logger.info("Лог %s полностью пустой, пропускаем", err_path)
                    continue
                filename_override = None
                if name_counts.get(err_path.name, 0) > 1:
                    filename_override = f"{err_path.parent.name}_{err_path.name}"
                await self._send_logs_file(query, tail_text, err_path, filename_override)
                sent_files += 1
            except Exception as exc:
                logger.warning("Не удалось прочитать лог %s: %s", err_path, exc)

        if not sent_files:
            return f"📄 За последние {self.ERROR_LOOKBACK_DAYS} дней логов не найдено."
        return f"📄 Отправлено файлов логов: {sent_files}."

    def _grep_logs(
        self,
        paths: Iterable[Path],
        limit: int,
        include_tracebacks: bool = True,
    ) -> Deque[str]:
        level_re = re.compile(r" - (ERROR|CRITICAL|EXCEPTION) - ", re.IGNORECASE)
        tb_keyword = "traceback"
        bucket: Deque[str] = deque(maxlen=limit)
        # Unique paths while preserving order
        unique_paths = list(dict.fromkeys(paths))
        existing = [path for path in unique_paths if path.exists()]
        existing.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        if not existing:
            return bucket
        error_paths = [path for path in existing if "error" in path.name.lower()]
        if error_paths:
            target_paths = error_paths + [p for p in existing if p not in error_paths][:1]
        else:
            target_paths = existing[:1]
        # Если есть errors.log, смотрим его; иначе берём самый свежий лог.
        for path in target_paths:
            if not path.exists():
                continue
            try:
                last_stamp = ""
                include_current = False
                cutoff = datetime.now(ZoneInfo("Europe/Moscow")) - timedelta(days=self.ERROR_LOOKBACK_DAYS)
                cutoff_naive = cutoff.replace(tzinfo=None)
                for line in self._read_log_lines(path):
                    normalized = line.rstrip()
                    if not normalized:
                        continue
                    ts_match = self.TIMESTAMP_RE.search(normalized.lstrip())
                    if ts_match:
                        last_stamp = ts_match.group(0)
                        include_current = self._is_recent_timestamp(last_stamp, cutoff_naive)
                        if not include_current:
                            continue
                    lower = normalized.lower()
                    if not include_current:
                        continue
                    if level_re.search(normalized):
                        bucket.append(f"[{path.name}] {normalized}")
                    elif include_tracebacks and tb_keyword in lower:
                        prefix = f"{last_stamp} | " if last_stamp and not ts_match else ""
                        bucket.append(f"[{path.name}] {prefix}{normalized}")
            except Exception as exc:
                logger.warning("Не удалось прочитать лог %s: %s", path, exc)
        return bucket

    async def _send_logs_file(
        self,
        query,
        log_text: str,
        log_path: Optional[Path],
        filename_override: Optional[str] = None,
    ) -> None:
        message = getattr(query, "message", None)
        if not message:
            logger.warning("Нет message для отправки логов файлом")
            return
        buffer = BytesIO()
        buffer.write(log_text.encode("utf-8"))
        buffer.seek(0)
        filename = filename_override or (log_path.name if isinstance(log_path, Path) else "logs.txt") or "logs.txt"
        caption = f"📄 Логи ({filename})"
        await message.reply_document(
            document=buffer,
            filename=filename,
            caption=caption,
        )
    def _read_log_lines(self, path: Path) -> list[str]:
        text = self._decode_log_bytes(path.read_bytes())
        return text.splitlines()

    def _read_log_tail_text(self, path: Path, max_bytes: int) -> str:
        data = path.read_bytes()
        if len(data) > max_bytes:
            data = data[-max_bytes:]
        return self._decode_log_bytes(data)

    def _filter_recent_log_text(self, text: str, lookback_days: int) -> str:
        cutoff = datetime.now(ZoneInfo("Europe/Moscow")) - timedelta(days=lookback_days)
        cutoff_naive = cutoff.replace(tzinfo=None)
        kept_lines = []
        include_current = True
        for line in text.splitlines():
            if not line:
                continue
            ts_match = self.TIMESTAMP_RE.search(line.lstrip())
            if ts_match:
                include_current = self._is_recent_timestamp(ts_match.group(0), cutoff_naive)
                if not include_current:
                    continue
            if include_current:
                kept_lines.append(line)
        return "\n".join(kept_lines)

    def _decode_log_bytes(self, data: bytes) -> str:
        for encoding in ("utf-8", "cp1251", "latin-1"):
            try:
                return data.decode(encoding)
            except UnicodeDecodeError:
                continue
        return data.decode("utf-8", errors="replace")

    def _is_recent_timestamp(self, timestamp: str, cutoff: datetime) -> bool:
        try:
            dt = datetime.strptime(timestamp, self.TIMESTAMP_FMT)
        except ValueError:
            return True
        return dt >= cutoff

    @log_async_exceptions
    async def handle_last_errors_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Выводит последние ошибки/трейсбеки из всех логов."""
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        if not (self.permissions.is_supreme_admin(user.id, user.username) or self.permissions.is_dev_admin(user.id, user.username)):
            await message.reply_text("❌ Команда доступна только разработчикам/основателям.")
            return

        errors = self._grep_logs(self.LOG_PATHS, limit=40)
        if not errors:
            await message.reply_text("✅ В логах нет сообщений уровней ERROR/Traceback.")
            return

        snippet = "\n".join(errors)
        escaped = html.escape(snippet)
        cropped = escaped[-3800:]
        await message.reply_text(
            "❌ <b>Последние ошибки/Traceback</b>\n"
            f"<code>{cropped}</code>",
            parse_mode="HTML",
        )

    async def _run_integrity_checks(self) -> str:
        status_text = await self._collect_status()
        if status_text.startswith("⚙️ <b>Состояние системы</b>"):
            status_text = status_text.replace(
                "⚙️ <b>Состояние системы</b>",
                "🔌 <b>Проверка БД</b>",
                1,
            )
        return status_text
    async def _clear_caches(self) -> str:
        self.roles_repo.clear_cache()
        self.permissions.clear_cache()
        clear_schema_cache()
        return "🗑️ Кэши ролей и схемы очищены."

    async def _log_system_action(self, user_id: int, action: str, text: str) -> None:
        try:
            await self.action_logger.log_action(
                actor_telegram_id=user_id,
                action="system_action",
                payload={"action": action, "result": text[:2000]},
            )
        except Exception:
            logger.debug("Не удалось записать system_action в лог", exc_info=True)


def register_system_handlers(
    application: Application,
    db_manager: DatabaseManager,
    permissions_manager: PermissionsManager,
) -> None:
    """Регистрирует обработчики для системного меню и кнопки помощи."""
    handler = SystemMenuHandler(db_manager, permissions_manager)
    application.add_handler(CommandHandler("system", handler.handle_system_command))
    application.add_handler(CommandHandler("last_errors", handler.handle_last_errors_command))
    application.add_handler(
        MessageHandler(
            filters.Regex(r"(?i)^\s*(?:⚙️\s*)?система\s*$"),
            handler.handle_system_command,
        ),
        group=0,
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(r"(?i)^\s*последние\s+ошибки\s*$"),
            handler.handle_last_errors_command,
        ),
        group=0,
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_system_callback, pattern=r"^system_")
    )
    # Кнопка «ℹ️ Помощь» работает как /help
    help_cb = partial(help_command, permissions=permissions_manager)
    application.add_handler(
        MessageHandler(
            filters.Regex(r"(?i)^\s*(?:ℹ️\s*)?помощ[ььи]\s*$"),
            help_cb,
        ),
        group=0,
    )
    application.bot_data["system_menu_handler"] = handler
