# Файл: app/telegram/handlers/admin_commands.py

"""
Быстрые команды для админских действий.
"""

from typing import List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from app.core.roles import (
    ROLE_ID_TO_NAME,
    ROLE_NAME_TO_ID,
    role_display_name_from_name,
    role_name_from_id,
)
from app.db.repositories.admin import AdminRepository
from app.logging_config import get_watchdog_logger
from app.services.notifications import NotificationService
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)
COMMAND_CALLBACK_PREFIX = "admincmd"


class AdminCommandsHandler:
    """Быстрые команды для админов."""

    def __init__(
        self,
        admin_repo: AdminRepository,
        permissions: PermissionsManager,
        notifications: NotificationService,
    ):
        self.admin_repo = admin_repo
        self.permissions = permissions
        self.notifications = notifications
        self.list_limit = 10
        self._roles_help_text = self._build_roles_help_text()

    def _build_roles_help_text(self) -> str:
        lines = []
        for role_id in sorted(ROLE_ID_TO_NAME.keys()):
            role_name = ROLE_ID_TO_NAME[role_id]
            display = role_display_name_from_name(role_name)
            lines.append(f"- {role_name}: {display} (ID {role_id})")
        return "\n".join(lines)

    @log_async_exceptions
    async def approve_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """
        Команда /approve <user_id>
        Быстрое утверждение пользователя.
        """
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        can_approve = await self.permissions.can_approve(user.id, user.username)
        if not can_approve:
            await message.reply_text("❌ Недостаточно прав")
            return

        if not context.args:
            await self._show_pending_requests(message)
            return

        try:
            target_user_id = int(context.args[0])
        except ValueError as exc:
            logger.warning(
                "Некорректный user_id в /approve от %s: %s",
                describe_user(user),
                exc,
                exc_info=True,
            )
            await message.reply_text("❌ user_id должен быть числом")
            return

        success = await self.admin_repo.approve_user(target_user_id, user.id)
        if success:
            await self._notify_approval(target_user_id, user.full_name)
            await message.reply_text(f"✅ Пользователь #{target_user_id} утвержден!")
        else:
            await message.reply_text(
                f"❌ Не удалось утвердить пользователя #{target_user_id}"
            )

    @log_async_exceptions
    async def make_admin_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """
        Команда /make_admin <user_id>
        Повышает пользователя до admin.
        """
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        can_promote = await self.permissions.can_promote(
            user.id, "admin", user.username
        )
        if not can_promote:
            await message.reply_text("❌ Недостаточно прав для повышения")
            return

        if not context.args:
            await self._show_promotion_candidates(message, target_role="admin")
            return

        try:
            target_user_id = int(context.args[0])
        except ValueError as exc:
            logger.warning(
                "Некорректный user_id в /make_admin от %s: %s",
                describe_user(user),
                exc,
                exc_info=True,
            )
            await message.reply_text("❌ user_id должен быть числом")
            return

        success = await self.admin_repo.promote_user(target_user_id, "admin", user.id)
        if success:
            await self._notify_promotion(target_user_id, "admin", user.full_name)
            await message.reply_text("✅ Роль пользователя обновлена.")
        else:
            await message.reply_text("❌ Ошибка при повышении")

    @log_async_exceptions
    async def make_superadmin_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """
        Команда /make_superadmin <user_id>
        Повышает до superadmin (только для supreme/dev admin).
        """
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        can_promote = await self.permissions.can_promote(
            user.id, "superadmin", user.username
        )
        if not can_promote:
            await message.reply_text(
                "❌ Только Supreme Admin может назначать superadmin"
            )
            return

        if not context.args:
            await self._show_promotion_candidates(message, target_role="superadmin")
            return

        try:
            target_user_id = int(context.args[0])
        except ValueError as exc:
            logger.warning(
                "Некорректный user_id в /make_superadmin от %s: %s",
                describe_user(user),
                exc,
                exc_info=True,
            )
            await message.reply_text("❌ user_id должен быть числом")
            return

        success = await self.admin_repo.promote_user(
            target_user_id, "superadmin", user.id
        )
        if success:
            await message.reply_text("✅ Роль пользователя обновлена.")
        else:
            await message.reply_text("❌ Ошибка при повышении")

    @log_async_exceptions
    async def set_role_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """
        Команда /set_role <telegram_id> <role>
        Позволяет назначить любую доступную роль.
        """
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        can_manage_roles = await self.permissions.can_manage_roles(user.id, user.username)
        if not can_manage_roles:
            await message.reply_text("❌ Недостаточно прав для управления ролями.")
            return

        if len(context.args) < 2:
            await message.reply_text(
                "Использование: /set_role <telegram_id> <role>\nДоступные роли:\n"
                f"{self._roles_help_text}"
            )
            return

        try:
            target_user_id = int(context.args[0])
        except ValueError as exc:
            logger.warning(
                "Некорректный user_id в /set_role от %s: %s",
                describe_user(user),
                exc,
                exc_info=True,
            )
            await message.reply_text("❌ user_id должен быть числом")
            return

        role_slug = context.args[1].lower()
        if role_slug not in ROLE_NAME_TO_ID:
            await message.reply_text(
                "❌ Неизвестная роль.\nДоступные роли:\n" + self._roles_help_text
            )
            return

        if not await self.permissions.can_promote(user.id, role_slug, user.username):
            await message.reply_text("❌ Нельзя назначить эту роль.")
            return

        success = await self.admin_repo.set_user_role(target_user_id, role_slug, user.id)
        if success:
            display = role_display_name_from_name(role_slug)
            await message.reply_text(f"✅ Роль пользователя обновлена: {display}.")
            await self._notify_promotion(target_user_id, role_slug, user.full_name)
        else:
            await message.reply_text("❌ Не удалось обновить роль.")

    @log_async_exceptions
    async def admins_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /admins
        Показывает список всех администраторов.
        """
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        is_admin = await self.permissions.is_admin(user.id, user.username)
        if not is_admin:
            await message.reply_text("❌ Недостаточно прав")
            return

        admins = await self.admin_repo.get_admins()
        if not admins:
            await message.reply_text("👑 Нет администраторов в системе")
            return

        message_text = "👑 <b>Список администраторов:</b>\n\n"
        for admin in admins:
            role_name = admin.get("role") or role_name_from_id(admin.get("role_id"))
            role_emoji = "⭐" if role_name in ("superadmin", "developer", "founder") else "👤"
            message_text += (
                f"{role_emoji} <b>{admin['full_name']}</b>\n"
                f"   @{admin.get('username', 'нет')} | Role: {role_name}\n\n"
            )

        await message.reply_text(message_text, parse_mode="HTML")

    @log_async_exceptions
    async def dev_alert_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """
        Команда /dev
        Отправляет уведомление о техработах всем пользователям.
        """
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        if not (
            self.permissions.is_dev_admin(user.id, user.username)
            or self.permissions.is_supreme_admin(user.id, user.username)
        ):
            await message.reply_text("❌ Команда доступна только разработчикам.")
            return

        alert_text = (
            "⚙️ Бот временно на техобслуживании.\n"
            "Возможны перебои в работе, попробуйте позже."
        )

        recipients = await self.admin_repo.get_users_with_chat_ids()
        sent = 0
        failed = 0
        for recipient in recipients:
            chat_id = recipient.get("chat_id")
            if not chat_id:
                continue
            try:
                await context.bot.send_message(chat_id=chat_id, text=alert_text)
                sent += 1
            except TelegramError as exc:
                failed += 1
                logger.warning(
                    "Не удалось отправить dev-уведомление chat_id=%s: %s",
                    chat_id,
                    exc,
                    exc_info=True,
                )

        await message.reply_text(
            f"Сообщение отправлено {sent} пользователям."
            + (f" Ошибок: {failed}." if failed else "")
        )

    # ---------- Helpers ----------

    async def _show_pending_requests(self, target) -> None:
        pending_users = self._filter_special_accounts(
            await self.admin_repo.get_pending_users()
        )
        if not pending_users:
            text = "⏳ Нет заявок, ожидающих подтверждения."
            keyboard = [
                [
                    InlineKeyboardButton(
                        "🔄 Обновить", callback_data=self._callback("approve", "list")
                    )
                ],
                [InlineKeyboardButton("✖️ Закрыть", callback_data=self._callback("close"))],
            ]
            await self._respond_with_markup(target, text, keyboard)
            return

        keyboard: List[List[InlineKeyboardButton]] = []
        for user in pending_users[: self.list_limit]:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        self._format_user_label(user),
                        callback_data=self._callback("approve", "view", user["id"]),
                    )
                ]
            )
        keyboard.append(
            [
                InlineKeyboardButton(
                    "🔄 Обновить", callback_data=self._callback("approve", "list")
                )
            ]
        )
        keyboard.append([InlineKeyboardButton("✖️ Закрыть", callback_data=self._callback("close"))])

        text = (
            "⏳ <b>Заявки на утверждение</b>\n"
            f"Всего: {len(pending_users)}\n"
            "Выберите пользователя, чтобы открыть карточку."
        )
        await self._respond_with_markup(target, text, keyboard)

    async def _show_promotion_candidates(self, target, target_role: str) -> None:
        candidates = self._filter_special_accounts(
            await self.admin_repo.get_users_for_promotion(target_role=target_role)
        )
        role_label = "администратора" if target_role == "admin" else "супер-админа"

        if not candidates:
            text = f"✅ Нет кандидатов для назначения на роль {role_label}."
            keyboard = [
                [
                    InlineKeyboardButton(
                        "🔄 Обновить",
                        callback_data=self._callback("promote", target_role, "list"),
                    )
                ],
                [InlineKeyboardButton("✖️ Закрыть", callback_data=self._callback("close"))],
            ]
            await self._respond_with_markup(target, text, keyboard)
            return

        keyboard: List[List[InlineKeyboardButton]] = []
        for user in candidates[: self.list_limit]:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        self._format_user_label(user),
                        callback_data=self._callback("promote", target_role, "view", user["id"]),
                    )
                ]
            )
        keyboard.append(
            [
                InlineKeyboardButton(
                    "🔄 Обновить",
                    callback_data=self._callback("promote", target_role, "list"),
                )
            ]
        )
        keyboard.append([InlineKeyboardButton("✖️ Закрыть", callback_data=self._callback("close"))])

        text = (
            f"⬆️ <b>Кандидаты для роли {role_label}</b>\n"
            "Выберите пользователя для подтверждения."
        )
        await self._respond_with_markup(target, text, keyboard)

    async def _respond_with_markup(
        self,
        target,
        text: str,
        keyboard: Optional[List[List[InlineKeyboardButton]]],
    ):
        markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        if isinstance(target, Message):
            await target.reply_text(text, reply_markup=markup, parse_mode="HTML")
        else:
            await safe_edit_message(
                target, text=text, reply_markup=markup, parse_mode="HTML"
            )

    def _callback(self, *parts: object) -> str:
        return ":".join(str(part) for part in (COMMAND_CALLBACK_PREFIX, *parts))

    def _filter_special_accounts(self, users: List[dict]) -> List[dict]:
        filtered = []
        for user in users:
            if not self._is_special_account(user):
                filtered.append(user)
        return filtered

    def _is_special_account(self, user: dict) -> bool:
        telegram_id = user.get("telegram_id")
        username = user.get("username")
        try:
            telegram_id = int(telegram_id) if telegram_id is not None else None
        except (TypeError, ValueError) as exc:
            logger.debug(
                "Не удалось привести telegram_id пользователя %s к int: %s",
                user,
                exc,
                exc_info=True,
            )
            telegram_id = None
        if telegram_id is None:
            return False
        return bool(
            self.permissions.is_supreme_admin(telegram_id, username)
            or self.permissions.is_dev_admin(telegram_id, username)
        )

    def _format_user_label(self, user: dict) -> str:
        name = user.get("full_name") or user.get("username") or f"#{user.get('id')}"
        username = f" @{user['username']}" if user.get("username") else ""
        ext = f" ({user.get('extension')})" if user.get("extension") else ""
        label = f"{name}{username}{ext}"
        return label[:64]

    def _format_user_card_text(self, user: dict) -> str:
        role_name = user.get("role") or role_name_from_id(user.get("role_id"))
        username = f"@{user.get('username')}" if user.get("username") else "—"
        extension = user.get("extension") or "—"
        created_at = user.get("created_at")
        if hasattr(created_at, "strftime"):
            created_str = created_at.strftime("%d.%m.%Y %H:%M")
        else:
            created_str = created_at or "—"
        status = user.get("status", "pending")
        return (
            f"👤 <b>{user.get('full_name', 'Без имени')}</b>\n"
            f"ID: #{user.get('id')}\n"
            f"Username: {username}\n"
            f"Extension: {extension}\n"
            f"Роль: <b>{role_name}</b>\n"
            f"Статус: <b>{status}</b>\n"
            f"Регистрация: {created_str}"
        )

    async def _notify_approval(self, user_id: int, actor_name: str) -> None:
        if not hasattr(self.notifications, "notify_approval"):
            return
        user_data = await self.admin_repo.db.execute_with_retry(
            "SELECT user_id AS telegram_id FROM UsersTelegaBot WHERE id = %s",
            params=(user_id,),
            fetchone=True,
        )
        if user_data:
            await self.notifications.notify_approval(user_data["telegram_id"], actor_name)

    async def _notify_promotion(
        self, user_id: int, role: str, actor_name: Optional[str]
    ):
        if not hasattr(self.notifications, "notify_promotion"):
            return
        user_data = await self.admin_repo.db.execute_with_retry(
            "SELECT user_id AS telegram_id FROM UsersTelegaBot WHERE id = %s",
            params=(user_id,),
            fetchone=True,
        )
        if user_data:
            await self.notifications.notify_promotion(
                user_data["telegram_id"], role, actor_name
            )

    async def _handle_role_callback_permission(
        self, query, *, target_role: Optional[str] = None
    ) -> bool:
        user = query.from_user
        if not user:
            return False
        if target_role:
            return await self.permissions.can_promote(user.id, target_role, user.username)
        return await self.permissions.can_approve(user.id, user.username)

    # ---------- Callback handlers ----------

    @log_async_exceptions
    async def handle_approve_list_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return
        await query.answer()
        if not await self._handle_role_callback_permission(query):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        await self._show_pending_requests(query)

    @log_async_exceptions
    async def handle_approve_view_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return
        await query.answer()
        if not await self._handle_role_callback_permission(query):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        user_id = self._extract_user_id(query.data)
        if not user_id:
            await query.answer("Некорректный пользователь", show_alert=True)
            return
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        keyboard = [
            [
                InlineKeyboardButton(
                    "✅ Одобрить",
                    callback_data=self._callback("approve", "action", "approve", user_id),
                ),
                InlineKeyboardButton(
                    "❌ Отклонить",
                    callback_data=self._callback("approve", "action", "decline", user_id),
                ),
            ],
            [
                InlineKeyboardButton(
                    "◀️ К списку", callback_data=self._callback("approve", "list")
                )
            ],
            [InlineKeyboardButton("✖️ Закрыть", callback_data=self._callback("close"))],
        ]
        await self._respond_with_markup(query, self._format_user_card_text(user), keyboard)

    @log_async_exceptions
    async def handle_approve_action_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return
        await query.answer()
        if not await self._handle_role_callback_permission(query):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        parts = query.data.split(":")
        if len(parts) < 4:
            await query.answer("Некорректный запрос", show_alert=True)
            return
        action = parts[2]
        user_id = self._extract_user_id(query.data)
        if not user_id:
            await query.answer("Некорректный пользователь", show_alert=True)
            return
        actor = query.from_user
        if not actor:
            return
        if action == "approve":
            success = await self.admin_repo.approve_user(user_id, actor.id)
            if success:
                await self._notify_approval(user_id, actor.full_name)
                await query.answer("✅ Пользователь одобрен")
            else:
                await query.answer("Не удалось одобрить", show_alert=True)
                return
        elif action == "decline":
            success = await self.admin_repo.decline_user(user_id, actor.id)
            if success:
                await query.answer("❌ Заявка отклонена")
            else:
                await query.answer("Не удалось отклонить", show_alert=True)
                return
        else:
            await query.answer("Некорректное действие", show_alert=True)
            return
        await self._show_pending_requests(query)

    @log_async_exceptions
    async def handle_promote_list_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return
        await query.answer()
        parts = query.data.split(":")
        if len(parts) < 3:
            await query.answer("Некорректный запрос", show_alert=True)
            return
        target_role = parts[2]
        if not await self._handle_role_callback_permission(query, target_role=target_role):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        await self._show_promotion_candidates(query, target_role=target_role)

    @log_async_exceptions
    async def handle_promote_view_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return
        await query.answer()
        parts = query.data.split(":")
        if len(parts) < 4:
            await query.answer("Некорректный запрос", show_alert=True)
            return
        target_role = parts[2]
        if not await self._handle_role_callback_permission(query, target_role=target_role):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        user_id = self._extract_user_id(query.data)
        if not user_id:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        action_label = "Назначить админом" if target_role == "admin" else "Сделать супер-админом"
        keyboard = [
            [
                InlineKeyboardButton(
                    f"✅ {action_label}",
                    callback_data=self._callback("promote", target_role, "do", user_id),
                )
            ],
            [
                InlineKeyboardButton(
                    "◀️ К кандидатам",
                    callback_data=self._callback("promote", target_role, "list"),
                )
            ],
            [InlineKeyboardButton("✖️ Закрыть", callback_data=self._callback("close"))],
        ]
        await self._respond_with_markup(query, self._format_user_card_text(user), keyboard)

    @log_async_exceptions
    async def handle_promote_action_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return
        await query.answer()
        parts = query.data.split(":")
        if len(parts) < 4:
            await query.answer("Некорректный запрос", show_alert=True)
            return
        target_role = parts[2]
        if not await self._handle_role_callback_permission(query, target_role=target_role):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        user_id = self._extract_user_id(query.data)
        if not user_id:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        actor = query.from_user
        if not actor:
            return
        success = await self.admin_repo.promote_user(user_id, target_role, actor.id)
        if success:
            await self._notify_promotion(user_id, target_role, actor.full_name)
            await query.answer("✅ Роль обновлена")
            await self._show_promotion_candidates(query, target_role=target_role)
        else:
            await query.answer("Не удалось обновить роль", show_alert=True)

    @log_async_exceptions
    async def handle_close_menu(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return
        await query.answer()
        if query.message:
            try:
                await query.message.delete()
            except TelegramError as exc:
                logger.warning(
                    "Не удалось удалить сообщение меню: %s",
                    exc,
                    exc_info=True,
                )
                await safe_edit_message(query, text="Меню закрыто.")

    def _extract_user_id(self, data: str) -> int:
        try:
            return int(data.split(":")[-1])
        except (ValueError, IndexError) as exc:
            logger.warning(
                "Не удалось извлечь user_id из callback '%s': %s",
                data,
                exc,
                exc_info=True,
            )
            return 0


def register_admin_commands_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
    notifications: NotificationService,
):
    """Регистрирует быстрые команды админов."""
    handler = AdminCommandsHandler(admin_repo, permissions, notifications)

    application.add_handler(CommandHandler("approve", handler.approve_command))
    application.add_handler(CommandHandler("make_admin", handler.make_admin_command))
    application.add_handler(
        CommandHandler("make_superadmin", handler.make_superadmin_command)
    )
    application.add_handler(CommandHandler("set_role", handler.set_role_command))
    application.add_handler(CommandHandler("admins", handler.admins_command))
    application.add_handler(CommandHandler("dev", handler.dev_alert_command))
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_approve_list_callback,
            pattern=rf"^{COMMAND_CALLBACK_PREFIX}:approve:list$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_approve_view_callback,
            pattern=rf"^{COMMAND_CALLBACK_PREFIX}:approve:view:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_approve_action_callback,
            pattern=rf"^{COMMAND_CALLBACK_PREFIX}:approve:action:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_promote_list_callback,
            pattern=rf"^{COMMAND_CALLBACK_PREFIX}:promote:(admin|superadmin):list$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_promote_view_callback,
            pattern=rf"^{COMMAND_CALLBACK_PREFIX}:promote:(admin|superadmin):view:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_promote_action_callback,
            pattern=rf"^{COMMAND_CALLBACK_PREFIX}:promote:(admin|superadmin):do:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_close_menu, pattern=rf"^{COMMAND_CALLBACK_PREFIX}:close$"
        )
    )

    logger.info("Admin commands handlers registered")
