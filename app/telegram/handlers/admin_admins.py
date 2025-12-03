"""
Хендлеры управления администраторами.
"""

from typing import List, Dict, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.services.notifications import NotificationService
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.core.roles import role_name_from_id
from app.telegram.utils.logging import describe_user

logger = get_watchdog_logger(__name__)


class AdminAdminsHandler:
    """Управление списком администраторов и повышениями."""

    def __init__(
        self,
        admin_repo: AdminRepository,
        permissions: PermissionsManager,
        notifications: NotificationService,
    ):
        self.admin_repo = admin_repo
        self.permissions = permissions
        self.notifications = notifications
        self._candidates_limit = 10

    @log_async_exceptions
    async def show_admins_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        logger.info(
            "Админ %s открыл список администраторов",
            describe_user(update.effective_user),
        )
        admins = await self.admin_repo.get_admins()
        message = "👑 <b>Администраторы</b>\n\n"
        keyboard: List[List[InlineKeyboardButton]] = []

        if not admins:
            message += "Список администраторов пуст."
        else:
            for admin in admins:
                message += self._format_admin_line(admin)
            for admin in admins[: self._candidates_limit]:
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            admin.get("full_name") or admin.get("username") or "Без имени",
                            callback_data=f"admin:admins:details:{admin['id']}",
                        )
                    ]
                )

        keyboard.append(
            [
                InlineKeyboardButton(
                    "➕ Назначить администратора",
                    callback_data="admin:admins:candidates",
                )
            ]
        )
        keyboard.append(
            [InlineKeyboardButton("◀️ Назад", callback_data="admin:back")]
        )

        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    @log_async_exceptions
    async def show_candidates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        logger.info(
            "Админ %s просматривает кандидатов для назначения",
            describe_user(update.effective_user),
        )
        candidates = await self.admin_repo.get_admin_candidates(
            limit=self._candidates_limit
        )

        if not candidates:
            message = "✅ Все утверждённые пользователи уже имеют роль администратора."
            keyboard = [
                [InlineKeyboardButton("◀️ Назад", callback_data="admin:admins:list")]
            ]
        else:
            message = (
                "➕ <b>Доступные кандидаты</b>\n"
                "Выберите пользователя, чтобы назначить роль администратора."
            )
            keyboard = [
                [
                    InlineKeyboardButton(
                        candidate.get("full_name") or candidate.get("username") or "Без имени",
                        callback_data=f"admin:admins:promote_admin:{candidate['id']}",
                    )
                ]
                for candidate in candidates
            ]
            keyboard.append(
                [InlineKeyboardButton("◀️ Назад", callback_data="admin:admins:list")]
            )

        await query.edit_message_text(
            message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
        )

    @log_async_exceptions
    async def show_admin_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        user_id = self._extract_user_id(query.data)
        if not user_id:
            await query.answer("Некорректный пользователь", show_alert=True)
            return
        logger.info(
            "Админ %s открыл карточку администратора id=%s",
            describe_user(update.effective_user),
            user_id,
        )
        await self._render_admin_details(query, update.effective_user, user_id)

    @log_async_exceptions
    async def promote_to_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)

        if not await self.permissions.can_promote(actor.id, "admin", actor.username):
            await query.answer("Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка назначить администратора без прав: %s -> target_id=%s",
                describe_user(actor),
                user_id,
            )
            return

        success = await self.admin_repo.promote_user(user_id, "admin", actor.id)

        if success:
            await self._notify_promotion(user_id, "admin", actor.full_name)
            await query.edit_message_text(
                "✅ Пользователь назначен администратором.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "◀️ К списку", callback_data="admin:admins:list"
                            )
                        ]
                    ]
                ),
            )
        else:
            await query.answer("Не удалось обновить роль", show_alert=True)
        logger.info(
            "Админ %s назначил пользователя id=%s администратором (успех=%s)",
            describe_user(actor),
            user_id,
            success,
        )

    @log_async_exceptions
    async def promote_to_superadmin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)

        if not await self.permissions.can_promote(
            actor.id, "superadmin", actor.username
        ):
            await query.answer("Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка назначить супер-админа без прав: %s -> target_id=%s",
                describe_user(actor),
                user_id,
            )
            return

        success = await self.admin_repo.promote_user(
            user_id, "superadmin", actor.id
        )

        if success:
            await self._notify_promotion(user_id, "superadmin", actor.full_name)
            await self._refresh_details(update, query, user_id)
        else:
            await query.answer("Не удалось обновить роль", show_alert=True)
        logger.info(
            "Админ %s повысил пользователя id=%s до супер-админа (успех=%s)",
            describe_user(actor),
            user_id,
            success,
        )

    @log_async_exceptions
    async def demote_to_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)
        user = await self.admin_repo.get_user_by_id(user_id)

        if not user:
            await query.answer("Пользователь не найден", show_alert=True)
            return

        if not await self.permissions.can_demote(
            actor.id, user.get("telegram_id"), actor.username
        ):
            await query.answer("Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка понижения admin->admin без прав: %s -> target_id=%s",
                describe_user(actor),
                user_id,
            )
            return

        success = await self.admin_repo.demote_user(user_id, "admin", actor.id)

        if success:
            await self._refresh_details(update, query, user_id)
        else:
            await query.answer("Не удалось изменить роль", show_alert=True)
        logger.info(
            "Админ %s понизил пользователя id=%s до admin (успех=%s)",
            describe_user(actor),
            user_id,
            success,
        )

    @log_async_exceptions
    async def demote_to_operator(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)
        user = await self.admin_repo.get_user_by_id(user_id)

        if not user:
            await query.answer("Пользователь не найден", show_alert=True)
            return

        if not await self.permissions.can_demote(
            actor.id, user.get("telegram_id"), actor.username
        ):
            await query.answer("Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка понижения до оператора без прав: %s -> target_id=%s",
                describe_user(actor),
                user_id,
            )
            return

        success = await self.admin_repo.demote_user(user_id, "operator", actor.id)

        if success:
            await self._refresh_details(update, query, user_id)
        else:
            await query.answer("Не удалось изменить роль", show_alert=True)
        logger.info(
            "Админ %s понизил пользователя id=%s до operator (успех=%s)",
            describe_user(actor),
            user_id,
            success,
        )

    async def _refresh_details(self, update: Update, query, user_id: int):
        await self._render_admin_details(query, update.effective_user, user_id)

    async def _render_admin_details(
        self,
        query,
        actor,
        user_id: int,
    ):
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await query.edit_message_text("❌ Пользователь не найден")
            return

        role_name = user.get("role") or role_name_from_id(user.get("role_id"))
        message = (
            f"👤 <b>{user.get('full_name', 'Без имени')}</b>\n"
            f"Username: @{user.get('username', 'нет')}\n"
            f"Telegram ID: {user.get('telegram_id')}\n"
            f"Роль: <b>{role_name}</b>\n"
            f"Статус: <b>{user.get('status')}</b>\n"
        )

        keyboard = await self._build_admin_actions(actor, user)
        keyboard.append(
            [InlineKeyboardButton("◀️ К списку", callback_data="admin:admins:list")]
        )

        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    async def _build_admin_actions(
        self,
        actor,
        target: Dict[str, Optional[str]],
    ) -> List[List[InlineKeyboardButton]]:
        role_name = target.get("role") or role_name_from_id(target.get("role_id"))
        keyboard: List[List[InlineKeyboardButton]] = []

        if role_name == "admin":
            if await self.permissions.can_promote(
                actor.id, "superadmin", actor.username
            ):
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            "⭐ Сделать супер-админом",
                            callback_data=f"admin:admins:promote_super:{target['id']}",
                        )
                    ]
                )
            if await self.permissions.can_demote(
                actor.id, target.get("telegram_id"), actor.username
            ):
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            "⬇️ В операторов",
                            callback_data=f"admin:admins:demote_operator:{target['id']}",
                        )
                    ]
                )
        elif role_name == "superadmin":
            if await self.permissions.can_demote(
                actor.id, target.get("telegram_id"), actor.username
            ):
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            "⬇️ Понизить до admin",
                            callback_data=f"admin:admins:demote_admin:{target['id']}",
                        )
                    ]
                )

        return keyboard

    def _format_admin_line(self, admin: Dict[str, Optional[str]]) -> str:
        role_name = admin.get("role") or role_name_from_id(admin.get("role_id"))
        role_emoji = "⭐" if role_name == "superadmin" else "👤"
        return (
            f"{role_emoji} <b>{admin.get('full_name', 'Без имени')}</b> "
            f"(@{admin.get('username', 'нет')}) — {role_name}\n"
        )

    async def _notify_promotion(
        self, user_id: int, role: str, actor_name: Optional[str]
    ):
        if not hasattr(self.notifications, "notify_promotion"):
            return
        user = await self.admin_repo.db.execute_with_retry(
            "SELECT telegram_id FROM users WHERE id = %s",
            params=(user_id,),
            fetchone=True,
        )
        if user:
            await getattr(self.notifications, "notify_promotion")(
                user["telegram_id"], role, actor_name
            )

    def _extract_user_id(self, data: str) -> int:
        try:
            return int(data.split(":")[-1])
        except (ValueError, IndexError):
            return 0


def register_admin_admins_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
    notifications: NotificationService,
):
    handler = AdminAdminsHandler(admin_repo, permissions, notifications)

    application.add_handler(
        CallbackQueryHandler(handler.show_admins_list, pattern=r"^admin:admins:list")
    )
    application.add_handler(
        CallbackQueryHandler(handler.show_candidates, pattern=r"^admin:admins:candidates")
    )
    application.add_handler(
        CallbackQueryHandler(handler.show_admin_details, pattern=r"^admin:admins:details:")
    )
    application.add_handler(
        CallbackQueryHandler(handler.promote_to_admin, pattern=r"^admin:admins:promote_admin:")
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.promote_to_superadmin, pattern=r"^admin:admins:promote_super:"
        )
    )
    application.add_handler(
        CallbackQueryHandler(handler.demote_to_admin, pattern=r"^admin:admins:demote_admin:")
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.demote_to_operator, pattern=r"^admin:admins:demote_operator:"
        )
    )

    logger.info("Admin admins handlers registered")
