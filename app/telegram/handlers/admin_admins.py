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
from app.telegram.utils.messages import safe_edit_message

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
        self._page_size = 10

    def _parse_list_page(self, data: str) -> int:
        parts = data.split(":")
        if len(parts) > 3:
            try:
                return max(0, int(parts[3]))
            except ValueError:
                return 0
        return 0

    def _parse_page_from_data(self, data: str) -> int:
        parts = data.split(":")
        if len(parts) > 4 and parts[-2].isdigit():
            return max(0, int(parts[-2]))
        return 0

    def _build_list_callback(self, page: int = 0) -> str:
        return f"admin:admins:list:{page}"

    def _build_details_callback(self, user_id: int, page: int = 0) -> str:
        return f"admin:admins:details:{page}:{user_id}"

    @log_async_exceptions
    async def show_admins_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        logger.info(
            "Админ %s открыл список администраторов",
            describe_user(update.effective_user),
            extra={"action": "open_admin_list", "result": "success"},
        )
        admins = await self.admin_repo.get_admins()
        page = self._parse_list_page(query.data)
        total = len(admins)
        max_page = max(0, (total - 1) // self._page_size) if total else 0
        page = min(page, max_page)
        start = page * self._page_size
        end = start + self._page_size
        page_items = admins[start:end]
        message = "👑 <b>Администраторы</b>\n"
        keyboard: List[List[InlineKeyboardButton]] = []

        if not admins:
            message += "Список администраторов пуст."
        else:
            message += f"Показано {start + 1}-{min(end, total)} из {total}\n\n"
            for admin in admins:
                message += self._format_admin_line(admin)
            for admin in page_items:
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            admin.get("full_name") or admin.get("username") or "Без имени",
                            callback_data=self._build_details_callback(admin["id"], page),
                        )
                    ]
                )

        keyboard.append(
            [InlineKeyboardButton("🔄 Обновить", callback_data=self._build_list_callback(page))]
        )
        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=self._build_list_callback(page - 1),
                )
            )
        if page < max_page:
            nav_row.append(
                InlineKeyboardButton(
                    "➡️ Далее",
                    callback_data=self._build_list_callback(page + 1),
                )
            )
        if nav_row:
            keyboard.append(nav_row)
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

        await safe_edit_message(
            query,
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
            extra={"action": "list_admin_candidates", "result": "success"},
        )
        candidates = await self.admin_repo.get_admin_candidates(
            limit=self._candidates_limit
        )

        if not candidates:
            message = "✅ Все утверждённые пользователи уже имеют роль администратора."
            keyboard = [
                [InlineKeyboardButton("🔄 Обновить", callback_data="admin:admins:candidates")],
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
                        callback_data=self._build_details_callback(candidate["id"], 0),
                    )
                ]
                for candidate in candidates
            ]
            keyboard.append(
                [InlineKeyboardButton("🔄 Обновить", callback_data="admin:admins:candidates")]
            )
            keyboard.append(
                [InlineKeyboardButton("◀️ Назад", callback_data="admin:admins:list")]
            )

        await safe_edit_message(
            query,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    @log_async_exceptions
    async def show_admin_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        user_id = self._extract_user_id(query.data)
        page = self._parse_page_from_data(query.data)
        if not user_id:
            await query.answer("Некорректный пользователь", show_alert=True)
            return
        logger.info(
            "Админ %s открыл карточку администратора id=%s",
            describe_user(update.effective_user),
            user_id,
        )
        await self._render_admin_details(query, update.effective_user, user_id, page)

    @log_async_exceptions
    async def promote_to_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)
        page = self._parse_page_from_data(query.data)

        if not await self.permissions.can_promote(actor.id, "admin", actor.username):
            await query.answer("Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка назначить администратора без прав: %s -> target_id=%s",
                describe_user(actor),
                user_id,
                extra={"action": "promote_admin", "result": "permission_denied", "target_user_id": user_id},
            )
            return

        success = await self.admin_repo.promote_user(user_id, "admin", actor.id)

        if success:
            await self._notify_promotion(user_id, "admin", actor.full_name)
            await query.answer("✅ Пользователь назначен администратором", show_alert=False)
            await self._render_admin_details(query, actor, user_id, page)
        else:
            await query.answer("Не удалось обновить роль", show_alert=True)
        logger.info(
            "Админ %s назначил пользователя id=%s администратором (успех=%s)",
            describe_user(actor),
            user_id,
            success,
            extra={"action": "promote_admin", "result": "success" if success else "error", "target_user_id": user_id},
        )

    @log_async_exceptions
    async def promote_to_superadmin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)
        page = self._parse_page_from_data(query.data)

        if not await self.permissions.can_promote(
            actor.id, "superadmin", actor.username
        ):
            await query.answer("Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка назначить супер-админа без прав: %s -> target_id=%s",
                describe_user(actor),
                user_id,
                extra={"action": "promote_superadmin", "result": "permission_denied", "target_user_id": user_id},
            )
            return

        success = await self.admin_repo.promote_user(
            user_id, "superadmin", actor.id
        )

        if success:
            await self._notify_promotion(user_id, "superadmin", actor.full_name)
            await self._render_admin_details(query, actor, user_id, page)
        else:
            await query.answer("Не удалось обновить роль", show_alert=True)
        logger.info(
            "Админ %s повысил пользователя id=%s до супер-админа (успех=%s)",
            describe_user(actor),
            user_id,
            success,
            extra={"action": "promote_superadmin", "result": "success" if success else "error", "target_user_id": user_id},
        )

    @log_async_exceptions
    async def demote_to_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)
        page = self._parse_page_from_data(query.data)
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
                extra={"action": "demote_to_admin", "result": "permission_denied", "target_user_id": user_id},
            )
            return

        success = await self.admin_repo.demote_user(user_id, "admin", actor.id)

        if success:
            await self._render_admin_details(query, actor, user_id, page)
        else:
            await query.answer("Не удалось изменить роль", show_alert=True)
        logger.info(
            "Админ %s понизил пользователя id=%s до admin (успех=%s)",
            describe_user(actor),
            user_id,
            success,
            extra={"action": "demote_to_admin", "result": "success" if success else "error", "target_user_id": user_id},
        )

    @log_async_exceptions
    async def demote_to_operator(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        actor = update.effective_user
        user_id = self._extract_user_id(query.data)
        page = self._parse_page_from_data(query.data)
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
                extra={"action": "demote_to_operator", "result": "permission_denied", "target_user_id": user_id},
            )
            return

        success = await self.admin_repo.demote_user(user_id, "operator", actor.id)

        if success:
            await self._render_admin_details(query, actor, user_id, page)
        else:
            await query.answer("Не удалось изменить роль", show_alert=True)
        logger.info(
            "Админ %s понизил пользователя id=%s до operator (успех=%s)",
            describe_user(actor),
            user_id,
            success,
            extra={"action": "demote_to_operator", "result": "success" if success else "error", "target_user_id": user_id},
        )

    async def _refresh_details(self, update: Update, query, user_id: int):
        page = self._parse_page_from_data(query.data)
        await self._render_admin_details(query, update.effective_user, user_id, page)

    async def _render_admin_details(
        self,
        query,
        actor,
        user_id: int,
        page: int = 0,
    ):
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await safe_edit_message(query, text="❌ Пользователь не найден")
            return

        role_name = user.get("role") or role_name_from_id(user.get("role_id"))
        message = (
            f"👤 <b>{user.get('full_name', 'Без имени')}</b>\n"
            f"Username: @{user.get('username', 'нет')}\n"
            f"Telegram ID: {user.get('telegram_id')}\n"
            f"Роль: <b>{role_name}</b>\n"
            f"Статус: <b>{user.get('status')}</b>\n"
        )

        keyboard = await self._build_admin_actions(actor, user, page)
        keyboard.append(
            [
                InlineKeyboardButton(
                    "🔄 Обновить",
                    callback_data=self._build_details_callback(user_id, page),
                )
            ]
        )
        keyboard.append(
            [InlineKeyboardButton("◀️ К списку", callback_data=self._build_list_callback(page))]
        )
        keyboard.append([InlineKeyboardButton("🏠 В панель", callback_data="admin:back")])

        await safe_edit_message(
            query,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    async def _build_admin_actions(
        self,
        actor,
        target: Dict[str, Optional[str]],
        page: int,
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
                            callback_data=f"admin:admins:promote_super:{page}:{target['id']}",
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
                            callback_data=f"admin:admins:demote_operator:{page}:{target['id']}",
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
                            callback_data=f"admin:admins:demote_admin:{page}:{target['id']}",
                        )
                    ]
                )

        elif role_name == "operator":
            if await self.permissions.can_promote(
                actor.id, "admin", actor.username
            ):
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            "⬆️ Назначить админом",
                            callback_data=f"admin:admins:promote_admin:{target['id']}",
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
            "SELECT user_id AS telegram_id FROM UsersTelegaBot WHERE id = %s",
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
