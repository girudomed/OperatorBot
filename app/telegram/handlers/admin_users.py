# Файл: app/telegram/handlers/admin_users.py

"""
Хендлеры для управления пользователями (approve/decline/block).
"""

from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ContextTypes,
)

from app.telegram.utils.callback_data import AdminCB

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.services.notifications import NotificationService
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.core.roles import role_name_from_id
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message
from app.utils.action_guard import ActionGuard
from app.utils.rate_limit import rate_limit_hit
from app.telegram.utils.admin_registry import register_admin_callback_handler

logger = get_watchdog_logger(__name__)


class AdminUsersHandler:
    """Управление пользователями."""
    
    def __init__(
        self,
        admin_repo: AdminRepository,
        permissions: PermissionsManager,
        notifications: NotificationService
    ):
        self.admin_repo = admin_repo
        self.permissions = permissions
        self.notifications = notifications
        self.default_filter = "pending"
        self.page_size = 10
        self.write_cooldown_seconds = 5.0
        self.read_cooldown_seconds = 1.5

    def _parse_status_page(self, data: str) -> tuple[str, int]:
        # Try new format
        action, args = AdminCB.parse(data)
        if action == AdminCB.USERS and args:
            # args: [sub_action, status, page, ...]
            # sub_action is LIST or DETAILS etc.
            if len(args) > 1:
                status = self._normalize_status_arg(args[1])
                page = int(args[2]) if len(args) > 2 and args[2].isdigit() else 0
                return status, page
                
        # Fallback to legacy
        parts = data.split(':')
        status = self._normalize_status_arg(parts[3] if len(parts) > 3 else self.default_filter)
        page = 0
        if len(parts) > 4:
            try:
                page = max(0, int(parts[4]))
            except ValueError:
                page = 0
        return status, page

    def _normalize_status_arg(self, raw: Optional[str]) -> Optional[str]:
        mapping = {
            "p": "pending",
            "pending": "pending",
            "a": "approved",
            "approved": "approved",
            "b": "blocked",
            "blocked": "blocked",
        }
        slug = (raw or "").strip().lower()
        return mapping.get(slug, slug or "pending")

    def _extract_user_id(self, data: str) -> int:
        # Try new format
        action, args = AdminCB.parse(data)
        if action == AdminCB.USERS and args:
            # Format: adm:usr:type:status:page:id
            # id is usually last
            try:
                return int(args[-1])
            except (ValueError, IndexError):
                pass
                
        # Fallback
        try:
            return int(data.split(':')[-1])
        except (ValueError, IndexError) as exc:
            logger.warning(
                "Не удалось извлечь user_id из callback '%s': %s",
                data,
                exc,
                exc_info=True,
            )
            return 0

    @log_async_exceptions
    async def handle_admin_command_action(
        self,
        action: Optional[str],
        payload: Optional[str],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        """
        Точка делегации для действий типа adm:cmd:<action>:<payload>.
        Если AdminPanel не распознал команду, он вызывает этот метод у зарегистрированного
        admin_commands_handler (через bot_data). Здесь можно реализовать старые admincmd
        сценарии или дополнительные быстрые действия.

        Возвращает True если действие обработано, иначе False.
        """
        # Пока специальных делегаций не требуется — возвращаем False, чтобы admin_panel
        # показал стандартное "Команда в разработке".
        return False

    def _build_list_callback(self, status: str, page: int = 0) -> str:
        return AdminCB.create(AdminCB.USERS, AdminCB.LIST, status, page)

    @log_async_exceptions
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if not query:
            return
        actor = update.effective_user
        if not actor:
            return
        if not await self.permissions.can_manage_users(actor.id, actor.username):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        action, args = AdminCB.parse(query.data or "")
        if action != AdminCB.USERS:
            return
        sub_action = args[0] if args else AdminCB.LIST
        if sub_action == AdminCB.LIST:
            await self.show_users_list(update, context)
        elif sub_action == AdminCB.DETAILS:
            await self.show_user_details(update, context)
        elif sub_action == AdminCB.APPROVE:
            await self.handle_approve(update, context)
        elif sub_action == AdminCB.DECLINE:
            await self.handle_decline(update, context)
        elif sub_action == AdminCB.BLOCK:
            await self.handle_block(update, context)
        elif sub_action == AdminCB.UNBLOCK:
            await self.handle_unblock(update, context)
    
    @log_async_exceptions
    async def show_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает список пользователей."""
        query = update.callback_query
        if not query:
            return
        await query.answer()
        if await self._rate_limit(
            query,
            context,
            "admin_users_list",
            self.read_cooldown_seconds,
            "Слишком часто обновляете список. Подождите немного.",
        ):
            return
        
        status_filter, page = self._parse_status_page(query.data)
        status_label = self._status_label(status_filter)
        logger.info(
            "Админ %s открыл список пользователей (%s)",
            describe_user(update.effective_user),
            status_filter,
            extra={"action": "list_users", "result": "success", "status": status_filter},
        )
        
        page = max(0, page)
        limit = self.page_size
        offset = page * limit
        page_slice, total = await self.admin_repo.get_users_page(status_filter, limit, offset)
        max_page = max(0, (total - 1) // limit) if total else 0
        if total and page > max_page:
            page = max_page
            offset = page * limit
            page_slice, total = await self.admin_repo.get_users_page(status_filter, limit, offset)
        
        keyboard: list[list[InlineKeyboardButton]] = []
        if total == 0:
            message = f"📋 Нет пользователей со статусом: {status_label}"
            keyboard.append([
                InlineKeyboardButton(
                    "🔄 Обновить",
                    callback_data=self._build_list_callback(status_filter, max(page, 0)),
                )
            ])
            logger.info(
                "Админ %s открыл пустой список пользователей (%s)",
                describe_user(update.effective_user),
                status_filter,
                extra={"action": "list_users", "result": "empty", "status": status_filter},
            )
        else:
            start = page * limit
            end = start + len(page_slice)
            message = (
                f"👥 <b>Пользователи ({status_label})</b>\n"
                f"Показано {start + 1}-{min(end, total)} из {total}\n"
            )
            for user in page_slice:
                user_text = f"{user.get('full_name', 'Нет имени')} (@{user.get('username', 'нет')})"
                user_id = user.get('id')
                keyboard.append([
                    InlineKeyboardButton(
                        user_text,
                        callback_data=AdminCB.create(
                            AdminCB.USERS,
                            AdminCB.DETAILS,
                            status_filter,
                            page,
                            user_id,
                        ),
                    )
                ])
            nav_row = []
            if page > 0:
                nav_row.append(
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data=self._build_list_callback(status_filter, page - 1),
                    )
                )
            if page < max_page:
                nav_row.append(
                    InlineKeyboardButton(
                        "➡️ Далее",
                        callback_data=self._build_list_callback(status_filter, page + 1),
                    )
                )
            if nav_row:
                keyboard.append(nav_row)
            logger.info(
                "Админ %s просматривает %s пользователей (%s показано)",
                describe_user(update.effective_user),
                status_filter,
                len(page_slice),
                extra={
                    "action": "list_users",
                    "result": "success",
                    "status": status_filter,
                    "displayed": len(page_slice),
                },
            )

        if status_filter == 'pending':
            filter_buttons = [
                InlineKeyboardButton("✅ Одобрены", callback_data=self._build_list_callback('approved')),
                InlineKeyboardButton("🚫 Заблокированы", callback_data=self._build_list_callback('blocked')),
            ]
            keyboard.append(filter_buttons)
            keyboard.append(
                [
                    InlineKeyboardButton("⏳ Заявки", callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.LIST, 0)),
                ]
            )
            keyboard.append(
                [
                    InlineKeyboardButton(
                        "👑 Список админов",
                        callback_data=AdminCB.create(AdminCB.ADMINS, AdminCB.LIST, 0),
                    ),
                ]
            )
        elif status_filter == 'approved':
            keyboard.append(
                [
                    InlineKeyboardButton(
                        "⬅️ К пользователям",
                        callback_data=self._build_list_callback(self.default_filter, 0),
                    )
                ]
            )

        # Всегда пытаемся обновить. safe_edit_message сам решит: редактировать или прислать новое,
        # если редактирование невозможно (например, текст совпал, но мы хотим обновить клавиатуру).
        # Однако ПРАВИИМ_ДАННЫЕ рекомендует: если экран новый - присылай новое.
        # Вход в список из меню - это новый экран.
        
        await safe_edit_message(
            query,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML',
        )
    
    @log_async_exceptions
    async def show_user_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает детали пользователя с кнопками действий."""
        query = update.callback_query
        if not query:
            return
        await query.answer()
        
        status_filter, page = self._parse_status_page(query.data)
        user_id = self._extract_user_id(query.data)
        if not user_id:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        if await self._rate_limit(
            query,
            context,
            f"admin_user_details:{user_id}",
            self.read_cooldown_seconds,
            "Слишком часто открываете карточку. Подождите немного.",
        ):
            return

        logger.info(
            "Админ %s открыл карточку пользователя #%s (filter=%s)",
            describe_user(update.effective_user),
            user_id,
            status_filter,
            extra={
                "action": "open_user_card",
                "result": "success",
                "target_user_id": user_id,
                "status": status_filter,
            },
        )
        await self._render_user_details(query, update.effective_user, user_id, status_filter, page)

    async def _render_user_details(
        self,
        query,
        actor,
        user_id: int,
        status_filter: str,
        page: int,
    ):
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await safe_edit_message(query, text="❌ Пользователь не найден")
            return
        
        role_info = user.get('role') or {}
        role_name = None
        if isinstance(role_info, dict):
            role_name = role_info.get('name') or role_info.get('slug')
        if not role_name:
            role_name = role_name_from_id(user.get('role_id'))
        username = user.get('username')
        username_line = f"@{username}" if username else "—"
        extension = user.get('extension') or "—"
        created_at = user.get('created_at')
        if hasattr(created_at, "strftime"):
            created_str = created_at.strftime("%d.%m.%Y %H:%M")
        else:
            created_str = created_at or "—"
        
        message = (
            f"👤 <b>Пользователь #{user_id}</b>\n\n"
            f"Имя: {user.get('full_name', 'Не указано')}\n"
            f"Username: {username_line}\n"
            f"Extension: {extension}\n"
            f"Роль: <b>{role_name}</b>\n"
            f"Статус: <b>{user.get('status', 'pending')}</b>\n"
            f"Регистрация: {created_str}\n"
        )
        
        keyboard = []
        
        if user.get('status') == 'pending':
            keyboard.append([
                InlineKeyboardButton(
                    "✅ Одобрить",
                    callback_data=AdminCB.create(
                        AdminCB.USERS,
                        AdminCB.APPROVE,
                        status_filter,
                        page,
                        user_id,
                    ),
                ),
                InlineKeyboardButton(
                    "❌ Отклонить",
                    callback_data=AdminCB.create(
                        AdminCB.USERS,
                        AdminCB.DECLINE,
                        status_filter,
                        page,
                        user_id,
                    ),
                ),
            ])
        elif user.get('status') == 'approved':
            keyboard.append([
                InlineKeyboardButton(
                    "🚫 Заблокировать",
                    callback_data=AdminCB.create(
                        AdminCB.USERS,
                        AdminCB.BLOCK,
                        status_filter,
                        page,
                        user_id,
                    ),
                )
            ])
        elif user.get('status') == 'blocked':
            keyboard.append([
                InlineKeyboardButton(
                    "🔓 Разблокировать",
                    callback_data=AdminCB.create(
                        AdminCB.USERS,
                        AdminCB.UNBLOCK,
                        status_filter,
                        page,
                        user_id,
                    ),
                )
            ])

        keyboard.append([
            InlineKeyboardButton(
                "🔄 Обновить",
                callback_data=AdminCB.create(AdminCB.USERS, AdminCB.DETAILS, status_filter, page, user_id)
            )
        ])
        
        keyboard.append([
            InlineKeyboardButton(
                "◀️ К списку",
                callback_data=self._build_list_callback(status_filter, page)
            )
        ])
        keyboard.append([
            InlineKeyboardButton("🏠 В панель", callback_data=AdminCB.create(AdminCB.BACK))
        ])
        
        await safe_edit_message(
            query,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML',
        )
    
    @log_async_exceptions
    async def handle_approve(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Утверждает пользователя."""
        query = update.callback_query
        await query.answer()
        
        status_filter, page = self._parse_status_page(query.data)
        user_id = self._extract_user_id(query.data)
        actor_id = update.effective_user.id
        guard = self._get_action_guard(context)
        guard_key = f"approve:{user_id}"
        guard_acquired = False
        if guard:
            guard_acquired = await guard.acquire(guard_key, cooldown_seconds=self.write_cooldown_seconds)
            if not guard_acquired:
                await query.answer("Операция уже выполняется. Подождите несколько секунд.", show_alert=True)
                return
        
        # Проверяем права
        can_approve = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_approve:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка approve без прав: %s -> target_id=%s",
                describe_user(update.effective_user),
                user_id,
                extra={"action": "approve_user", "result": "permission_denied", "target_user_id": user_id},
            )
            if guard and guard_acquired:
                guard.release(guard_key, success=False)
            return
        
        # Утверждаем
        success = False
        try:
            success = await self.admin_repo.approve_user(user_id, actor_id)
            if success:
                # Получаем данные пользователя для уведомления
                user = await self.admin_repo.db.execute_with_retry(
                    "SELECT user_id AS telegram_id, username FROM UsersTelegaBot WHERE id = %s",
                    params=(user_id,), fetchone=True
                )
                
                if user and hasattr(self.notifications, "notify_approval"):
                    await self.notifications.notify_approval(
                        user['telegram_id'],
                        update.effective_user.full_name
                    )
                
                await safe_edit_message(
                    query,
                    text="✅ Пользователь успешно одобрен. Теперь он может пользоваться ботом.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "◀️ К списку",
                            callback_data=self._build_list_callback(status_filter, page)
                        )
                    ]]),
                )
            else:
                await query.answer("❌ Ошибка при утверждении", show_alert=True)
        finally:
            if guard and guard_acquired:
                guard.release(guard_key, success=success)
        logger.info(
            "Админ %s утвердил пользователя id=%s (успех=%s)",
            describe_user(update.effective_user),
            user_id,
            success,
            extra={"action": "approve_user", "result": "success" if success else "error", "target_user_id": user_id},
        )
    
    @log_async_exceptions
    async def handle_decline(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отклоняет заявку."""
        query = update.callback_query
        await query.answer()
        
        status_filter, page = self._parse_status_page(query.data)
        user_id = self._extract_user_id(query.data)
        actor_id = update.effective_user.id
        guard = self._get_action_guard(context)
        guard_key = f"decline:{user_id}"
        guard_acquired = False
        if guard:
            guard_acquired = await guard.acquire(guard_key, cooldown_seconds=self.write_cooldown_seconds)
            if not guard_acquired:
                await query.answer("Операция уже выполняется. Подождите несколько секунд.", show_alert=True)
                return
        
        can_approve = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_approve:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка decline без прав: %s -> target_id=%s",
                describe_user(update.effective_user),
                user_id,
                extra={"action": "decline_user", "result": "permission_denied", "target_user_id": user_id},
            )
            if guard and guard_acquired:
                guard.release(guard_key, success=False)
            return
        
        success = False
        try:
            success = await self.admin_repo.decline_user(user_id, actor_id)
        
            if success:
                await safe_edit_message(
                    query,
                    text=f"❌ Заявка #{user_id} отклонена",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "◀️ К списку",
                            callback_data=self._build_list_callback(status_filter, page)
                        )
                    ]]),
                )
            else:
                await query.answer("❌ Ошибка", show_alert=True)
        finally:
            if guard and guard_acquired:
                guard.release(guard_key, success=success)
        logger.info(
            "Админ %s отклонил заявку пользователя id=%s (успех=%s)",
            describe_user(update.effective_user),
            user_id,
            success,
            extra={"action": "decline_user", "result": "success" if success else "error", "target_user_id": user_id},
        )
    
    @log_async_exceptions
    async def handle_block(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Блокирует пользователя."""
        query = update.callback_query
        status_filter, page = self._parse_status_page(query.data)
        user_id = self._extract_user_id(query.data)
        actor_id = update.effective_user.id
        guard = self._get_action_guard(context)
        guard_key = f"block:{user_id}"
        guard_acquired = False
        if guard:
            guard_acquired = await guard.acquire(guard_key, cooldown_seconds=self.write_cooldown_seconds)
            if not guard_acquired:
                await query.answer("Операция уже выполняется. Подождите несколько секунд.", show_alert=True)
                return
        
        can_exclude = await self.permissions.can_exclude_user(actor_id, update.effective_user.username)
        if not can_exclude:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка блокировки без прав: %s -> target_id=%s",
                describe_user(update.effective_user),
                user_id,
                extra={"action": "block_user", "result": "permission_denied", "target_user_id": user_id},
            )
            if guard and guard_acquired:
                guard.release(guard_key, success=False)
            return
        
        success = False
        try:
            success = await self.admin_repo.block_user(user_id, actor_id)
        
            if success:
                await query.answer("🚫 Пользователь заблокирован. Он больше не сможет пользоваться ботом.", show_alert=True)
                await self._render_user_details(query, update.effective_user, user_id, status_filter, page)
            else:
                await query.answer("❌ Ошибка", show_alert=True)
        finally:
            if guard and guard_acquired:
                guard.release(guard_key, success=success)
        logger.info(
            "Админ %s заблокировал пользователя id=%s (успех=%s)",
            describe_user(update.effective_user),
            user_id,
            success,
            extra={"action": "block_user", "result": "success" if success else "error", "target_user_id": user_id},
        )
    
    @log_async_exceptions
    async def handle_unblock(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Разблокирует пользователя."""
        query = update.callback_query
        status_filter, page = self._parse_status_page(query.data)
        user_id = self._extract_user_id(query.data)
        actor_id = update.effective_user.id
        guard = self._get_action_guard(context)
        guard_key = f"unblock:{user_id}"
        guard_acquired = False
        if guard:
            guard_acquired = await guard.acquire(guard_key, cooldown_seconds=self.write_cooldown_seconds)
            if not guard_acquired:
                await query.answer("Операция уже выполняется. Подождите несколько секунд.", show_alert=True)
                return
        
        can_exclude = await self.permissions.can_exclude_user(actor_id, update.effective_user.username)
        if not can_exclude:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка разблокировки без прав: %s -> target_id=%s",
                describe_user(update.effective_user),
                user_id,
                extra={"action": "unblock_user", "result": "permission_denied", "target_user_id": user_id},
            )
            if guard and guard_acquired:
                guard.release(guard_key, success=False)
            return
        
        success = False
        try:
            success = await self.admin_repo.unblock_user(user_id, actor_id)
        
            if success:
                await query.answer("✅ Пользователь разблокирован и снова активен.", show_alert=True)
                await self._render_user_details(query, update.effective_user, user_id, status_filter, page)
            else:
                await query.answer("❌ Ошибка", show_alert=True)
        finally:
            if guard and guard_acquired:
                guard.release(guard_key, success=success)
        logger.info(
            "Админ %s разблокировал пользователя id=%s (успех=%s)",
            describe_user(update.effective_user),
            user_id,
            success,
            extra={"action": "unblock_user", "result": "success" if success else "error", "target_user_id": user_id},
        )
    
    def _status_label(self, slug: str) -> str:
        mapping = {
            "pending": "ожидают",
            "approved": "одобрены",
            "blocked": "заблокированы",
        }
        return mapping.get((slug or "").lower(), slug or "—")
    
    async def _rate_limit(
        self,
        query,
        context: ContextTypes.DEFAULT_TYPE,
        key: str,
        cooldown: float,
        alert_text: str,
    ) -> bool:
        user = query.from_user if query else None
        if not user:
            return False
        if rate_limit_hit(
            context.application.bot_data,
            user.id,
            key,
            cooldown_seconds=cooldown,
        ):
            await query.answer(alert_text, show_alert=True)
            return True
        return False
    
    def _get_action_guard(self, context: ContextTypes.DEFAULT_TYPE) -> Optional[ActionGuard]:
        guard = context.application.bot_data.get("action_guard")
        return guard if isinstance(guard, ActionGuard) else None


def register_admin_users_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
    notifications: NotificationService
):
    """Регистрирует хендлеры управления пользователями."""
    handler = AdminUsersHandler(admin_repo, permissions, notifications)
    
    # Сохраняем для доступа через роутер
    application.bot_data["admin_users_handler"] = handler
    
    register_admin_callback_handler(application, AdminCB.USERS, handler.handle_callback)

    
    logger.info("Admin users handlers registered")
