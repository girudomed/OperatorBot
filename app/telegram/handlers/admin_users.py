"""
Хендлеры для управления пользователями (approve/decline/block).
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, ContextTypes, Application

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.services.notifications import NotificationService
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.core.roles import role_name_from_id
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message

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

    def _parse_status_page(self, data: str) -> tuple[str, int]:
        parts = data.split(':')
        status = parts[3] if len(parts) > 3 else self.default_filter
        page = 0
        if len(parts) > 4:
            try:
                page = max(0, int(parts[4]))
            except ValueError as exc:
                logger.warning(
                    "Некорректный номер страницы в callback '%s': %s",
                    data,
                    exc,
                    exc_info=True,
                )
                page = 0
        return status, page

    def _extract_user_id(self, data: str) -> int:
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

    def _build_list_callback(self, status: str, page: int = 0) -> str:
        return f"admin:users:list:{status}:{page}"
    
    @log_async_exceptions
    async def show_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает список пользователей."""
        query = update.callback_query
        await query.answer()
        
        status_filter, page = self._parse_status_page(query.data)
        logger.info(
            "Админ %s открыл список пользователей (%s)",
            describe_user(update.effective_user),
            status_filter,
            extra={"action": "list_users", "result": "success", "status": status_filter},
        )
        
        users = await self.admin_repo.get_all_users(status_filter)
        total = len(users)
        max_page = max(0, (total - 1) // self.page_size) if total else 0
        page = min(page, max_page)
        start = page * self.page_size
        end = start + self.page_size
        page_slice = users[start:end]
        
        if not users:
            message = f"📋 Нет пользователей со статусом: {status_filter}"
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin:back")]]
            logger.info(
                "Админ %s открыл пустой список пользователей (%s)",
                describe_user(update.effective_user),
                status_filter,
                extra={"action": "list_users", "result": "empty", "status": status_filter},
            )
        else:
            message = (
                f"👥 <b>Пользователи ({status_filter})</b>\n"
                f"Показано {start + 1}-{min(end, total)} из {total}\n"
            )
            
            keyboard = []
            for user in page_slice:
                user_text = f"{user.get('full_name', 'Нет имени')} (@{user.get('username', 'нет')})"
                user_id = user.get('id')
                
                keyboard.append([
                    InlineKeyboardButton(
                        user_text,
                        callback_data=f"admin:users:details:{status_filter}:{page}:{user_id}"
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
            
            # Фильтры
            filters = [
                InlineKeyboardButton("⏳ Pending", callback_data=self._build_list_callback('pending')),
                InlineKeyboardButton("✅ Approved", callback_data=self._build_list_callback('approved')),
                InlineKeyboardButton("🚫 Blocked", callback_data=self._build_list_callback('blocked'))
            ]
            keyboard.append(filters)
            keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin:back")])
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
        await query.answer()
        
        status_filter, page = self._parse_status_page(query.data)
        user_id = self._extract_user_id(query.data)
        if not user_id:
            await query.answer("❌ Пользователь не найден", show_alert=True)
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
        
        role_name = user.get('role') or role_name_from_id(user.get('role_id'))
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
        base_callback_suffix = f"{status_filter}:{page}:{user_id}"
        
        if user.get('status') == 'pending':
            keyboard.append([
                InlineKeyboardButton("✅ Approve", callback_data=f"admin:users:approve:{base_callback_suffix}"),
                InlineKeyboardButton("❌ Decline", callback_data=f"admin:users:decline:{base_callback_suffix}")
            ])
        elif user.get('status') == 'approved':
            keyboard.append([
                InlineKeyboardButton("🚫 Block", callback_data=f"admin:users:block:{base_callback_suffix}")
            ])
        elif user.get('status') == 'blocked':
            keyboard.append([
                InlineKeyboardButton("🔓 Unblock", callback_data=f"admin:users:unblock:{base_callback_suffix}")
            ])

        can_promote_admin = False
        if actor and user.get('status') == 'approved' and role_name == 'operator':
            can_promote_admin = await self.permissions.can_promote(
                actor.id,
                'admin',
                actor.username,
            )
        if can_promote_admin:
            keyboard.append([
                InlineKeyboardButton(
                    "⬆️ Назначить админом",
                    callback_data=f"admin:admins:promote_admin:{user_id}"
                )
            ])

        keyboard.append([
            InlineKeyboardButton(
                "🔄 Обновить",
                callback_data=f"admin:users:details:{status_filter}:{page}:{user_id}"
            )
        ])
        
        keyboard.append([
            InlineKeyboardButton(
                "◀️ К списку",
                callback_data=self._build_list_callback(status_filter, page)
            )
        ])
        keyboard.append([
            InlineKeyboardButton("🏠 В панель", callback_data="admin:back")
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
            return
        
        # Утверждаем
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
        
        can_approve = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_approve:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка decline без прав: %s -> target_id=%s",
                describe_user(update.effective_user),
                user_id,
                extra={"action": "decline_user", "result": "permission_denied", "target_user_id": user_id},
            )
            return
        
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
        
        can_manage = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_manage:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка блокировки без прав: %s -> target_id=%s",
                describe_user(update.effective_user),
                user_id,
                extra={"action": "block_user", "result": "permission_denied", "target_user_id": user_id},
            )
            return
        
        success = await self.admin_repo.block_user(user_id, actor_id)
        
        if success:
            await query.answer("🚫 Пользователь заблокирован. Он больше не сможет пользоваться ботом.", show_alert=True)
            await self._render_user_details(query, update.effective_user, user_id, status_filter, page)
        else:
            await query.answer("❌ Ошибка", show_alert=True)
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
        
        can_manage = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_manage:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            logger.warning(
                "Попытка разблокировки без прав: %s -> target_id=%s",
                describe_user(update.effective_user),
                user_id,
                extra={"action": "unblock_user", "result": "permission_denied", "target_user_id": user_id},
            )
            return
        
        success = await self.admin_repo.unblock_user(user_id, actor_id)
        
        if success:
            await query.answer("✅ Пользователь разблокирован и снова активен.", show_alert=True)
            await self._render_user_details(query, update.effective_user, user_id, status_filter, page)
        else:
            await query.answer("❌ Ошибка", show_alert=True)
        logger.info(
            "Админ %s разблокировал пользователя id=%s (успех=%s)",
            describe_user(update.effective_user),
            user_id,
            success,
            extra={"action": "unblock_user", "result": "success" if success else "error", "target_user_id": user_id},
        )


def register_admin_users_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
    notifications: NotificationService
):
    """Регистрирует хендлеры управления пользователями."""
    handler = AdminUsersHandler(admin_repo, permissions, notifications)
    
    # Список пользователей
    application.add_handler(
        CallbackQueryHandler(handler.show_users_list, pattern=r"^admin:users:list")
    )
    
    # Детали пользователя
    application.add_handler(
        CallbackQueryHandler(handler.show_user_details, pattern=r"^admin:users:details:")
    )
    
    # Действия
    application.add_handler(
        CallbackQueryHandler(handler.handle_approve, pattern=r"^admin:users:approve:")
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_decline, pattern=r"^admin:users:decline:")
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_block, pattern=r"^admin:users:block:")
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_unblock, pattern=r"^admin:users:unblock:")
    )
    
    logger.info("Admin users handlers registered")
