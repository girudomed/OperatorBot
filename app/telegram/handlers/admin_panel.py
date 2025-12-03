"""
Главный обработчик админ-панели.

Предоставляет точку входа /admin и основное меню.
"""

import re
from typing import Optional, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup

from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    Application,
    MessageHandler,
    filters,
)

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.telegram.utils.buttons import ADMIN_PANEL_BUTTON, CALL_LOOKUP_BUTTON
from app.telegram.utils.logging import describe_user
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)
ADMIN_PREFIX = "admin"


class AdminPanelHandler:
    """Основной хендлер админ-панели."""
    
    def __init__(
        self,
        admin_repo: AdminRepository,
        permissions: PermissionsManager
    ):
        self.admin_repo = admin_repo
        self.permissions = permissions
    
    @log_async_exceptions
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /admin - вход в админ-панель."""
        user = update.effective_user
        
        # Проверка прав доступа
        has_access = await self.permissions.can_access_admin_panel(
            user.id, user.username
        )
        
        if not has_access:
            logger.warning(
                "Denied admin panel access for %s",
                describe_user(user),
            )
            await update.message.reply_text(
                "❌ У вас нет доступа к админ-панели.\n"
                "Требуется роль администратора."
            )
            return
        
        logger.info("Админ-панель открыта пользователем %s", describe_user(user))
        # Показываем главное меню
        await self._show_main_menu(update, context)
    
    async def _show_main_menu(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        message_text: Optional[str] = None,
    ):
        """Отображает главное меню админ-панели."""
        user = update.effective_user
        keyboard = [
            [
                InlineKeyboardButton(
                    "📊 LIVE Dashboard", callback_data=self._callback("dashboard")
                ),
                InlineKeyboardButton(
                    "👥 Операторы",
                    callback_data=self._callback("users", "list", "pending"),
                ),
            ],
            [
                InlineKeyboardButton(
                    "👑 Администраторы",
                    callback_data=self._callback("admins", "list"),
                ),
                InlineKeyboardButton(
                    "📈 Статистика", callback_data=self._callback("stats")
                ),
            ],
            [
                InlineKeyboardButton(
                    "📂 Расшифровки", callback_data=self._callback("lookup")
                ),
                InlineKeyboardButton(
                    "⚙️ Настройки", callback_data=self._callback("settings")
                ),
            ],
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = message_text or (
            "👑 <b>Админ-панель</b>\n\n"
            "Выберите раздел для управления:"
        )
        
        # Если это callback, редактируем сообщение
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        logger.debug(
            "Показано главное меню админ-панели для %s",
            describe_user(user),
        )
    
    @log_async_exceptions
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Роутер для callback-запросов админ-панели."""
        query = update.callback_query
        await query.answer()
        
        section, action, payload = self._parse_callback(query.data)

        user = update.effective_user
        logger.info(
            "Admin callback: section=%s action=%s payload=%s user=%s",
            section,
            action,
            payload,
            describe_user(user),
        )

        if section in ("back", "menu"):
            await self._show_main_menu(update, context)
            return

        if section == "dashboard":
            await self._show_dashboard(update, context)
            return

        if section == "settings":
            await query.edit_message_text(
                "⚙️ Настройки в разработке",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀️ Назад", callback_data=self._callback("back"))]]
                ),
            )
            return


    @log_async_exceptions
    async def handle_admin_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает кнопку reply-клавиатуры "👑 Админ-панель"."""
        logger.info(
            "Кнопка админ-панели нажата пользователем %s",
            describe_user(update.effective_user),
        )
        await self.admin_command(update, context)

    @log_async_exceptions
    async def handle_lookup_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает кнопку "📂 Расшифровки" и подсказывает синтаксис."""
        message = (
            "🔎 Чтобы получить расшифровку, используйте команду \n"
            "<code>/call_lookup &lt;номер&gt; [период]</code>\n"
            "Пример: <code>/call_lookup +7 999 123 45 67 weekly</code>"
        )
        logger.info(
            "Пользователь %s запросил подсказку по расшифровкам",
            describe_user(update.effective_user),
        )
        await update.message.reply_text(message, parse_mode='HTML')

    async def _show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает dashboard с основными метриками."""
        query = update.callback_query
        
        # Получаем статистику
        pending_count = len(await self.admin_repo.get_pending_users())
        all_admins = await self.admin_repo.get_admins()

        logger.info(
            "Dashboard открыт пользователем %s (pending=%s admins=%s)",
            describe_user(update.effective_user),
            pending_count,
            len(all_admins),
        )
        
        message = (
            f"📊 <b>Dashboard</b>\n\n"
            f"👥 Ожидают утверждения: <b>{pending_count}</b>\n"
            f"👑 Администраторов: <b>{len(all_admins)}</b>\n\n"
            f"Последние действия:\n"
            f"<i>Скоро будет доступно</i>"
        )
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=self._callback("back"))]]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

    def _callback(
        self,
        section: str,
        action: Optional[str] = None,
        payload: Optional[str] = None,
    ) -> str:
        parts = [ADMIN_PREFIX, section]
        if action:
            parts.append(action)
        if payload:
            parts.append(str(payload))
        return ":".join(parts)

    def _parse_callback(self, data: str) -> Tuple[str, Optional[str], Optional[str]]:
        if not data.startswith(f"{ADMIN_PREFIX}:"):
            return data, None, None
        parts = data.split(":")
        section = parts[1] if len(parts) > 1 else None
        action = parts[2] if len(parts) > 2 else None
        payload = parts[3] if len(parts) > 3 else None
        return section or "", action, payload


def register_admin_panel_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager
):
    """Регистрирует хендлеры админ-панели."""
    handler = AdminPanelHandler(admin_repo, permissions)
    
    # Команда /admin
    application.add_handler(CommandHandler("admin", handler.admin_command))
    
    # Callback handlers
    application.add_handler(
        CallbackQueryHandler(handler.handle_callback, pattern=r"^admin:(dashboard|settings|back|menu)$")
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex(f"^{re.escape(ADMIN_PANEL_BUTTON)}$"),
            handler.handle_admin_button
        )
    )
    application.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex(f"^{re.escape(CALL_LOOKUP_BUTTON)}$"),
            handler.handle_lookup_button
        )
    )
    
    logger.info("Admin panel handlers registered")
