# Файл: app/telegram/handlers/admin_panel.py

"""
Главный обработчик админ-панели.

Предоставляет точку входа /admin и основное меню.
"""

from typing import Optional, Tuple, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TelegramError

from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    Application,
)

try:  # filters/MessageHandler появились не во всех версиях PTB
    from telegram.ext import MessageHandler, filters
except ImportError:  # pragma: no cover - fallback для старых версий
    MessageHandler = None
    filters = None

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message
from app.utils.error_handlers import log_async_exceptions
from app.core.roles import role_display_name_from_name, role_name_from_id

logger = get_watchdog_logger(__name__)
ADMIN_PREFIX = "admin"
ROLE_DISPLAY_ORDER = [
    "founder",
    "developer",
    "superadmin",
    "head_of_registry",
    "admin",
    "marketing_director",
    "operator",
]
ROLE_EMOJI = {
    "founder": "🛡️",
    "developer": "👨‍💻",
    "superadmin": "⭐",
    "head_of_registry": "📋",
    "admin": "👑",
    "marketing_director": "📣",
    "operator": "👷",
}


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
        counters = None
        role_slug: Optional[str] = None
        allow_commands = False
        try:
            counters = await self.admin_repo.get_users_counters()
        except Exception as exc:
            logger.error("Не удалось получить счётчики пользователей: %s", exc)
        try:
            role_slug = await self.permissions.get_effective_role(user.id, user.username)
            allow_commands = role_slug not in {"operator", "admin"}
        except Exception:
            logger.warning("Не удалось определить роль пользователя %s", describe_user(user))

        roles_summary = self._build_roles_summary(counters)
        keyboard = [
            [
                InlineKeyboardButton(
                    "📊 Dashboard", callback_data=self._callback("dashboard")
                )
            ],
            [
                InlineKeyboardButton(
                    "👥 Операторы",
                    callback_data=self._callback("users", "list", "pending"),
                )
            ],
            [
                InlineKeyboardButton(
                    "👑 Админы",
                    callback_data=self._callback("admins", "list"),
                )
            ],
            [
                InlineKeyboardButton(
                    "📈 Статистика", callback_data=self._callback("stats")
                )
            ],
            [
                InlineKeyboardButton(
                    "📂 Расшифровки", callback_data=self._callback("lookup")
                )
            ],
            [
                InlineKeyboardButton(
                    "⚙️ Настройки", callback_data=self._callback("settings")
                )
            ],
        ]
        if allow_commands:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        "📑 Команды", callback_data=self._callback("commands")
                    )
                ]
            )
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if not message_text:
            if counters:
                message_text = (
                    "👑 <b>Админ-панель</b>\n"
                    "Следите за ключевыми метриками и управляйте командой.\n\n"
                    f"Всего пользователей: <b>{counters['total_users']}</b>\n"
                    f"⏳ Pending: <b>{counters['pending_users']}</b>\n"
                    f"✅ Approved: <b>{counters['approved_users']}</b>\n"
                    f"👑 Админов: <b>{counters['admins']}</b>\n"
                    f"👷 Операторов: <b>{counters['operators']}</b>\n\n"
                    f"<b>Роли:</b>\n{roles_summary}\n\n"
                    "Выберите раздел:"
                )
            else:
                message_text = (
                    "👑 <b>Админ-панель</b>\n\n"
                    "Выберите раздел для управления:"
                )
        
        # Если это callback, редактируем сообщение
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            except (BadRequest, TelegramError) as exc:
                logger.warning(
                    "Не удалось обновить сообщение панели (%s), отправляем новое",
                    exc,
                )
                await update.callback_query.message.reply_text(
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

        if section == "command":
            await self._handle_command_action(action, payload, update, context)
            return
        
        if section == "commands":
            await self._show_command_shortcuts(update, context)
            return

    async def _show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает dashboard с основными метриками."""
        query = update.callback_query
        
        # Получаем статистику
        try:
            counters = await self.admin_repo.get_users_counters()
        except Exception as exc:
            logger.error("Не удалось загрузить Dashboard: %s", exc)
            await safe_edit_message(
                query,
                text="⚠️ Не удалось загрузить Dashboard.\nПопробуйте снова чуть позже.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀️ Назад", callback_data=self._callback("back"))]]
                ),
            )
            return
        pending_count = counters.get('pending_users', 0)
        admin_count = counters.get('admins', 0)
        approved_count = counters.get('approved_users', 0)
        operators_count = counters.get('operators', 0)
        blocked_count = counters.get('blocked_users', 0)
        total_users = counters.get('total_users', 0)
        roles_summary = self._build_roles_summary(counters)

        logger.info(
            "Dashboard открыт пользователем %s (pending=%s admins=%s)",
            describe_user(update.effective_user),
            pending_count,
            admin_count,
        )
        
        message = (
            f"📊 <b>Dashboard</b>\n\n"
            f"👥 Всего пользователей: <b>{total_users}</b>\n"
            f"⏳ Pending: <b>{pending_count}</b>\n"
            f"✅ Approved: <b>{approved_count}</b>\n"
            f"🚫 Заблокировано: <b>{blocked_count}</b>\n"
            f"👑 Администраторов: <b>{admin_count}</b>\n"
            f"👷 Операторов: <b>{operators_count}</b>\n\n"
            f"Роли (approved):\n{roles_summary}\n\n"
            f"Последние действия:\n"
            f"<i>Скоро будет доступно</i>"
        )
        
        keyboard = [
            [
                InlineKeyboardButton(
                    "👥 Операторы", callback_data=self._callback("users", "list", "pending")
                ),
                InlineKeyboardButton(
                    "👑 Администраторы", callback_data=self._callback("admins", "list")
                ),
            ],
            [
                InlineKeyboardButton(
                    "📈 Статистика", callback_data=self._callback("stats")
                ),
                InlineKeyboardButton(
                    "📂 Расшифровки", callback_data=self._callback("lookup")
                ),
            ],
            [InlineKeyboardButton("🔄 Обновить", callback_data=self._callback("dashboard"))],
            [InlineKeyboardButton("◀️ Назад", callback_data=self._callback("back"))],
        ]
        
        await safe_edit_message(
            query,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML',
        )

    async def _show_command_shortcuts(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return
        if not await self._has_commands_access(user.id, user.username):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        text = (
            "📑 <b>Команды бота</b>\n"
            "Выберите нужное действие – команда выполнится автоматически."
        )
        keyboard = [
            [
                InlineKeyboardButton(
                    "📅 Еженедельный отчёт",
                    callback_data=self._callback("command", "weekly_quality"),
                )
            ],
            [
                InlineKeyboardButton(
                    "🧠 AI-отчёт",
                    callback_data=self._callback("command", "report"),
                )
            ],
            [
                InlineKeyboardButton(
                    "✅ Утвердить заявки",
                    callback_data="admincmd:approve:list",
                )
            ],
            [
                InlineKeyboardButton(
                    "👤 Кандидаты в админы",
                    callback_data="admincmd:promote:admin:list",
                )
            ],
            [
                InlineKeyboardButton(
                    "⭐ Кандидаты в супер-админы",
                    callback_data="admincmd:promote:superadmin:list",
                )
            ],
            [
                InlineKeyboardButton(
                    "👑 Список админов",
                    callback_data=self._callback("command", "admins"),
                )
            ],
            [
                InlineKeyboardButton(
                    "🧩 Назначить роль",
                    callback_data=self._callback("command", "set_role"),
                )
            ],
            [
                InlineKeyboardButton(
                    "⚠️ Оповестить о техработах",
                    callback_data=self._callback("command", "maintenance_alert"),
                )
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data=self._callback("back"))],
        ]
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    async def _has_commands_access(self, user_id: int, username: Optional[str]) -> bool:
        role_slug = await self.permissions.get_effective_role(user_id, username)
        return role_slug not in {"operator", "admin"}

    async def _handle_command_action(
        self,
        action: Optional[str],
        payload: Optional[str],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return
        if not await self._has_commands_access(user.id, user.username):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        if action == "weekly_quality":
            await self._run_weekly_quality(query, context)
            return
        if action == "report":
            await self._open_report_flow(update, context)
            return
        if action == "admins":
            await self._show_admins_list(query)
            return
        if action == "set_role":
            await self._show_set_role_users(query, 0)
            return
        if action == "set_role_page":
            page = int(payload or "0")
            await self._show_set_role_users(query, page)
            return
        if action == "set_role_select":
            if not payload:
                await query.answer("Некорректный пользователь", show_alert=True)
                return
            await self._show_set_role_detail(query, int(payload))
            return
        if action == "set_role_assign":
            if not payload:
                await query.answer("Нет данных", show_alert=True)
                return
            user_part, role_part = payload.split("|", 1)
            await self._assign_role_from_panel(query, int(user_part), role_part)
            return
        if action == "maintenance_alert":
            await self._send_maintenance_alert(query, context)
            return
        await query.answer("Команда в разработке", show_alert=True)

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

    async def _run_weekly_quality(self, query, context: ContextTypes.DEFAULT_TYPE) -> None:
        service = context.application.bot_data.get("weekly_quality_service")
        if not service:
            await query.answer("Сервис недоступен", show_alert=True)
            return
        try:
            report_text = await service.get_text_report(period="weekly")
        except Exception as exc:
            logger.exception("weekly_quality shortcut failed: %s", exc)
            await safe_edit_message(
                query,
                text="❌ Не удалось построить отчёт качества.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀️ Назад", callback_data=self._callback("commands"))]]
                ),
            )
            return
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔄 Обновить", callback_data=self._callback("command", "weekly_quality")
                    )
                ],
                [InlineKeyboardButton("◀️ Назад", callback_data=self._callback("commands"))],
            ]
        )
        await safe_edit_message(
            query,
            text=report_text,
            reply_markup=keyboard,
        )

    async def _open_report_flow(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        handler = context.application.bot_data.get("report_handler")
        if not handler:
            await update.callback_query.answer("Сервис отчётов недоступен", show_alert=True)
            return
        await handler.start_report_flow(update, context, period="daily", date_range=None)

    async def _show_admins_list(self, query) -> None:
        admins = await self.admin_repo.get_admins()
        if not admins:
            await safe_edit_message(
                query,
                text="👑 Нет администраторов в системе.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀️ Назад", callback_data=self._callback("commands"))]]
                ),
            )
            return
        text = "👑 <b>Список администраторов</b>\n\n"
        for admin in admins:
            role_info = admin.get("role")
            role_name = None
            if isinstance(role_info, dict):
                role_name = role_info.get("name")
            role_name = role_name or admin.get("role_name") or "—"
            username = admin.get("username") or "—"
            text += f"• <b>{admin.get('full_name', 'Без имени')}</b> — {role_name}\n   @{username}\n\n"
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("◀️ Назад", callback_data=self._callback("commands"))]]
            ),
            parse_mode="HTML",
        )

    async def _show_set_role_users(self, query, page: int) -> None:
        users = await self.admin_repo.get_all_users(status_filter="approved")
        if not users:
            await safe_edit_message(
                query,
                text="Нет утверждённых пользователей.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀️ Назад", callback_data=self._callback("commands"))]]
                ),
            )
            return
        page_size = 8
        total_pages = max(1, (len(users) + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))
        start = page * page_size
        end = start + page_size
        keyboard: List[List[InlineKeyboardButton]] = []
        for user in users[start:end]:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        user.get("full_name") or f"#{user.get('id')}",
                        callback_data=self._callback(
                            "command", "set_role_select", str(user.get("id"))
                        ),
                    )
                ]
            )
        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    "⬅️", callback_data=self._callback("command", "set_role_page", str(page - 1))
                )
            )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    "➡️", callback_data=self._callback("command", "set_role_page", str(page + 1))
                )
            )
        if nav_row:
            keyboard.append(nav_row)
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=self._callback("commands"))])
        await safe_edit_message(
            query,
            text="🧩 <b>Выберите пользователя для смены роли</b>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    async def _show_set_role_detail(self, query, user_id: int) -> None:
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        roles = await self.permissions.list_roles()
        seen: set[str] = set()
        actor = query.from_user
        username = actor.username if actor else None
        buttons: List[List[InlineKeyboardButton]] = []
        for role in roles:
            slug = role["slug"]
            display = role["display_name"]
            if slug in seen:
                continue
            seen.add(slug)
            can_assign = (
                await self.permissions.can_promote(actor.id, slug, username)
                if actor
                else False
            )
            if not can_assign:
                continue
            buttons.append(
                [
                    InlineKeyboardButton(
                        display,
                        callback_data=self._callback(
                            "command", "set_role_assign", f"{user_id}|{slug}"
                        ),
                    )
                ]
            )
        if not buttons:
            await query.answer("Нет доступных ролей", show_alert=True)
            await self._show_set_role_users(query, 0)
            return
        buttons.append(
            [
                InlineKeyboardButton(
                    "◀️ Назад к списку",
                    callback_data=self._callback("command", "set_role"),
                )
            ]
        )
        role_info = user.get("role")
        if isinstance(role_info, dict):
            current_role = role_info.get("name") or role_info.get("slug")
        else:
            current_role = role_name_from_id(user.get("role_id"))
        info = (
            f"🧩 <b>Смена роли</b>\n"
            f"Пользователь: <b>{user.get('full_name', '—')}</b>\n"
            f"Текущая роль: {current_role}\n"
            "Выберите новую роль:"
        )
        await safe_edit_message(
            query,
            text=info,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML",
        )

    async def _assign_role_from_panel(
        self, query, user_id: int, role_slug: str
    ) -> None:
        actor = query.from_user
        if not actor:
            return
        can_assign = await self.permissions.can_promote(actor.id, role_slug, actor.username)
        if not can_assign:
            await query.answer("Недостаточно прав для назначения роли", show_alert=True)
            return
        success = await self.admin_repo.set_user_role(user_id, role_slug, actor.id)
        if success:
            await query.answer("✅ Роль обновлена")
        else:
            await query.answer("Не удалось обновить роль", show_alert=True)
        await self._show_set_role_detail(query, user_id)

    async def _send_maintenance_alert(
        self, query, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        recipients = await self.admin_repo.get_users_with_chat_ids()
        bot = context.application.bot
        message = (
            "⚠️ <b>Внимание!</b>\n"
            "Ведутся технические работы. Возможны временные перебои в работе бота."
        )
        sent = 0
        for row in recipients:
            raw_id = row.get("user_id")
            if not raw_id:
                continue
            try:
                chat_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            try:
                await bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
                sent += 1
            except Exception as exc:
                logger.warning(
                    "Не удалось отправить оповещение пользователю %s: %s",
                    chat_id,
                    exc,
                )
        await query.answer(f"Сообщение отправлено ({sent})", show_alert=True)

    def _parse_callback(self, data: str) -> Tuple[str, Optional[str], Optional[str]]:
        if not data.startswith(f"{ADMIN_PREFIX}:"):
            return data, None, None
        parts = data.split(":")
        section = parts[1] if len(parts) > 1 else None
        action = parts[2] if len(parts) > 2 else None
        payload = parts[3] if len(parts) > 3 else None
        return section or "", action, payload

    def _build_roles_summary(self, counters: Optional[dict]) -> str:
        """Формирует текстовый блок с разбивкой по ролям."""
        if not counters:
            return "—"
        breakdown = counters.get("roles_breakdown") or {}
        lines = []
        for role in ROLE_DISPLAY_ORDER:
            stats = breakdown.get(role, {})
            emoji = ROLE_EMOJI.get(role, "•")
            display_name = stats.get("display") or role_display_name_from_name(role)
            approved = int(stats.get("approved") or 0)
            lines.append(f"{emoji} {display_name}: <b>{approved}</b>")
        # Выводим роли, которых нет в стандартном порядке, но присутствуют в БД
        for role_name in breakdown.keys():
            if role_name in ROLE_DISPLAY_ORDER:
                continue
            display_name = stats.get("display") or role_display_name_from_name(role_name)
            emoji = ROLE_EMOJI.get(role_name, "•")
            approved = int(breakdown[role_name].get("approved") or 0)
            lines.append(f"{emoji} {display_name}: <b>{approved}</b>")
        return "\n".join(lines) if lines else "—"


def register_admin_panel_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager
):
    """Регистрирует хендлеры админ-панели."""
    handler = AdminPanelHandler(admin_repo, permissions)
    
    # Команда /admin и reply-кнопка (если библиотека поддерживает MessageHandler)
    if MessageHandler and filters:
        application.add_handler(
            MessageHandler(
                filters.Regex(r"^👑 Админ-панель$"),
                handler.admin_command,
            )
        )
    application.add_handler(CommandHandler("admin", handler.admin_command))
    
    # Callback handlers
    application.add_handler(
        CallbackQueryHandler(handler.handle_callback, pattern=r"^admin:(dashboard|back|menu|commands|command)")
    )

    logger.info("Admin panel handlers registered")
