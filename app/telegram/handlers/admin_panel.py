# Файл: app/telegram/handlers/admin_panel.py

"""
Главный обработчик админ-панели.

Предоставляет точку входа /admin и основное меню.
"""

from typing import Optional, List
from datetime import datetime
from zoneinfo import ZoneInfo

from app.telegram.utils.callback_data import AdminCB
from app.telegram.utils.callback_lm import LMCB
from app.telegram.utils.state import reset_feature_states

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TelegramError

from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    Application,
    MessageHandler,
    filters,
)

from app.db.repositories.admin import AdminRepository
from app.db.repositories.lm_repository import LMRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message
from app.utils.error_handlers import log_async_exceptions
from app.utils.rate_limit import rate_limit_hit
from app.utils.job_guard import JobGuard
from app.core.roles import role_name_from_id
from app.telegram.ui.admin.screens import Screen
from app.telegram.ui.admin.screens.menu import render_main_menu_screen
from app.config import DEV_ADMIN_ID
from app.telegram.ui.admin.screens.dashboard import render_dashboard_screen
from app.telegram.ui.admin.screens.alerts import render_alerts_screen
from app.telegram.ui.admin.screens.export import render_export_screen
from app.telegram.ui.admin.screens.manual import render_manual_screen
from app.telegram.handlers.manual import _load_video_file_id
from app.telegram.ui.admin.screens.dangerous_ops import (
    render_dangerous_ops_screen,
    render_critical_confirmation,
)
from app.telegram.ui.admin.screens.approvals import (
    render_approvals_list_screen,
    render_empty_approvals_screen,
    render_approval_detail_screen,
)
from app.telegram.ui.admin.screens.promotions import (
    render_promotion_menu_screen,
    render_promotion_list_screen,
    render_empty_promotion_screen,
    render_promotion_detail_screen,
)
from app.telegram.ui.admin.screens.lm_screens import render_lm_periods_screen
from app.telegram.ui.admin.screens.call_export import render_call_export_screen
from app.telegram.ui.admin import keyboards as admin_keyboards
from app.telegram.keyboards.inline_system import build_system_menu
from app.telegram.utils.admin_registry import get_admin_callback_handler
from app.telegram.handlers.admin_lm import LMHandlers
from app.utils.periods import calculate_period_bounds
from app.services.call_export import CallExportService, EXPORT_PERIOD_OPTIONS

LM_PERIOD_OPTIONS = (7, 14, 30, 180)

logger = get_watchdog_logger(__name__)


# MessageHandler is already imported as _MessageHandler to avoid name clashes, 
# but we can just use the standard name if we don't need a shim.
from telegram.ext import MessageHandler

class AdminPanelHandler:
    """Основной хендлер админ-панели."""
    
    SYSTEM_MENU_ROLES = {"founder", "head_of_registry"}
    
    def __init__(
        self,
        admin_repo: AdminRepository,
        permissions: PermissionsManager
    ):
        self.admin_repo = admin_repo
        self.permissions = permissions
        self.approvals_page_size = 8
    
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

        # Сброс состояний других фич (чтобы не перехватывали ввод)
        reset_feature_states(context, update.effective_chat.id if update.effective_chat else None)
        
        logger.info("Админ-панель открыта пользователем %s", describe_user(user))
        # Главный экран — меню админ-панели
        await self._show_main_menu(update, context)
    
    async def _show_main_menu(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ):
        """Отображает компактное главное меню с навигацией по разделам."""
        user = update.effective_user
        allow_commands = False
        allow_yandex_tools = False
        try:
            allow_commands = await self.permissions.has_permission(
                user.id,
                "commands",
                user.username,
            )
            allow_yandex_tools = await self.permissions.has_permission(
                user.id,
                "debug",
                user.username,
            )
        except Exception:
            logger.warning("Не удалось определить права пользователя %s", describe_user(user))

        # Сброс состояний других фич
        reset_feature_states(context, update.effective_chat.id if update.effective_chat else None)

        screen = render_main_menu_screen(
            allow_commands,
            allow_yandex_tools,
            False,
            False,
            None,
        )
        await self._render_screen(update, screen)
        logger.debug(
            "Показано главное меню админ-панели для %s",
            describe_user(user),
        )

    async def _render_screen(
        self,
        update: Update,
        screen: Screen,
    ) -> None:
        """Единая точка отрисовки экранов (редактирование/отправка)."""
        markup = InlineKeyboardMarkup(screen.keyboard)
        query = update.callback_query
        if query:
            try:
                await safe_edit_message(
                    query,
                    text=screen.text,
                    reply_markup=markup,
                    parse_mode=screen.parse_mode,
                )
                return
            except (BadRequest, TelegramError) as exc:
                error_text = str(exc)
                if isinstance(exc, BadRequest) and "message is not modified" in error_text.lower():
                    logger.debug("Admin screen refresh ignored: message is not modified.")
                    await query.answer()
                    return
                logger.warning("Не удалось отредактировать сообщение админки: %s", exc)
                message = query.message
                if message:
                    await message.reply_text(
                        text=screen.text,
                        reply_markup=markup,
                        parse_mode=screen.parse_mode,
                    )
                return
        if update.message:
            await update.message.reply_text(
                text=screen.text,
                reply_markup=markup,
                parse_mode=screen.parse_mode,
            )
        elif update.effective_chat:
            await update.effective_chat.send_message(
                text=screen.text,
                reply_markup=markup,
                parse_mode=screen.parse_mode,
            )
    
    @log_async_exceptions
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Роутер для callback-запросов админ-панели."""
        query = update.callback_query
        if not query:
            return

        # Сразу подтверждаем callback, чтобы избежать таймаута Telegram.
        await self._safe_answer(query)

        data = query.data or ""
        cb_action, cb_args = AdminCB.parse(data)

        # Resolve hashed fallback callback_data (adm:hd:<digest>) if present.
        # When AdminCB.create produced a hashed fallback, it registers the original
        # callback string in AdminCB._hash_registry via AdminCB.register_hash.
        # Here we try to resolve that digest back to the original callback_data and
        # re-parse it so normal routing can proceed. We first check in-memory cache,
        # then attempt async Redis lookup if configured.
        if cb_action == AdminCB.HD:
            digest = cb_args[0] if cb_args else None
            original = None
            if digest:
                # Fast path: in-memory
                try:
                    original = AdminCB.resolve_hash(digest)
                except Exception:
                    original = None
                # Slow path: async Redis-backed resolve
                if not original:
                    try:
                        original = await AdminCB.resolve_hash_async(digest)
                    except Exception:
                        original = None
            if original:
                data = original
                cb_action, cb_args = AdminCB.parse(data)
            else:
                # Не удалось разрешить хеш — аккуратный fallback в меню.
                await self._safe_answer(query)
                await self._handle_unknown_callback(query)
                return True

        user = update.effective_user
        logger.info(
            "Admin callback: action=%s args=%s data=%s",
            cb_action,
            cb_args,
            data,
            extra={"user_id": user.id if user else None},
        )

        if not cb_action:
            await self._handle_unknown_callback(query)
            return True

        handled = await self._handle_new_callback(cb_action, cb_args, update, context)
        if not handled:
            await self._handle_unknown_callback(query)
        return True

    async def _safe_answer(self, query, *args, **kwargs) -> bool:
        """Best-effort ответ на callback без проброса ошибок таймаута/invalid id."""
        try:
            await query.answer(*args, **kwargs)
            return True
        except BadRequest as exc:
            error_text = str(exc).lower()
            if "query is too old" in error_text or "query id is invalid" in error_text:
                logger.debug("Callback уже устарел или id недействителен: %s", exc)
                return False
            logger.warning("Ошибка ответа на callback: %s", exc)
            return False
        except TelegramError as exc:
            logger.warning("Ошибка Telegram при ответе на callback: %s", exc)
            return False

    async def _handle_new_callback(
        self,
        action: str,
        args: List[str],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        if action == AdminCB.DASHBOARD:
            await self._show_dashboard(update, context)
            return True
        if action == AdminCB.DASHBOARD_DETAILS:
            await self._handle_dashboard_details_deprecated(update, context)
            return True
        if action == AdminCB.ALERTS:
            await self._show_alerts_screen(update)
            return True
        if action == AdminCB.EXPORT:
            await self._show_export_screen(update)
            return True
        if action == AdminCB.CALL_EXPORT:
            await self._handle_call_export_action(args, update, context)
            return True
        if action == AdminCB.LM_MENU:
            sub_action = args[0] if args else None
            period_key = args[1] if len(args) > 1 else None
            if sub_action == AdminCB.lm_SUM:
                await self._show_lm_periods(update, context, period_key)
            else:
                await self._show_lm_periods(update, context)
            return True
        if action == AdminCB.CRITICAL:
            target = args[0] if args else None
            if target:
                await self._show_critical_operation_confirmation(update, target)
            else:
                await self._show_dangerous_ops_screen(update)
            return True
        if action == AdminCB.SYSTEM:
            await self._open_system_tools(update, context)
            return True
        if action == AdminCB.BACK:
            await self._show_main_menu(update, context)
            return True
        if action == AdminCB.COMMANDS:
            await self._show_main_menu(update, context)
            return True
        if action == AdminCB.APPROVALS:
            await self._handle_approvals_flow(args, update, context)
            return True
        if action == AdminCB.PROMOTION:
            await self._handle_promotion_flow(args, update, context)
            return True
        if action == AdminCB.CALL_LOOKUP:
            handler = context.application.bot_data.get("call_lookup_handler")
            if not handler:
                logger.error("Call lookup handler is not registered in bot_data")
                return False
            await handler.handle_callback(update, context)
            return True
        if action == AdminCB.USERS:
            handler = get_admin_callback_handler(context, AdminCB.USERS)
            if not handler:
                logger.error("Admin users handler is not registered in bot_data")
                return False
            await handler(update, context)
            return True
        if action == AdminCB.CALL:
            handler = context.application.bot_data.get("call_lookup_handler")
            if not handler:
                logger.error("Call handler is not registered in bot_data")
                return False
            await handler.handle_call_callback(update, context, args)
            return True
        if action == AdminCB.YANDEX:
            handler = context.application.bot_data.get("call_lookup_handler")
            if not handler:
                logger.error("Call lookup handler is not registered in bot_data")
                return False
            await handler.handle_reindex(update, context)
            return True
        if action == AdminCB.COMMAND:
            payload = args[1] if len(args) > 1 else None
            await self._handle_command_action(args[0] if args else None, payload, update, context)
            return True
        if action == AdminCB.HELP_SCREEN:
            await self._show_inline_help(update, context)
            return True
        if action == AdminCB.MANUAL:
            if args and args[0] == "video":
                await self._start_manual_video_upload(update, context)
            elif args and args[0] == "video_delete":
                await self._delete_manual_video(update, context)
            else:
                await self._show_manual_link(update, context)
            return True
        handler = get_admin_callback_handler(context, action)
        if handler:
            await handler(update, context)
            return True
        return False

    async def _show_inline_help(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        handler = context.application.bot_data.get("help_command_handler")
        if handler:
            await handler(update, context)
            return
        await self._reply_feature_unavailable(update, "Справка временно недоступна.")

    async def _show_manual_link(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        user = update.effective_user
        allow_video_upload = bool(user and self.permissions.is_dev_admin(user.id, user.username))
        allow_video_delete = False
        video_status = None
        video_id = None
        if allow_video_upload:
            has_video = context.application.bot_data.get("manual_video_has_file")
            if callable(has_video) and has_video():
                allow_video_delete = True
                video_status = "установлено"
                try:
                    video_id = _load_video_file_id()
                except Exception:
                    video_id = None
            else:
                video_status = "нет"
        screen = render_manual_screen(
            allow_video_upload=allow_video_upload,
            allow_video_delete=allow_video_delete,
            video_status=video_status,
        )
        await self._render_screen(update, screen)
        if video_id:
            message = update.effective_message
            if message:
                await context.bot.send_video(
                    chat_id=message.chat_id,
                    video=video_id,
                    message_thread_id=getattr(message, "message_thread_id", None),
                )

    async def _start_manual_video_upload(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        handler = context.application.bot_data.get("manual_video_upload_handler")
        if handler:
            await handler(update, context)
            return
        await self._reply_feature_unavailable(update, "Отправка видео недоступна.")

    async def _delete_manual_video(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        handler = context.application.bot_data.get("manual_video_delete_handler")
        if handler:
            await handler(update, context)
            return
        await self._reply_feature_unavailable(update, "Удаление видео недоступно.")

    async def _reply_feature_unavailable(
        self,
        update: Update,
        message: str,
    ) -> None:
        target = update.effective_message
        if target:
            await target.reply_text(message)
        elif update.callback_query:
            await update.callback_query.answer(message, show_alert=True)

    async def _handle_approvals_flow(
        self,
        args: List[str],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        actor = update.effective_user
        if not query or not actor:
            return
        if not await self._can_approve(actor.id, actor.username):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        sub_action = args[0] if args else AdminCB.LIST
        if sub_action == AdminCB.DETAILS:
            page = self._safe_int(args[1]) if len(args) > 1 else 0
            user_id = self._safe_int(args[2]) if len(args) > 2 else 0
            await self._show_approval_detail(update, page, user_id)
            return
        if sub_action == AdminCB.APPROVE:
            user_id = self._safe_int(args[1]) if len(args) > 1 else 0
            telegram_id = self._safe_int(args[2]) if len(args) > 2 else 0
            page = self._safe_int(args[3]) if len(args) > 3 else 0
            await self._approve_pending_user(update, context, user_id, telegram_id, page)
            return
        if sub_action == AdminCB.DECLINE:
            telegram_id = self._safe_int(args[1]) if len(args) > 1 else 0
            page = self._safe_int(args[2]) if len(args) > 2 else 0
            await self._decline_pending_user(update, context, telegram_id, page)
            return
        if sub_action == AdminCB.BACK:
            handler = get_admin_callback_handler(context, AdminCB.USERS)
            if handler:
                await handler(update, context)
            else:
                await self._show_main_menu(update, context)
            return
        page = self._safe_int(args[1]) if len(args) > 1 else 0
        await self._show_approvals_list(update, page)

    async def _show_lm_periods(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        period_key: Optional[str] = None,
    ) -> None:
        lm_repo: Optional[LMRepository] = context.application.bot_data.get("lm_repository")  # type: ignore[assignment]
        if not lm_repo:
            await self._reply_feature_unavailable(update, "LM-аналитика временно недоступна.")
            return

        try:
            requested_days = int(period_key) if period_key else LM_PERIOD_OPTIONS[0]
        except (TypeError, ValueError):
            requested_days = LM_PERIOD_OPTIONS[0]
        if requested_days not in LM_PERIOD_OPTIONS:
            requested_days = LM_PERIOD_OPTIONS[0]

        reference_ts = datetime.now()
        try:
            summary = await lm_repo.get_lm_period_summary(requested_days, reference=reference_ts)
        except Exception as exc:
            logger.exception("Не удалось загрузить LM агрегаты: %s", exc)
            await self._reply_feature_unavailable(update, "Ошибка загрузки LM-аналитики.")
            return

        # Синхронизируем числа на кнопках с реальными списками действий.
        try:
            period_start, period_end = calculate_period_bounds(requested_days, reference=reference_ts)
            action_counts = {
                "complaints": await lm_repo.get_action_count(
                    "complaints", start_date=period_start, end_date=period_end
                ),
                "followup": await lm_repo.get_action_count(
                    "followup", start_date=period_start, end_date=period_end
                ),
                "lost": await lm_repo.get_action_count(
                    "lost", start_date=period_start, end_date=period_end
                ),
                "churn": await lm_repo.get_action_count(
                    "churn", start_date=period_start, end_date=period_end
                ),
            }
            summary["action_counts"] = action_counts
        except Exception as exc:
            logger.warning("Не удалось синхронизировать счётчики списков LM: %s", exc)

        context.user_data["lm:last_period_days"] = requested_days
        context.user_data["lm:last_period_reference"] = reference_ts.timestamp()
        screen = render_lm_periods_screen(summary, requested_days, LM_PERIOD_OPTIONS)
        await self._render_screen(update, screen)

    async def _handle_promotion_flow(
        self,
        args: List[str],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        actor = update.effective_user
        if not query or not actor:
            return
        sub_action = args[0] if args else "menu"
        if sub_action == "menu":
            await self._render_screen(update, render_promotion_menu_screen())
            return
        if sub_action == AdminCB.LIST:
            role_slug = args[1] if len(args) > 1 else "admin"
            await self._show_promotion_list(update, actor, role_slug)
            return
        if sub_action == AdminCB.DETAILS:
            role_slug = args[1] if len(args) > 1 else "admin"
            user_id = self._safe_int(args[2]) if len(args) > 2 else 0
            await self._show_promotion_detail(update, actor, role_slug, user_id)
            return
        if sub_action == AdminCB.APPROVE:
            role_slug = args[1] if len(args) > 1 else "admin"
            telegram_id = self._safe_int(args[2]) if len(args) > 2 else 0
            await self._promote_user(update, context, actor, role_slug, telegram_id)
            return
        await self._render_screen(update, render_promotion_menu_screen())

    async def _show_approvals_list(self, update: Update, page: int) -> None:
        page = max(0, page)
        limit = self.approvals_page_size
        offset = page * limit
        users, total = await self.admin_repo.get_users_page("pending", limit, offset)
        if not total:
            await self._render_screen(update, render_empty_approvals_screen())
            return
        total_pages = max(1, (total + limit - 1) // limit)
        if page >= total_pages:
            page = total_pages - 1
            offset = page * limit
            users, _ = await self.admin_repo.get_users_page("pending", limit, offset)
        await self._render_screen(
            update,
            render_approvals_list_screen(users, page, total_pages),
        )

    async def _show_approval_detail(self, update: Update, page: int, user_id: int) -> None:
        if not user_id:
            await self._show_approvals_list(update, page)
            return
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await self._render_screen(
                update,
                Screen(
                    text="❌ Пользователь не найден",
                    keyboard=admin_keyboards.back_only_keyboard(),
                ),
            )
            return
        await self._render_screen(update, render_approval_detail_screen(user, page))

    async def _approve_pending_user(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        user_id: int,
        telegram_id: int,
        page: int,
    ) -> None:
        query = update.callback_query
        actor = update.effective_user
        if not query or not actor or not user_id:
            return
        if await self._rate_limit(
            query,
            context,
            "admin_approvals_action",
            cooldown=1.5,
            alert_text="Слишком часто выполняете действие. Подождите немного.",
        ):
            return
        success = await self.admin_repo.approve_user(user_id, actor.id)
        if success:
            await query.answer("✅ Пользователь утверждён", show_alert=True)
        else:
            await query.answer("❌ Не удалось утвердить", show_alert=True)
        await self._show_approvals_list(update, page)

    async def _decline_pending_user(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        telegram_id: int,
        page: int,
    ) -> None:
        query = update.callback_query
        actor = update.effective_user
        if not query or not actor or not telegram_id:
            return
        if await self._rate_limit(
            query,
            context,
            "admin_decline_action",
            cooldown=1.5,
            alert_text="Слишком часто выполняете действие. Подождите немного.",
        ):
            return
        success = await self.admin_repo.decline_user(telegram_id, actor.id)
        if success:
            await query.answer("🗑️ Заявка отклонена", show_alert=True)
        else:
            await query.answer("❌ Не удалось отклонить", show_alert=True)
        await self._show_approvals_list(update, page)

    async def _show_promotion_list(self, update: Update, actor, role_slug: str) -> None:
        if not await self.permissions.can_promote(actor.id, role_slug, actor.username):
            await update.callback_query.answer("Недостаточно прав", show_alert=True)
            return
        candidates = await self.admin_repo.get_users_for_promotion(target_role=role_slug)
        if not candidates:
            await self._render_screen(update, render_empty_promotion_screen(role_slug))
            return
        await self._render_screen(update, render_promotion_list_screen(candidates, role_slug))

    async def _show_promotion_detail(self, update: Update, actor, role_slug: str, user_id: int) -> None:
        if not user_id:
            await self._show_promotion_list(update, actor, role_slug)
            return
        if not await self.permissions.can_promote(actor.id, role_slug, actor.username):
            await update.callback_query.answer("Недостаточно прав", show_alert=True)
            return
        user = await self.admin_repo.get_user_by_id(user_id)
        if not user:
            await self._render_screen(update, render_empty_promotion_screen(role_slug))
            return
        await self._render_screen(update, render_promotion_detail_screen(user, role_slug))

    async def _promote_user(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        actor,
        role_slug: str,
        telegram_id: int,
    ) -> None:
        query = update.callback_query
        if not query or not telegram_id:
            return
        if not await self.permissions.can_promote(actor.id, role_slug, actor.username):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        if await self._rate_limit(
            query,
            context,
            f"admin_promote_{role_slug}",
            cooldown=2.5,
            alert_text="Слишком часто выполняете повышение. Подождите немного.",
        ):
            return
        success = await self.admin_repo.promote_user(telegram_id, role_slug, actor.id)
        if success:
            await query.answer("✅ Пользователь повышен", show_alert=True)
        else:
            await query.answer("❌ Не удалось повысить", show_alert=True)
        await self._show_promotion_list(update, actor, role_slug)

    async def _can_approve(self, user_id: int, username: Optional[str]) -> bool:
        try:
            return await self.permissions.can_approve(user_id, username)
        except Exception:
            logger.warning("Не удалось проверить право approve для %s", user_id)
            return False

    async def _handle_unknown_callback(self, query) -> None:
        await query.answer("Неизвестная команда", show_alert=True)
        await safe_edit_message(
            query,
            text="❓ Команда не поддерживается. Вернитесь в меню.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 В меню", callback_data=AdminCB.create(AdminCB.BACK))]]
            ),
            parse_mode="HTML",
        )

    async def _show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает dashboard с основными метриками."""
        query = update.callback_query
        user = update.effective_user
        if query and await self._rate_limit(
            query,
            context,
            "admin_dashboard",
            cooldown=2.0,
            alert_text="Слишком часто обновляете. Подождите пару секунд.",
        ):
            return

        try:
            counters = await self.admin_repo.get_users_counters()
        except Exception as exc:
            logger.error("Не удалось загрузить дашборд: %s", exc)
            await self._render_screen(
                update,
                Screen(
                    text="⚠️ Не удалось загрузить дашборд. Попробуйте снова чуть позже.",
                    keyboard=admin_keyboards.dashboard_error_keyboard(),
                ),
            )
            return
        pending_count = counters.get('pending_users', 0)
        admin_count = counters.get('admins', 0)
        approved_count = counters.get('approved_users', 0)
        regular_users = counters.get(
            'non_admin_approved',
            max(0, approved_count - admin_count)
        )
        blocked_count = counters.get('blocked_users', 0)
        total_users = counters.get('total_users', 0)

        updated_at = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%H:%M:%S")

        logger.info(
            "Дашборд открыт пользователем %s (pending=%s admins=%s)",
            describe_user(user),
            pending_count,
            admin_count,
        )
        
        screen = render_dashboard_screen(counters, updated_at)
        await self._render_screen(update, screen)

    async def _show_alerts_screen(self, update: Update) -> None:
        await self._render_screen(update, render_alerts_screen())

    async def _show_export_screen(self, update: Update) -> None:
        await self._render_screen(update, render_export_screen())

    async def _show_call_export_screen(self, update: Update) -> None:
        await self._render_screen(update, render_call_export_screen())

    async def _show_dangerous_ops_screen(self, update: Update) -> None:
        await self._render_screen(update, render_dangerous_ops_screen())

    async def _handle_dashboard_details_deprecated(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        if query:
            await query.answer("Экран деталей отключён. Обновляю дашборд.", show_alert=True)
        await self._show_dashboard(update, context)

    async def _open_system_tools(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return
        if not await self._can_use_system_tools(user.id, user.username):
            await query.answer("Недостаточно прав", show_alert=True)
            return
        include_cache_reset = self.permissions.is_dev_admin(user.id, user.username)
        description = (
            "⚙️ <b>Системные функции</b>\n"
            "⚠️ Только для Dev и руководства. Команды выполняются мгновенно и могут влиять на прод.\n\n"
            "• 🔍 Состояние бота — проверка доступности БД/пула.\n"
            "• ❌ Последние ошибки — выборка последних записей из логов.\n"
            "• 🔌 Проверка БД — короткий тест соединения и версии MySQL.\n"
            "• 🗑️ Очистить кеш — только Dev, очищает Redis/локальный кеш.\n"
        )
        await safe_edit_message(
            query,
            text=description,
            reply_markup=build_system_menu(include_cache_reset),
            parse_mode="HTML",
        )

    async def _can_use_system_tools(self, user_id: int, username: Optional[str]) -> bool:
        if self.permissions.is_supreme_admin(user_id, username):
            return True
        if self.permissions.is_dev_admin(user_id, username):
            return True
        role = await self.permissions.get_effective_role(user_id, username)
        return role in self.SYSTEM_MENU_ROLES

    async def _show_critical_operation_confirmation(
        self,
        update: Update,
        action_key: str,
    ) -> None:
        action_texts = {
            "weekly_quality": "Еженедельный контроль качества. Готовит тяжёлый CSV и отчёт.",
            "report": "Отчет-Операторы по текущим звонкам. Потребляет LM-квоту.",
            "maintenance_alert": "Рассылает всем пользователям предупреждение о техработах.",
        }
        description = action_texts.get(action_key)
        if not description:
            await self._show_dangerous_ops_screen(update)
            return
        await self._render_screen(update, render_critical_confirmation(action_key, description))

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
        if await self._rate_limit(
            query,
            context,
            "admin_commands_menu",
            cooldown=1.5,
            alert_text="Слишком часто открываете меню. Подождите немного.",
        ):
            return
        text = (
            "📑 <b>Команды бота</b>\n"
            "Выберите нужное действие – команда выполнится автоматически."
        )
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    async def _has_commands_access(self, user_id: int, username: Optional[str]) -> bool:
        try:
            return await self.permissions.has_permission(
                user_id,
                "commands",
                username,
            )
        except Exception:
            logger.warning("Не удалось проверить доступ к командам для %s", user_id)
            return False

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
        job_guard = self._get_job_guard(context)
        if action == "weekly_quality":
            if await self._rate_limit(query, context, "admin_weekly_quality", 2.0):
                return
            guard_key = "job:weekly_quality"
            if not await job_guard.acquire(guard_key):
                await query.answer("Задача уже выполняется. Подождите.", show_alert=True)
                return
            try:
                await self._run_weekly_quality(query, context)
            finally:
                job_guard.release(guard_key)
            return
        if action == "report":
            if await self._rate_limit(query, context, "admin_ai_report", 2.0):
                return
            guard_key = "job:ai_report"
            if not await job_guard.acquire(guard_key):
                await query.answer("Задача уже выполняется. Подождите.", show_alert=True)
                return
            try:
                await self._open_report_flow(update, context)
            finally:
                job_guard.release(guard_key)
            return
        if action == "admins":
            if await self._rate_limit(query, context, "admin_list_admins", 1.5):
                return
            await self._show_admins_list(query)
            return
        if action == "set_role":
            if await self._rate_limit(query, context, "admin_set_role_list", 1.5):
                return
            await self._show_set_role_users(query, 0)
            return
        if action == "set_role_page":
            page = int(payload or "0")
            if await self._rate_limit(query, context, "admin_set_role_page", 1.0):
                return
            await self._show_set_role_users(query, page)
            return
        if action == "set_role_select":
            if not payload:
                await query.answer("Некорректный пользователь", show_alert=True)
                return
            if await self._rate_limit(query, context, "admin_set_role_detail", 1.0):
                return
            await self._show_set_role_detail(query, int(payload))
            return
        if action == "set_role_assign":
            if not payload:
                await query.answer("Нет данных", show_alert=True)
                return
            if await self._rate_limit(
                query,
                context,
                "admin_set_role_assign",
                cooldown=6.0,
                alert_text="Недавно изменяли роли. Подождите несколько секунд.",
            ):
                return
            user_part, role_part = payload.split("|", 1)
            await self._assign_role_from_panel(query, int(user_part), role_part)
            return
        if action == "maintenance_alert":
            if await self._rate_limit(
                query,
                context,
                "admin_maintenance_alert",
                cooldown=8.0,
                alert_text="Слишком часто отправляете оповещения. Сделайте паузу.",
            ):
                return
            guard_key = "job:maintenance_alert"
            if not await job_guard.acquire(guard_key):
                await query.answer("Рассылка уже выполняется.", show_alert=True)
                return
            try:
                await self._send_maintenance_alert(query, context)
            finally:
                job_guard.release(guard_key)
            return

        # Delegate unknown command actions to the admin_commands handler if present.
        # This keeps admin-panel as the single router while allowing feature handlers
        # (like AdminCommandsHandler) to implement their own business logic.
        commands_handler = context.application.bot_data.get("admin_commands_handler")
        if commands_handler and hasattr(commands_handler, "handle_admin_command_action"):
            try:
                delegated = await commands_handler.handle_admin_command_action(action, payload, update, context)
                if delegated:
                    return
            except Exception as exc:
                logger.exception("Delegation to admin_commands failed: %s", exc)

        await query.answer("Команда в разработке", show_alert=True)

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
                    [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]
                ),
            )
            return
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔄 Обновить", callback_data=AdminCB.create(AdminCB.COMMAND, "weekly_quality")
                    )
                ],
                [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))],
            ]
        )
        await safe_edit_message(
            query,
            text=report_text,
            reply_markup=keyboard,
        )

    async def _handle_call_export_action(
        self,
        args: List[str],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        if not query:
            return
        if not args:
            await self._show_call_export_screen(update)
            return
        days = self._safe_int(args[0])
        if days not in EXPORT_PERIOD_OPTIONS:
            await query.answer("Неизвестный диапазон", show_alert=True)
            return
        if await self._rate_limit(
            query,
            context,
            f"call_export_{days}",
            cooldown=6.0,
            alert_text="Недавно запускали выгрузку. Подождите пару секунд.",
        ):
            return
        try:
            await query.answer("Готовлю файл, это может занять несколько секунд…", show_alert=False)
        except TelegramError:
            logger.debug("Не удалось ответить на callback перед выгрузкой", exc_info=True)
        guard = self._get_job_guard(context)
        guard_key = f"job:call_export:{days}"
        if not await guard.acquire(guard_key):
            await query.answer("Ваша выгрузка уже готовится, подождите", show_alert=True)
            return
        try:
            await self._run_call_export(update, context, days)
        finally:
            guard.release(guard_key)

    async def _run_call_export(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        days: int,
    ) -> None:
        query = update.callback_query
        if not query:
            return
        service = context.application.bot_data.get("call_export_service")
        if not isinstance(service, CallExportService):
            await query.answer("Сервис выгрузки недоступен", show_alert=True)
            return
        try:
            buffer, filename, total_rows, (start_dt, end_dt) = await service.build_export(days)
        except Exception as exc:  # pragma: no cover - зависит от БД
            logger.exception("call_export failed: %s", exc)
            await query.answer("Не удалось подготовить файл", show_alert=True)
            return
        buffer.name = filename
        chat_id = (
            query.message.chat_id if query.message else update.effective_chat.id if update.effective_chat else None
        )
        if chat_id is None:
            await query.answer("Ошибка определения чата", show_alert=True)
            return
        caption = (
            f"Выгрузка звонков за {days} дн.\n"
            f"Период: {start_dt:%d.%m.%Y} — {end_dt:%d.%m.%Y}\n"
            f"Строк: {total_rows}"
        )
        try:
            await context.bot.send_document(
                chat_id=chat_id,
                document=buffer,
                filename=filename,
                caption=caption,
            )
        except TelegramError as exc:
            logger.exception("Не удалось отправить выгрузку: %s", exc)
            await query.answer("Отправка файла не удалась", show_alert=True)
            return
        try:
            await query.answer("Файл отправлен ✅")
        except TelegramError:
            logger.debug("Callback уже был закрыт во время отправки файла", exc_info=True)

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
                    [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]
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
                [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]
            ),
            parse_mode="HTML",
        )

    async def _show_set_role_users(self, query, page: int) -> None:
        page_size = 8
        page = max(0, page)
        offset = page * page_size
        users, total = await self.admin_repo.get_users_page(
            status_filter="approved",
            limit=page_size,
            offset=offset,
        )
        total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
        if total and not users and page > 0:
            page = total_pages - 1
            offset = page * page_size
            users, total = await self.admin_repo.get_users_page(
                status_filter="approved",
                limit=page_size,
                offset=offset,
            )
        if total == 0:
            await safe_edit_message(
                query,
                text="Нет утверждённых пользователей.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]
                ),
            )
            return
        keyboard: List[List[InlineKeyboardButton]] = []
        for user in users:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        user.get("full_name") or f"#{user.get('id')}",
                        callback_data=AdminCB.create(
                            AdminCB.COMMAND, "set_role_select", str(user.get("id"))
                        ),
                    )
                ]
            )
        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    "⬅️", callback_data=AdminCB.create(AdminCB.COMMAND, "set_role_page", str(page - 1))
                )
            )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    "➡️", callback_data=AdminCB.create(AdminCB.COMMAND, "set_role_page", str(page + 1))
                )
            )
        if nav_row:
            keyboard.append(nav_row)
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))])
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
                        callback_data=AdminCB.create(
                            AdminCB.COMMAND, "set_role_assign", f"{user_id}|{slug}"
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
                    callback_data=AdminCB.create(AdminCB.COMMAND, "set_role"),
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
            except (TypeError, ValueError) as exc:
                logger.warning("Некорректный chat_id в рассылке тех. работ: %s (%s)", raw_id, exc)
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

    async def _rate_limit(
        self,
        query,
        context: ContextTypes.DEFAULT_TYPE,
        key: str,
        cooldown: float,
        alert_text: str = "Слишком часто выполняете действие. Попробуйте позже.",
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
    
    @staticmethod
    def _safe_int(value: Optional[str], default: int = 0) -> int:
        try:
            return int(value) if value is not None else default
        except (TypeError, ValueError):
            return default
    
    def _get_job_guard(self, context: ContextTypes.DEFAULT_TYPE) -> JobGuard:
        guard = context.application.bot_data.get("job_guard")
        if isinstance(guard, JobGuard):
            return guard
        guard = JobGuard()
        context.application.bot_data["job_guard"] = guard
        return guard


def register_admin_panel_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager
):
    """Регистрирует хендлеры админ-панели."""
    application.bot_data.setdefault("admin_callback_handlers", {})
    handler = AdminPanelHandler(admin_repo, permissions)
    
    # Команда /admin и reply-кнопка
    reply_handler = MessageHandler(
        filters.Regex(r"(?i)^\s*(?:👑\s*)?админ-?панел[ья]\s*$"),
        handler.admin_command,
    )
    reply_handler.block = False
    application.add_handler(reply_handler, group=0)
    logger.info("Registered admin reply button handler (regex: админ-панел)")
    application.add_handler(CommandHandler("admin", handler.admin_command))
    
    # Callback handlers
    callback_handler = CallbackQueryHandler(
        handler.handle_callback,
        pattern=rf"^{AdminCB.PREFIX}:",
    )
    callback_handler.block = False
    application.add_handler(callback_handler)

    logger.info("Admin panel handlers registered")
