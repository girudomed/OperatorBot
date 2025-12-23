
"""
Обработчик LM-метрик (namespace 'lm:').
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from telegram import Update
from telegram.error import TelegramError
from telegram.ext import ContextTypes, CallbackQueryHandler, Application

from app.telegram.utils.callback_lm import LMCB
from app.telegram.utils.callback_data import AdminCB
from app.telegram.ui.admin.screens.lm_screens import (
    render_lm_summary_screen,
    render_lm_action_list_screen,
    render_lm_methodology_screen,
)
from app.db.repositories.lm_repository import LMRepository
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.services.lm_service import LMService
from app.utils.periods import calculate_period_bounds

logger = get_watchdog_logger(__name__)

class LMHandlers:
    """Обработка команд и callback-ов для LM метрик."""
    
    def __init__(self, repo: LMRepository, permissions=None, lm_service: Optional[LMService] = None):
        self.repo = repo
        self.permissions = permissions
        self.lm_service = lm_service

    @log_async_exceptions
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Роутер для lm: callback-ов."""
        query = update.callback_query
        if not query:
            return
            
        data = query.data
        if not LMCB.is_lm(data):
            return
            
        parts = LMCB.parse(data)
        if not parts:
            await query.answer("Ошибка формата данных")
            return
            
        action = parts[0]
        args = parts[1:]
        
        logger.info(f"[LM] Callback action: {action} (args: {args})")
        
        if action == LMCB.ACTION_SUMMARY:
            await self._show_summary(update, context, args)
        elif action == LMCB.ACTION_LIST:
            await self._show_action_list(update, context, args)
        elif action == LMCB.ACTION_METHOD:
            await self._show_methodology(update, context, args)
        elif action == LMCB.ACTION_REFRESH:
            await self._refresh_summary(update, context, args)
        else:
            await query.answer(f"LM Action: {action}. (В разработке)")

    async def _show_summary(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        args: List[Any],
        *,
        skip_answer: bool = False,
    ):
        """Отображение сводки по конкретному звонку."""
        query = update.callback_query
        
        try:
            # 1. Защищенный парсинг аргументов
            if not args:
                h_id_raw = "last"
            else:
                h_id_raw = args[0]
            
            if h_id_raw == "last":
                h_id = context.user_data.get("lm:last_history_id")
            else:
                try:
                    h_id = int(h_id_raw)
                except (ValueError, TypeError):
                    logger.error(f"[LM] Invalid history_id: {h_id_raw}")
                    await query.answer("Некорректный ID звонка", show_alert=True)
                    return
            
            if not h_id:
                await query.answer("ID звонка не найден в сессии. Попробуйте найти звонок заново.", show_alert=True)
                return

            context.user_data["lm:last_history_id"] = h_id
            
            # 2. Получаем данные с обработкой ошибок БД
            try:
                metrics = await self.repo.get_lm_values_by_call(h_id)
                if not metrics and self.lm_service:
                    await self._calculate_on_demand(h_id)
                    metrics = await self.repo.get_lm_values_by_call(h_id)
                
                # Загружаем расширенную инфо для證據 (evidence)
                h_rec, s_rec = await self.repo.get_call_records_for_lm(h_id)
                call_info = {**(h_rec or {}), **(s_rec or {})}
                
            except Exception as e:
                logger.error(f"[LM] Database error for history_id={h_id}: {e}")
                await query.answer("Ошибка базы данных", show_alert=True)
                return
            
            # Извлекаем action_context если есть (кто вызвал это окно)
            action_context = args[1] if len(args) > 1 else context.user_data.get("lm:last_action_context")
            if action_context and action_context != "none":
                context.user_data["lm:last_action_context"] = action_context
            
            # Преобразуем список в словарь для удобства экрана
            metrics_dict = {m['metric_code']: m for m in metrics} if metrics else {}
            
            # 3. Рендерим и отправляем экран
            try:
                period_days = context.user_data.get("lm:last_period_days")
                screen = render_lm_summary_screen(
                    h_id, 
                    metrics_dict, 
                    call_info, 
                    action_context=action_context,
                    period_days=period_days
                )
                await query.edit_message_text(
                    text=screen.text,
                    reply_markup=screen.markup,
                    parse_mode=screen.parse_mode
                )
                if not skip_answer:
                    await query.answer()
            except Exception as e:
                logger.exception(f"[LM] Render error for history_id={h_id}: {e}")
                await query.answer("Ошибка отображения сводки", show_alert=True)
            
        except Exception as e:
            logger.exception(f"Unexpected error in _show_summary: {e}")
            await query.answer("Произошла непредвиденная ошибка")

    async def _refresh_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: List[Any]):
        """Пересчет метрик для конкретного звонка по кнопке 'Обновить'."""
        query = update.callback_query
        if not self.lm_service:
            await query.answer("Пересчет недоступен в этой среде.", show_alert=True)
            return
        if not args:
            await query.answer("Не указан звонок для обновления.", show_alert=True)
            return
        try:
            history_id = int(args[0])
        except (TypeError, ValueError):
            await query.answer("Некорректный ID звонка.", show_alert=True)
            return
        action_context = args[1] if len(args) > 1 else context.user_data.get("lm:last_action_context") or "none"
        await self._calculate_on_demand(history_id)
        await self._show_summary(update, context, [history_id, action_context], skip_answer=True)
        await query.answer("Метрики обновлены.", show_alert=False)

    async def _show_action_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: List[Any]):
        """Отображение списков действий."""
        query = update.callback_query
        
        try:
            # 1. Защищенный парсинг
            action_type = str(args[0]) if len(args) > 0 else "followup"
            try:
                page = int(args[1]) if len(args) > 1 else 0
            except (ValueError, TypeError):
                page = 0
            
            # 2. Получение данных
            context.user_data["lm:last_action_context"] = action_type
            period_days = context.user_data.get("lm:last_period_days")
            try:
                period_days_int = int(period_days)
            except (TypeError, ValueError):
                period_days_int = 7
            if period_days_int <= 0:
                period_days_int = 7
            reference_ts = context.user_data.get("lm:last_period_reference")
            reference_dt: Optional[datetime] = None
            if isinstance(reference_ts, (int, float)):
                try:
                    reference_dt = datetime.fromtimestamp(reference_ts)
                except (OSError, ValueError):
                    reference_dt = None
            period_start, period_end = calculate_period_bounds(period_days_int, reference=reference_dt)
            try:
                items = await self.repo.get_action_list(
                    action_type,
                    limit=10,
                    offset=page*10,
                    start_date=period_start,
                    end_date=period_end,
                )
                total = await self.repo.get_action_count(
                    action_type,
                    start_date=period_start,
                    end_date=period_end,
                )
            except Exception as e:
                logger.error(f"[LM] DB error in action_list {action_type}: {e}")
                await query.answer("Ошибка БД при загрузке списка", show_alert=True)
                return
            
            # 3. Рендеринг и отправка
            try:
                period_days = period_days_int
                screen = render_lm_action_list_screen(action_type, items, page, total, period_days=period_days)
                await query.edit_message_text(
                    text=screen.text,
                    reply_markup=screen.markup,
                    parse_mode=screen.parse_mode
                )
                await query.answer()
            except Exception as e:
                logger.exception(f"[LM] Render error for action_list {action_type}: {e}")
                await query.answer("Ошибка отображения списка", show_alert=True)
            
        except Exception as e:
            logger.exception(f"Unexpected error in _show_action_list: {e}")
            await query.answer("Ошибка при открытии списка")
    
    async def _show_methodology(self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: List[Any]):
        """Отображение методики расчёта метрик."""
        query = update.callback_query
        origin = args[0] if args else "summary"
        payload = args[1:] if len(args) > 1 else []
        back_callback: Optional[str] = None

        if origin == "summary":
            history_arg = payload[0] if payload else context.user_data.get("lm:last_history_id")
            action_context = payload[1] if len(payload) > 1 else context.user_data.get("lm:last_action_context")
            if action_context in (None, "", "none"):
                action_context = context.user_data.get("lm:last_action_context")
            if history_arg == "last":
                history_id = context.user_data.get("lm:last_history_id")
            else:
                try:
                    history_id = int(history_arg) if history_arg is not None else context.user_data.get("lm:last_history_id")
                except (TypeError, ValueError):
                    history_id = context.user_data.get("lm:last_history_id")
            back_callback = LMCB.create(
                LMCB.ACTION_SUMMARY,
                history_id or "last",
                action_context or "",
            )
        elif origin == "period":
            period_days = payload[0] if payload else context.user_data.get("lm:last_period_days")
            back_callback = AdminCB.create(AdminCB.LM_MENU, AdminCB.lm_SUM, period_days or "")
        else:
            back_callback = LMCB.create(LMCB.ACTION_SUMMARY, "last")

        try:
            screen = render_lm_methodology_screen(back_callback)
        except Exception as exc:
            logger.exception("Ошибка подготовки методики: %s", exc)
            await query.answer("Не удалось открыть методику расчёта", show_alert=True)
            return

        try:
            await query.edit_message_text(
                text=screen.text,
                reply_markup=screen.markup,
                parse_mode=screen.parse_mode,
            )
            await query.answer()
        except TelegramError as tg_error:
            logger.warning("Telegram отказал в редактировании методики: %s", tg_error)
            try:
                if query.message:
                    await query.message.reply_text(
                        screen.text,
                        reply_markup=screen.markup,
                        parse_mode=screen.parse_mode,
                    )
                    await query.answer()
                else:
                    raise tg_error
            except Exception as exc:
                logger.exception("Fallback отправка методики не удалась: %s", exc)
                await query.answer("Не удалось открыть методику расчёта", show_alert=True)
        except Exception as exc:
            logger.exception("Ошибка рендера методики: %s", exc)
            await query.answer("Не удалось открыть методику расчёта", show_alert=True)

    @log_async_exceptions
    async def handle_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текстового ввода для LM (placeholder)."""
        user_data = context.user_data
        mode = user_data.get("lm:mode")
        if not mode:
            return False
            
        if update.message.text.lower() in ("отмена", "exit", "/cancel"):
            user_data.pop("lm:mode", None)
            await update.message.reply_text("Ввод для LM отменен.")
            return True
            
        return False

    async def _calculate_on_demand(self, history_id: int) -> None:
        if not self.lm_service:
            return
        try:
            h_rec, s_rec = await self.repo.get_call_records_for_lm(history_id)
        except Exception as exc:
            logger.error("[LM] Ошибка загрузки данных для on-demand расчета %s: %s", history_id, exc)
            return
        if not h_rec:
            logger.warning("[LM] Нет call_history для on-demand расчета history_id=%s", history_id)
            return
        try:
            await self.lm_service.calculate_all_metrics(history_id, h_rec, s_rec, calc_source="on_demand")
        except Exception as exc:
            logger.error("[LM] On-demand расчет не удался для history_id=%s: %s", history_id, exc, exc_info=True)

def register_admin_lm_handlers(application: Application, repo: LMRepository, permissions=None, lm_service: Optional[LMService] = None):
    """Регистрирует хендлеры LM."""
    handler = LMHandlers(repo, permissions, lm_service)
    
    # Регистрация callback-хендлера с паттерном lm:
    # Важно: он должен идти ПЕРЕД общим callback-хендлером, если тот слишком жадный,
    # либо иметь четкий паттерн. В main.py мы используем паттерн ^lm:
    application.add_handler(
        CallbackQueryHandler(handler.handle_callback, pattern=r"^lm:")
    )
    
    # Сохраняем ссылку в bot_data для доступа из других мест если нужно
    application.bot_data["lm_handler"] = handler
    application.bot_data["lm_repository"] = repo
    logger.info("LM Metrics handlers registered successfully")
