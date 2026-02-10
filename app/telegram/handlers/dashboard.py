# Файл: app/telegram/handlers/dashboard.py

"""
Telegram handlers для Live Dashboard операторов.
Отображает метрики в реальном времени с возможностью переключения периодов.
Полное логирование всех действий пользователя и бизнес-логики.
"""

from __future__ import annotations

from typing import List, Optional, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler
import traceback

from app.db.manager import DatabaseManager
from app.db.repositories.analytics import AnalyticsRepository
from app.db.repositories.users import UserRepository
from app.services.dashboard_cache import DashboardCacheService
from app.logging_config import get_watchdog_logger
from app.utils.rate_limit import rate_limit_hit
from app.core.roles import DEFAULT_ROLE_ID

logger = get_watchdog_logger(__name__)


class DashboardHandler:
    """Handler для live dashboard с кешированием и детальным логированием."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.analytics_repo = AnalyticsRepository(db_manager)
        self.user_repo = UserRepository(db_manager)
        self.cache_service = DashboardCacheService(db_manager)
        self._max_aggregated = 30
        self._rate_limit_seconds = 2.0
        self._busy_key = "dashboard_busy"
    
    async def dashboard_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /dashboard - показывает главное меню дашборда.
        """
        try:
            user_id = update.effective_user.id
            user_name = update.effective_user.full_name
            
            logger.info(f"[DASHBOARD] Command received from user_id={user_id}, name={user_name}")
            
            # Получаем роль пользователя для проверки прав
            user_record = await self.user_repo.get_user_by_telegram_id(user_id)
            
            if not user_record:
                logger.warning(f"[DASHBOARD] Unregistered user {user_id} attempted to access dashboard")
                await update.message.reply_text(
                    "❌ Вы не зарегистрированы в системе.\n"
                    "Используйте /start для регистрации."
                )
                return
            
            role_id = int(user_record.get('role_id') or DEFAULT_ROLE_ID)
            operator_name = user_record.get('operator_name')
            
            logger.info(
                f"[DASHBOARD] User authorized: user_id={user_id}, role_id={role_id}, "
                f"operator_name={operator_name}"
            )
            
            is_operator = role_id == DEFAULT_ROLE_ID

            # Формируем клавиатуру в зависимости от роли
            keyboard = []

            if operator_name:
                keyboard.append([
                    InlineKeyboardButton(
                        "👤 Моя статистика",
                        callback_data=self._build_callback("my", "day")
                    )
                ])
                logger.debug(
                    f"[DASHBOARD] Added personal dashboard button for {operator_name}"
                )

            if not is_operator:
                keyboard.append([
                    InlineKeyboardButton(
                        "📊 Сводка по всем",
                        callback_data="dash_all_day"
                    )
                ])
                keyboard.append([
                    InlineKeyboardButton(
                        "🔍 Другой оператор",
                        callback_data="dash_select_operator"
                    )
                ])
                logger.debug(f"[DASHBOARD] Added leadership buttons for role_id={role_id}")

            if not keyboard:
                if is_operator:
                    await update.message.reply_text(
                        "⚠️ Персональная статистика недоступна: оператор не привязан. "
                        "Обратитесь к администратору, чтобы закрепить ваш операторский ID."
                    )
                    logger.warning(
                        "[DASHBOARD] Operator %s has no operator binding", user_id
                    )
                else:
                    logger.warning(
                        f"[DASHBOARD] User {user_id} with role_id={role_id} has no dashboard entries"
                    )
                    await update.message.reply_text(
                        "⚠️ Нет доступных разделов дашборда. Обратитесь к разработчику."
                    )
                return

            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "📊 <b>Аналитика и Статистика</b>\n\n"
                "Выберите что вы хотите посмотреть:",
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            
            logger.info(f"[DASHBOARD] Menu displayed successfully for user_id={user_id}")
        
        except Exception as e:
            logger.error(
                f"[DASHBOARD] Error in dashboard_command for user {update.effective_user.id}: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            await update.message.reply_text(
                "❌ Произошла ошибка при загрузке дашборда.\n"
                "Попробуйте позже или обратитесь к администратору."
            )
    
    async def dashboard_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка callback кнопок дашборда."""
        query = update.callback_query
        if not await self.safe_answer_callback(query):
            return
        
        try:
            data = query.data or ""
            if await self._rate_limit_callback(query, context, data):
                return
            user_id = update.effective_user.id
            
            logger.info(f"[DASHBOARD] Callback received: user_id={user_id}, data={data}")
            
            callback_type, period = self._parse_callback_data(data)
            if callback_type is None:
                logger.warning("[DASHBOARD] Unknown callback format: %s", data)
                await self.safe_answer_callback(
                    query,
                    "Интерфейс устарел. Откройте /dashboard заново.",
                    show_alert=True,
                )
                await query.edit_message_text(
                    "⚠️ Обнаружена устаревшая клавиатура.\n"
                    "Откройте /dashboard, чтобы получить актуальное меню."
                )
                return

            if callback_type == "my":
                if not await self._acquire_guard(context, query):
                    return
                operator_name = await self._resolve_operator_name(user_id)
                if not operator_name:
                    await self.safe_answer_callback(
                        query,
                        "Оператор не привязан. Обратитесь к администратору.",
                        show_alert=True,
                    )
                    return
                logger.debug(
                    "[DASHBOARD] Personal dashboard: user_id=%s operator=%s period=%s",
                    user_id,
                    operator_name,
                    period,
                )
                try:
                    await self._show_single_dashboard(query, operator_name, period)
                finally:
                    self._release_guard(context)
            
            elif callback_type == "all":
                if not await self._acquire_guard(context, query):
                    return
                logger.debug(f"[DASHBOARD] Aggregated dashboard: period={period}")
                try:
                    await self._show_all_operators_dashboard(query, period)
                finally:
                    self._release_guard(context)
            
            elif callback_type == "select_operator":
                # Выбор оператора
                logger.debug(f"[DASHBOARD] Operator selection requested by user_id={user_id}")
                await self._show_operator_selection(query)
            
            elif callback_type == "refresh_my":
                if not await self._acquire_guard(context, query):
                    return
                logger.info("[DASHBOARD] Refresh requested: type=my period=%s", period)
                operator_name = await self._resolve_operator_name(user_id)
                if not operator_name:
                    await self.safe_answer_callback(
                        query,
                        "Оператор не привязан. Обратитесь к администратору.",
                        show_alert=True,
                    )
                    return
                await self.cache_service.invalidate_cache(operator_name, period)
                try:
                    await self._show_single_dashboard(query, operator_name, period, refresh=True)
                finally:
                    self._release_guard(context)
            
            elif callback_type == "refresh_all":
                if not await self._acquire_guard(context, query):
                    return
                logger.info("[DASHBOARD] Refresh requested: type=all period=%s", period)
                await self.cache_service.invalidate_cache(period_type=period)
                try:
                    await self._show_all_operators_dashboard(query, period, refresh=True)
                finally:
                    self._release_guard(context)
            
            elif callback_type == "period_my":
                if not await self._acquire_guard(context, query):
                    return
                logger.info("[DASHBOARD] Period change: type=my new_period=%s", period)
                operator_name = await self._resolve_operator_name(user_id)
                if not operator_name:
                    await self.safe_answer_callback(
                        query,
                        "Оператор не привязан. Обратитесь к администратору.",
                        show_alert=True,
                    )
                    return
                try:
                    await self._show_single_dashboard(query, operator_name, period)
                finally:
                    self._release_guard(context)
            
            elif callback_type == "period_all":
                if not await self._acquire_guard(context, query):
                    return
                logger.info("[DASHBOARD] Period change: type=all new_period=%s", period)
                try:
                    await self._show_all_operators_dashboard(query, period)
                finally:
                    self._release_guard(context)
            
            elif callback_type == "back":
                # Возврат в главное меню
                logger.debug(f"[DASHBOARD] Back to main menu requested by user_id={user_id}")
                await query.edit_message_text(
                    "📊 Используйте /dashboard для открытия меню аналитики."
                )
        
        except Exception as e:
            logger.error(
                f"[DASHBOARD] Error in dashboard_callback for user {update.effective_user.id}, "
                f"data={query.data}: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            await query.edit_message_text(
                "❌ Произошла ошибка при обработке запроса.\n"
                "Попробуйте /dashboard снова."
            )
    
    async def _show_single_dashboard(
        self,
        query,
        operator_name: str,
        period: str = 'day',
        refresh: bool = False
    ):
        """Показать персональный дашборд оператора с кешированием."""
        try:
            logger.info(
                f"[DASHBOARD] Showing single dashboard: operator={operator_name}, "
                f"period={period}, refresh={refresh}"
            )
            timestamp = self._current_msk_time()
            
            # Пробуем получить из кеша если не refresh
            dashboard = None
            if not refresh:
                from datetime import date, timedelta
                today = date.today()
                if period == 'day':
                    date_from = today
                    date_to = today
                elif period == 'week':
                    date_from = today - timedelta(days=today.weekday())
                    date_to = today
                else:  # month
                    date_from = today.replace(day=1)
                    date_to = today
                
                dashboard = await self.cache_service.get_cached_dashboard(
                    operator_name, period, date_from, date_to
                )
                
                if dashboard:
                    logger.info(f"[DASHBOARD] Cache HIT for {operator_name} {period}")
            
            # Если нет в кеше или refresh - получаем свежие данные
            if not dashboard:
                logger.info(f"[DASHBOARD] Cache MISS or refresh - fetching fresh data")
                dashboard = await self.analytics_repo.get_live_dashboard_single(
                    operator_name,
                    period
                )
                
                # Сохраняем в кеш
                try:
                    await self.cache_service.save_dashboard_cache(dashboard)
                    logger.debug(f"[DASHBOARD] Saved to cache: {operator_name} {period}")
                except Exception as cache_error:
                    logger.warning(
                        f"[DASHBOARD] Failed to save to cache: {cache_error}"
                    )
            
            # Форматируем сообщение
            message = self._format_single_dashboard(dashboard, refresh)
            
            # Кнопки управления - человекочитаемые названия
            keyboard = [
                [
                    InlineKeyboardButton(
                        "День" + (" ◉" if period == 'day' else ""),
                        callback_data=self._build_callback("period_my", "day")
                    ),
                    InlineKeyboardButton(
                        "Неделя" + (" ◉" if period == 'week' else ""),
                        callback_data=self._build_callback("period_my", "week")
                    ),
                    InlineKeyboardButton(
                        "Месяц" + (" ◉" if period == 'month' else ""),
                        callback_data=self._build_callback("period_my", "month")
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🔄 Обновить данные",
                        callback_data=self._build_callback("refresh_my", period)
                    )
                ],
                [
                    InlineKeyboardButton("◀️ Назад в меню", callback_data="dash_back")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                message,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            
            logger.info(f"[DASHBOARD] Single dashboard displayed successfully for {operator_name}")
        
        except Exception as e:
            logger.error(
                f"[DASHBOARD] Error showing single dashboard for {operator_name}: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            await query.edit_message_text(
                f"❌ Ошибка загрузки статистики для {operator_name}.\n"
                f"Попробуйте позже или обратитесь к администратору."
            )
    
    async def _show_all_operators_dashboard(
        self,
        query,
        period: str = 'day',
        refresh: bool = False
    ):
        """Показать сводный дашборд по всем операторам."""
        try:
            logger.info(f"[DASHBOARD] Showing aggregated dashboard: period={period}, refresh={refresh}")
            
            # Получаем метрики для всех
            dashboards, total_count = await self.analytics_repo.get_live_dashboard_all_operators(
                period,
                limit=self._max_aggregated,
            )
            
            if not dashboards:
                logger.warning(f"[DASHBOARD] No data found for aggregated dashboard, period={period}")
                await query.edit_message_text(
                    "📊 Нет данных для отображения сводной статистики.\n"
                    "Возможно, в выбранном периоде не было звонков."
                )
                return
            
            logger.info(f"[DASHBOARD] Found {len(dashboards)} operators for aggregated view")
            
            # Форматируем сообщение
            message = self._format_all_dashboards(
                dashboards,
                period,
                refresh,
                total_count=total_count,
            )
            
            # Кнопки управления
            keyboard = [
                [
                    InlineKeyboardButton(
                        "День" + (" ◉" if period == 'day' else ""),
                        callback_data=self._build_callback("period_all", "day")
                    ),
                    InlineKeyboardButton(
                        "Неделя" + (" ◉" if period == 'week' else ""),
                        callback_data=self._build_callback("period_all", "week")
                    ),
                    InlineKeyboardButton(
                        "Месяц" + (" ◉" if period == 'month' else ""),
                        callback_data=self._build_callback("period_all", "month")
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🔄 Обновить данные",
                        callback_data=self._build_callback("refresh_all", period)
                    )
                ],
                [
                    InlineKeyboardButton("◀️ Назад в меню", callback_data="dash_back")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                message,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            
            logger.info(f"[DASHBOARD] Aggregated dashboard displayed successfully")
        
        except Exception as e:
            logger.error(
                f"[DASHBOARD] Error showing aggregated dashboard: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            await query.edit_message_text(
                "❌ Ошибка загрузки сводной статистики.\n"
                "Попробуйте позже."
            )
    
    async def _show_operator_selection(self, query):
        """Показать выбор оператора."""
        try:
            logger.info(f"[DASHBOARD] Showing operator selection")
            
            # TODO: Реализовать список операторов с кнопками
            # Пока заглушка
            await query.edit_message_text(
                "🔍 <b>Выбор оператора</b>\n\n"
                "Функция в разработке.\n"
                "Используйте /dashboard для возврата в меню.",
                parse_mode='HTML'
            )
            
            logger.warning("[DASHBOARD] Operator selection not yet implemented")
        
        except Exception as e:
            logger.error(
                f"[DASHBOARD] Error in operator selection: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            await query.edit_message_text("❌ Ошибка. Используйте /dashboard")
    
    def _format_single_dashboard(self, dashboard: dict, refresh: bool = False) -> str:
        """Форматирует данные дашборда для отображения."""
        try:
            timestamp = self._current_msk_time()
            period_names = {
                'day': 'за сегодня',
                'week': 'за неделю',
                'month': 'за месяц'
            }
            
            period_label = period_names.get(dashboard.get('period_type', 'day'), 'за сегодня')
            operator_name = dashboard.get('operator_name', 'Неизвестно')
            
            logger.debug(f"[DASHBOARD] Formatting dashboard for {operator_name}")
            
            # Преобразуем секунды в минуты:секунды
            def format_time(seconds):
                if not seconds:
                    return "0:00"
                mins = seconds // 60
                secs = seconds % 60
                return f"{mins}:{secs:02d}"
            
            message = f"""
📊 <b>Статистика: {operator_name}</b>
📅 Период: <b>{period_label}</b>
🕒 Обновлено: <b>{timestamp} МСК</b>
{"🔄 <i>Данные обновлены</i>" if refresh else ""}

<b>1️⃣ Общая статистика:</b>
   • Всего звонков: {dashboard.get('accepted_calls', 0)}
   • Записей на услугу: {dashboard.get('records_count', 0)}
   • Желающих записаться: {dashboard.get('wish_to_record', 0)}
   • Конверсия: <b>{dashboard.get('conversion_rate', 0)}%</b>

<b>2️⃣ Качество обслуживания:</b>
   • Средняя оценка: <b>{dashboard.get('avg_score_all', 0)}/10</b>
   • Оценка лидов: {dashboard.get('avg_score_leads', 0)}/10

<b>3️⃣ Отмены и переносы:</b>
   • Отмен: {dashboard.get('cancel_calls', 0)}
   • Переносов: {dashboard.get('reschedule_calls', 0)}
   • Доля отмен: {dashboard.get('cancel_share', 0)}%

<b>4️⃣ Время на звонки:</b>
   • Общее время: {dashboard.get('total_talk_time', 0) // 60} мин
   • Среднее (запись): {format_time(dashboard.get('avg_talk_record', 0))}
   • Среднее (навигация): {format_time(dashboard.get('avg_talk_navigation', 0))}
   • Среднее (спам): {format_time(dashboard.get('avg_talk_spam', 0))}

<b>5️⃣ Жалобы:</b>
   • Звонков с жалобами: {dashboard.get('complaint_calls', 0)}
   • Оценка жалоб: {dashboard.get('avg_score_complaint', 0)}/10
"""
            
            return message.strip()
        
        except Exception as e:
            logger.error(
                f"[DASHBOARD] Error formatting single dashboard: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            return "❌ Ошибка форматирования данных"
    
    def _format_all_dashboards(
        self,
        dashboards: List[dict],
        period: str,
        refresh: bool = False,
        *,
        total_count: Optional[int] = None,
    ) -> str:
        """Форматирует сводный дашборд всех операторов."""
        try:
            period_names = {
                'day': 'за сегодня',
                'week': 'за неделю',
                'month': 'за месяц'
            }
            
            period_label = period_names.get(period, 'за сегодня')
            timestamp = self._current_msk_time()
            
            logger.debug(f"[DASHBOARD] Formatting aggregated dashboard for {len(dashboards)} operators")
            
            message = f"""
📊 <b>Сводная статистика по всем операторам</b>
📅 Период: <b>{period_label}</b>
🕒 Обновлено: <b>{timestamp} МСК</b>
{"🔄 <i>Данные обновлены</i>" if refresh else ""}

"""
            
            # Сортируем по конверсии (лучшие сверху)
            sorted_dashboards = sorted(
                dashboards,
                key=lambda x: x.get('conversion_rate', 0),
                reverse=True
            )
            
            for i, dash in enumerate(sorted_dashboards[:10], 1):  # Топ-10
                operator_name = dash.get('operator_name', 'Неизвестно')
                calls = dash.get('accepted_calls', 0)
                records = dash.get('records_count', 0)
                conversion = dash.get('conversion_rate', 0)
                avg_score = dash.get('avg_score_all', 0)
                
                # Эмодзи по конверсии
                if conversion >= 40:
                    emoji = "🔥"
                elif conversion >= 30:
                    emoji = "✅"
                elif conversion >= 20:
                    emoji = "⚠️"
                else:
                    emoji = "❌"
                
                message += f"""
{emoji} <b>{operator_name}</b>
   Звонков: {calls} | Записей: {records} | Конверсия: <b>{conversion}%</b> | Оценка: {avg_score}/10
"""
            
            if len(sorted_dashboards) > 10:
                message += f"\n<i>... и ещё {len(sorted_dashboards) - 10} операторов</i>"
            if total_count and total_count > len(sorted_dashboards):
                message += (
                    f"\n<i>Показаны первые {len(sorted_dashboards)} из {total_count} операторов."
                    " Остальные скрыты, чтобы не перегружать БД.</i>"
                )
            
            return message.strip()
        
        except Exception as e:
            logger.error(
                f"[DASHBOARD] Error formatting aggregated dashboard: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            return "❌ Ошибка форматирования данных"
    
    def get_handlers(self):
        """Возвращает список handlers для регистрации."""
        return [
            CommandHandler('dashboard', self.dashboard_command),
            CallbackQueryHandler(
                self.dashboard_callback,
                pattern=r'^dash(?:_|:)'
            )
        ]
    
    def _current_msk_time(self) -> str:
        return datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d.%m %H:%M")
    
    async def _acquire_guard(self, context: ContextTypes.DEFAULT_TYPE, query) -> bool:
        if context.user_data.get(self._busy_key):
            await self.safe_answer_callback(
                query,
                "Запрос уже выполняется. Подождите пару секунд.",
                show_alert=True,
            )
            return False
        context.user_data[self._busy_key] = True
        return True

    def _release_guard(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data.pop(self._busy_key, None)

    async def _rate_limit_callback(self, query, context, data: str) -> bool:
        user = query.from_user if query else None
        if not user:
            return False
        key_suffix = data.split('_', 1)[0] if data else 'dash'
        key = f"dashboard:{key_suffix}"
        if rate_limit_hit(
            context.application.bot_data,
            user.id,
            key,
            cooldown_seconds=self._rate_limit_seconds,
        ):
            await self.safe_answer_callback(
                query,
                "Слишком часто обновляете статистику. Подождите пару секунд.",
                show_alert=True,
            )
            return True
        return False

    async def safe_answer_callback(
        self,
        query,
        text: Optional[str] = None,
        *,
        show_alert: bool = False,
    ) -> bool:
        """Подтверждает callback без падения на stale/invalid query."""
        try:
            if text is None:
                await query.answer()
            else:
                await query.answer(text, show_alert=show_alert)
            return True
        except BadRequest as exc:
            error_text = str(exc).lower()
            if "query is too old" in error_text or "query id is invalid" in error_text:
                logger.warning("[DASHBOARD] Stale callback query ignored: %s", exc)
                return False
            logger.warning("[DASHBOARD] Failed to answer callback: %s", exc)
            return False
        except TelegramError as exc:
            logger.warning("[DASHBOARD] Telegram callback answer error: %s", exc)
            return False

    async def _resolve_operator_name(self, user_id: int) -> Optional[str]:
        user_record = await self.user_repo.get_user_by_telegram_id(user_id)
        if not user_record:
            return None
        return user_record.get("operator_name")

    @staticmethod
    def _build_callback(action: str, period: Optional[str] = None) -> str:
        if period:
            return f"dash:{action}:{period}"
        return f"dash:{action}"

    @staticmethod
    def _parse_callback_data(data: str) -> Tuple[Optional[str], str]:
        """
        Поддерживает новый формат `dash:<action>:<period>` и legacy `dash_*`.
        Возвращает (`callback_type`, `period`).
        """
        if not data:
            return None, "day"

        if data.startswith("dash:"):
            parts = data.split(":")
            if len(parts) == 2 and parts[1] in {"back", "select_operator"}:
                return parts[1], "day"
            if len(parts) == 3:
                action, period = parts[1], parts[2]
                if action in {"my", "all", "period_my", "period_all", "refresh_my", "refresh_all"}:
                    return action, period
            return None, "day"

        if data == "dash_back":
            return "back", "day"
        if data == "dash_select_operator":
            return "select_operator", "day"
        if data.startswith("dash_my_"):
            parts = data.split("_")
            if len(parts) >= 3:
                return "my", parts[2]
        if data.startswith("dash_all_"):
            parts = data.split("_")
            if len(parts) >= 3:
                return "all", parts[2]
        if data.startswith("dash_refresh_my_"):
            parts = data.split("_")
            if len(parts) >= 4:
                return "refresh_my", parts[3]
        if data.startswith("dash_refresh_all_"):
            parts = data.split("_")
            if len(parts) >= 4:
                return "refresh_all", parts[3]
        if data.startswith("dash_period_my_"):
            parts = data.split("_")
            if len(parts) >= 4:
                return "period_my", parts[3]
        if data.startswith("dash_period_all_"):
            parts = data.split("_")
            if len(parts) >= 4:
                return "period_all", parts[3]
        return None, "day"
