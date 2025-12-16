# Файл: app/telegram/handlers/sync_analytics.py

"""
Admin command для синхронизации call_scores → call_analytics.

Доступен только для SuperAdmin/Dev.
"""

import traceback
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

from app.db.manager import DatabaseManager
from app.services.call_analytics_sync import CallAnalyticsSyncService
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class SyncAnalyticsHandler:
    """Handler для команды /sync_analytics."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.sync_service = CallAnalyticsSyncService(db_manager)
        self.permissions = PermissionsManager(db_manager)
    
    async def sync_analytics_command(
        self, 
        update: Update, 
        context: ContextTypes.DEFAULT_TYPE
    ):
        """
        Команда /sync_analytics - синхронизация call_scores → call_analytics.
        
        Usage:
            /sync_analytics - инкрементальная (за последние 2 дня)
            /sync_analytics full - полная синхронизация
            /sync_analytics status - текущий статус
        """
        user_id = update.effective_user.id
        username = update.effective_user.username
        
        logger.info(f"[SYNC] Command from user {user_id} ({username})")
        
        # Проверка прав (только SuperAdmin/Dev)
        if not (self.permissions.is_supreme_admin(user_id, username) or 
                self.permissions.is_dev_admin(user_id, username)):
            logger.warning(f"[SYNC] Unauthorized access attempt by {user_id}")
            await update.message.reply_text(
                "❌ У вас нет прав для этой команды.\n"
                "Доступна только для SuperAdmin/Dev."
            )
            return
        
        # Получить аргумент команды
        args = context.args or []
        mode = args[0].lower() if args else 'incremental'
        
        try:
            if mode == 'status':
                # Показать статус синхронизации
                await self._show_status(update)
            elif mode == 'full':
                # Полная синхронизация
                await self._run_full_sync(update)
            else:
                # Инкрементальная синхронизация
                await self._run_incremental_sync(update)
                
        except Exception as e:
            logger.error(f"[SYNC] Error: {e}\n{traceback.format_exc()}")
            await update.message.reply_text(
                f"❌ Ошибка при синхронизации:\n{str(e)}"
            )
    
    async def _show_status(self, update: Update):
        """Показать текущий статус синхронизации."""
        await update.message.reply_text("⏳ Проверяю статус...")
        
        status = await self.sync_service.get_sync_status()
        
        if not status:
            await update.message.reply_text("❌ Не удалось получить статус")
            return
        
        cs_count = status.get('call_scores_count', 0)
        ca_count = status.get('call_analytics_count', 0)
        missing = status.get('missing_count', 0)
        percent = status.get('sync_percentage', 0)
        last_sync = status.get('last_sync')
        is_synced = status.get('is_synced', False)
        
        icon = "✅" if is_synced else "⚠️"
        
        text = f"""
{icon} **Статус Синхронизации**

**call_scores:** {cs_count:,} записей
**call_analytics:** {ca_count:,} записей
**Не синхронизировано:** {missing:,}
**Процент:** {percent:.1f}%

**Последняя синхронизация:**
{last_sync or 'Никогда'}

**Статус:** {'Синхронизировано' if is_synced else 'Требуется синхронизац��я'}
"""
        
        await update.message.reply_text(text, parse_mode='Markdown')
        
        logger.info(
            f"[SYNC] Status shown: {ca_count}/{cs_count} ({percent:.1f}%), "
            f"missing={missing}"
        )
    
    async def _run_full_sync(self, update: Update):
        """Запустить полную синхронизацию."""
        await update.message.reply_text(
            "🔄 Запускаю **полную** синхронизацию...\n"
            "Это может занять несколько минут.",
            parse_mode='Markdown'
        )
        
        logger.info("[SYNC] Starting FULL sync")
        
        stats = await self.sync_service.sync_all(batch_size=1000)
        
        inserted = stats.get('inserted', 0)
        skipped = stats.get('skipped', 0)
        errors = stats.get('errors', 0)
        duration = stats.get('duration', 0)
        
        icon = "✅" if errors == 0 else "⚠️"
        
        text = f"""
{icon} **Полная синхронизация завершена**

**Добавлено:** {inserted:,} записей
**Пропущено:** {skipped:,}
**Ошибок:** {errors}
**Время:** {duration:.1f}с

{f'❌ Обнаружены ошибки!' if errors > 0 else '✅ Синхронизация успешна'}
"""
        
        await update.message.reply_text(text, parse_mode='Markdown')
        
        logger.info(
            f"[SYNC] Full sync completed: inserted={inserted}, errors={errors}, "
            f"duration={duration:.1f}s"
        )
    
    async def _run_incremental_sync(self, update: Update):
        """Запустить инкрементальную синхронизацию."""
        await update.message.reply_text(
            "🔄 Синхронизирую новые звонки...",
            parse_mode='Markdown'
        )
        
        logger.info("[SYNC] Starting incremental sync")
        
        stats = await self.sync_service.sync_new(batch_size=500)
        
        inserted = stats.get('inserted', 0)
        updated = stats.get('updated', 0)
        errors = stats.get('errors', 0)
        duration = stats.get('duration', 0)
        
        if inserted == 0 and errors == 0:
            await update.message.reply_text(
                "✅ Синхронизация не требуется\n"
                "Все данные актуальны."
            )
        else:
            icon = "✅" if errors == 0 else "⚠️"
            
            text = f"""
{icon} **Синхронизация завершена**

**Добавлено:** {inserted:,} новых звонков
**Время:** {duration:.1f}с

{f'❌ Обнаружены ошибки: {errors}' if errors > 0 else '✅ Успешно'}
"""
            
            await update.message.reply_text(text, parse_mode='Markdown')
        
        logger.info(
            f"[SYNC] Incremental sync completed: inserted={inserted}, "
            f"errors={errors}, duration={duration:.1f}s"
        )
    
    def get_handler(self):
        """Получить CommandHandler для регистрации."""
        return CommandHandler('sync_analytics', self.sync_analytics_command)
