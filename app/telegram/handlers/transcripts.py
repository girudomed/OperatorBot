"""
Telegram handler для получения расшифровок звонков.
Операторы видят только свои звонки, админы - все.
"""

from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.services.permissions import PermissionChecker, require_role
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user

logger = get_watchdog_logger(__name__)


class TranscriptHandler:
    """Handler для работы с расшифровками звонков."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.user_repo = UserRepository(db_manager)
        self.permission_checker = PermissionChecker(db_manager)
    
    async def transcript_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /transcript <call_id или history_id>
        
        Возвращает расшифровку звонка с метаданными:
        - Дата и время
        - Телефон
        - History ID
        - Оценка
        - Транскрипт
        """
        user_id = update.effective_user.id
        
        # Проверяем доступ
        has_access = await self.permission_checker.can_view_transcripts(user_id)
        if not has_access:
            logger.warning(
                "Пользователь %s запросил расшифровку без доступа",
                describe_user(update.effective_user),
            )
            await update.message.reply_text(
                "🔒 У вас нет доступа к расшифровкам звонков."
            )
            return
        
        # Получаем параметры
        if not context.args:
            await update.message.reply_text(
                "❌ Использование: /transcript <call_id или history_id>\n\n"
                "Пример: /transcript 12345"
            )
            return
        
        call_identifier = context.args[0]
        
        try:
            call_id = int(call_identifier)
        except ValueError as exc:
            logger.warning(
                "Некорректный ID расшифровки '%s' от %s: %s",
                call_identifier,
                describe_user(update.effective_user),
                exc,
                exc_info=True,
            )
            await update.message.reply_text(
                "❌ ID звонка должен быть числом."
            )
            return
        
        # Получаем данные звонка
        call_data = await self._get_call_data(call_id)
        
        if not call_data:
            await update.message.reply_text(
                f"❌ Звонок с ID {call_id} не найден."
            )
            return
        
        # Проверяем права на просмотр этого звонка
        can_view = await self._can_view_this_call(user_id, call_data)
        
        if not can_view:
            await update.message.reply_text(
                "🔒 У вас нет прав для просмотра этого звонка.\n"
                "Вы можете просматривать только свои звонки."
            )
            return
        
        # Форматируем и отправляем
        message = self._format_transcript(call_data)
        
        # Telegram имеет лимит 4096 символов
        if len(message) > 4000:
            # Отправляем частями
            await update.message.reply_text(message[:4000], parse_mode='HTML')
            await update.message.reply_text(
                f"<b>Продолжение транскрипта:</b>\n\n{message[4000:]}",
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(message, parse_mode='HTML')
    
    async def _get_call_data(self, call_id: int) -> dict:
        """Получить данные звонка по ID."""
        # Пробуем найти по id в call_scores
        query = """
        SELECT 
            cs.id,
            cs.history_id,
            cs.call_date,
            cs.caller_number,
            cs.called_number,
            cs.call_score,
            cs.transcript,
            cs.call_category,
            cs.outcome,
            cs.talk_duration,
            cs.caller_info,
            cs.called_info,
            cs.call_type as context_type
        FROM call_scores cs
        WHERE cs.id = %s OR cs.history_id = %s
        LIMIT 1
        """
        
        result = await self.db_manager.execute_query(
            query,
            (call_id, call_id),
            fetchone=True
        )
        
        return result
    
    async def _can_view_this_call(self, telegram_id: int, call_data: dict) -> bool:
        """Проверить, может ли пользователь видеть этот звонок."""
        # Админы видят все
        can_view_all = await self.permission_checker.can_view_other_transcripts(telegram_id)
        if can_view_all:
            return True
        
        # Операторы видят только свои
        user = await self.user_repo.get_user_by_telegram_id(telegram_id)
        if not user:
            return False
        
        operator_name = user.get('operator_name')
        extension = user.get('extension')
        
        if not operator_name and not extension:
            return False
        
        # Проверяем, относится ли звонок к этому оператору
        caller_info = call_data.get('caller_info', '')
        called_info = call_data.get('called_info', '')
        context_type = call_data.get('context_type', '')
        
        # Для входящих проверяем called_info
        if context_type == 'входящий':
            if operator_name and operator_name in called_info:
                return True
            if extension and extension in called_info:
                return True
        # Для исходящих проверяем caller_info
        else:
            if operator_name and operator_name in caller_info:
                return True
            if extension and extension in caller_info:
                return True
        
        return False
    
    def _format_transcript(self, call_data: dict) -> str:
        """Форматировать данные звонка для отображения."""
        call_id = call_data.get('id', 'Н/Д')
        history_id = call_data.get('history_id', 'Н/Д')
        call_date = call_data.get('call_date', 'Н/Д')
        caller_number = call_data.get('caller_number', 'Н/Д')
        called_number = call_data.get('called_number', 'Н/Д')
        call_score = call_data.get('call_score', 'Н/Д')
        transcript = call_data.get('transcript', 'Транскрипт отсутствует')
        call_category = call_data.get('call_category', 'Н/Д')
        outcome = call_data.get('outcome', 'Н/Д')
        talk_duration = call_data.get('talk_duration', 0)
        
        # Форматируем длительность
        if talk_duration:
            minutes = talk_duration // 60
            seconds = talk_duration % 60
            duration_str = f"{minutes}:{seconds:02d}"
        else:
            duration_str = "0:00"
        
        # Маскируем номер телефона (показываем первые 3 и последние 2 цифры)
        if caller_number and len(caller_number) > 5:
            masked_number = f"{caller_number[:3]}*****{caller_number[-2:]}"
        else:
            masked_number = caller_number
        
        message = f"""
📞 <b>Расшифровка звонка #{call_id}</b>

🆔 History ID: {history_id}
📅 Дата: {call_date}
📱 Телефон: {masked_number}
📞 Принял: {called_number}
⏱ Длительность: {duration_str}
⭐️ Оценка: {call_score}/10
📂 Категория: {call_category}
🎯 Результат: {outcome}

━━━━━━━━━━━━━━━━━━━━

📝 <b>Транскрипт:</b>

{transcript}
"""
        
        return message.strip()
    
    def get_handlers(self):
        """Возвращает список handlers для регистрации."""
        return [
            CommandHandler('transcript', self.transcript_command)
        ]
