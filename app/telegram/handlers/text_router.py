import logging
from telegram import Update
from telegram.ext import ContextTypes, MessageHandler, filters

from app.telegram.handlers.auth import help_bug_message
# Note: we need to import permissions manager type or just use it from bot_data

logger = logging.getLogger(__name__)

class TextRouter:
    """
    Центральный маршрутизатор текстовых сообщений (group=10).
    Проверяет активный режим (state) и направляет сообщение в соответствующий модуль.
    """
    
    @staticmethod
    async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # 1. Проверяем Call Lookup (хранит состояние в chat_data[call_lookup_pending:chat_id])
        # Ключ определен в call_lookup.py как "call_lookup_pending"
        # Но конкретный ключ: f"call_lookup_pending:{chat_id}"
        # Проверим наличие ключа.
        
        # Лучше было бы экспортировать константы, но пока хардкодим по логике call_lookup.py
        call_lookup_key = f"call_lookup_pending:{chat_id}"
        if context.chat_data.get(call_lookup_key):
            handler = context.application.bot_data.get("call_lookup_handler")
            if handler:
                await handler.handle_phone_input(update, context)
                return

        # 2. Проверяем Help Bug (хранит состояние в user_data["help_bug_pending"])
        # Ключ "help_bug_pending" из auth.py
        if context.user_data.get("help_bug_pending"):
            permissions = context.application.bot_data.get("permissions_manager")
            if permissions:
                await help_bug_message(update, context, permissions)
                return
                
        # 3. Здесь можно добавить другие проверки (например, admin reply)
        
        # Если ни один режим не активен - игнорируем (или логируем для отладки)
        # logger.debug("Text received but no mode active: %s", update.effective_message.text)

    @staticmethod
    def get_handler():
        """Возвращает сконфигурированный MessageHandler."""
        return MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            TextRouter.handle_text,
            group=10,
            block=False
        )
