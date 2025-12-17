from telegram.ext import ContextTypes

# Ключи, используемые в разных модулях
# call_lookup.py
CALL_LOOKUP_KEY = "call_lookup_pending" # + :{chat_id}
# auth.py
HELP_BUG_KEY = "help_bug_pending"
# reports.py uses "report_args" but it doesn't block text input usually.

def reset_feature_states(context: ContextTypes.DEFAULT_TYPE, chat_id: int = None):
    """
    Сбрасывает активные состояния (режимы ввода) для пользователя/чата.
    Вызывать при входе в главное меню или переключении фичи.
    """
    # 1. Сброс Call Lookup (chat_data)
    # Ключ динамический: f"{CALL_LOOKUP_KEY}:{chat_id}"
    # Если chat_id передан, чистим конкретно.
    if chat_id:
        context.chat_data.pop(f"{CALL_LOOKUP_KEY}:{chat_id}", None)
    
    # Также можно пройтись и удалить все ключи, начинающиеся с префикса, если chat_id не известен (сложнее)
    
    # 2. Сброс Help Bug (user_data)
    context.user_data.pop(HELP_BUG_KEY, None)
    
    # 3. Сброс Report Args (опционально, хотя это просто данные)
    # context.user_data.pop("report_args", None)

def check_feature_state(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> str | None:
    """Возвращает название активной фичи или None."""
    if context.chat_data.get(f"{CALL_LOOKUP_KEY}:{chat_id}"):
        return "call_lookup"
    if context.user_data.get(HELP_BUG_KEY):
        return "help_bug"
    return None
