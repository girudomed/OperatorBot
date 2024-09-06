from telegram import KeyboardButton, ReplyKeyboardMarkup

from logger_utils import setup_logging

logger = setup_logging()

def some_function():
    logger.info("Функция some_function начала работу.")
    # Логика функции
    try:
        # Некоторый код
        logger.info("Успешное выполнение.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


# Основная клавиатура в зависимости от роли пользователя
def create_kb_for_role(role: str, home_page=False):
    """
    Создает основную клавиатуру на основе роли пользователя.
    """
    # Начальная клавиатура для всех пользователей
    kb_list = [
        [KeyboardButton(text="👤 Мой профиль")],
        [KeyboardButton(text="📊 Запросить текущую статистику")]
    ]
    
    if home_page:
        kb_list = [
            [KeyboardButton(text="🔙 Назад")],
            [KeyboardButton(text="📊 Запросить текущую статистику")]
        ]

    # Добавляем кнопки на основе разрешений роли
    if role == "Admin" or role == "SuperAdmin":
        kb_list.append([KeyboardButton(text="⚙️ Админ панель")])
    if role in ["Marketing Director", "SuperAdmin"]:
        kb_list.append([KeyboardButton(text="📊 Управление отчетами")])
    if role == "Head of Registry":
        kb_list.append([KeyboardButton(text="📋 Отчеты по операторам")])
    if role == "Operator":
        kb_list.append([KeyboardButton(text="📄 Просмотр отчетов")])

    return ReplyKeyboardMarkup(
        keyboard=kb_list,
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Воспользуйтесь меню:"
    )

# Клавиатура по умолчанию, если роль не найдена
def default_kb():
    """
    Возвращает клавиатуру по умолчанию.
    """
    kb_list = [
        [KeyboardButton(text="👤 Мой профиль")],
        [KeyboardButton(text="📊 Запросить текущую статистику")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=kb_list,
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Воспользуйтесь меню:"
    )
