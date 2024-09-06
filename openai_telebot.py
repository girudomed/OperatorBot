import os
import telebot
from gtts import gTTS
from admin_utils import log_user, log_request, log_admin, get_logged_users, get_logged_admins, remove_user
from logger_utils import setup_logging
from report_generator import ReportGenerator

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


# Настройка логирования
logger = setup_logging()

# Токен вашего Telegram бота и OpenAI API (из переменных окружения или напрямую)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "your_telegram_token_here")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your_openai_key_here")

# Инициализация OpenAI клиента и ReportGenerator
report_generator = ReportGenerator(model="gpt-4o-mini")

# Создаем экземпляр бота
bot = telebot.TeleBot(TELEGRAM_TOKEN)

# Команда /start
@bot.message_handler(commands=['start'])
def send_welcome(message):
    log_user(message.from_user)
    bot.reply_to(message, "Привет! Я ваш Telegram бот с поддержкой OpenAI. Напишите /help для получения дополнительной информации.")

# Команда /help
@bot.message_handler(commands=['help'])
def send_help(message):
    help_message = "Команды бота:\n/start - запустить бота\n/help - получить помощь\n/ask - задать вопрос модели GPT-4o-mini"
    if message.from_user.username in get_logged_admins():
        help_message += "\n/remove_user [username] - удалить пользователя\n/add_admin [username] - добавить администратора"
    bot.reply_to(message, help_message)

# Команда /ask
@bot.message_handler(commands=['ask'])
def ask(message):
    log_request(message.from_user, message.text)
    question = message.text[len('/ask '):].strip()
    if not question:
        bot.reply_to(message, "Пожалуйста, укажите вопрос после команды /ask.")
        return
    try:
        recommendations = report_generator.generate_recommendations(question)
        bot.reply_to(message, recommendations)
    except Exception as e:
        bot.reply_to(message, f"Произошла ошибка: {e}")
        logger.error(f"Ошибка при генерации рекомендаций: {e}")

# Команда для администрирования
@bot.message_handler(commands=['admin'])
def admin_command(message):
    if message.from_user.username in get_logged_admins():
        bot.reply_to(message, "Добро пожаловать в админ-панель! Используйте команды /remove_user [username] для удаления пользователей и /add_admin [username] для добавления администраторов.")
    else:
        bot.reply_to(message, "На какой IDE был написан этот бот?")

# Проверка ответа для доступа к админ-панели
@bot.message_handler(func=lambda message: message.text and message.text.lower() == "vs")
def authenticate_admin(message):
    log_admin(message.from_user)
    bot.reply_to(message, "Правильно! Теперь у вас есть доступ к админ-панели. Используйте команду /admin для входа.")

# Обрабатываем текстовые сообщения (эхо-бот)
@bot.message_handler(func=lambda message: True)
def echo_all(message):
    log_request(message.from_user, message.text)
    try:
        recommendations = report_generator.generate_recommendations(message.text)
        
        # Преобразование текста в голос
        tts = gTTS(text=recommendations, lang='ru')
        temp_file = "response.mp3"
        tts.save(temp_file)
        
        # Отправка голосового сообщения
        with open(temp_file, "rb") as voice:
            bot.send_voice(message.chat.id, voice)
        
        # Удаление временного файла
        os.remove(temp_file)
        
        # Отправка текстового ответа
        bot.reply_to(message, recommendations)
    except Exception as e:
        bot.reply_to(message, f"Произошла ошибка: {e}")
        logger.error(f"Ошибка при обработке сообщения: {e}")

# Запуск бота
if __name__ == "__main__":
    bot.polling(non_stop=True)

    # Просмотр подключенных пользователей
    users = get_logged_users()
    logger.info("Подключенные пользователи:")
    for user in users:
        logger.info(user)
