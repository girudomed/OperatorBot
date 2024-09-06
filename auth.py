import os
import jwt
import datetime
import logging
import random
import string
from db_setup import create_async_connection, get_user_role, add_user, get_user_password
from permissions_manager import PermissionsManager
from logger_utils import setup_logging
from telegram import Update
from telegram.ext import CallbackContext, CommandHandler, ConversationHandler, MessageHandler, filters

logger = setup_logging()

# Этапы диалога для регистрации
ASK_NAME, ASK_ROLE, ASK_PASSWORD = range(3)

class AuthManager:
    SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your_default_secret_key")
    ALGORITHM = "HS256"
    
    def __init__(self):
        self.permissions_manager = PermissionsManager()

    def generate_password(self, length=8):
        """Генерация случайного пароля."""
        letters = string.ascii_letters + string.digits + string.punctuation
        return ''.join(random.choice(letters) for i in range(length))

    async def register_user(self, full_name, role):
        """
        Регистрация пользователя, генерация пароля и сохранение в базе данных.
        """
        connection = await create_async_connection()
        if not connection:
            return {"status": "error", "message": "Ошибка подключения к базе данных"}
        
        try:
            # Добавление пользователя с указанной ролью
            user_id = random.randint(100000, 999999)  # Можете заменить на реальный user_id
            username = full_name.split()[0]  # Пример: взять имя как username
            password = self.generate_password()

            # Добавляем пользователя с ролью
            await add_user(connection, user_id, username, full_name, role_name=role)
            
            logger.info(f"Пользователь {full_name} зарегистрирован с ролью {role} и паролем {password}.")
            return {"status": "success", "password": password}
        except Exception as e:
            logger.error(f"Ошибка при регистрации пользователя {full_name}: {e}")
            return {"status": "error", "message": f"Registration failed: {e}"}
        finally:
            await connection.ensure_closed()

    async def verify_password(self, user_password):
        """
        Проверка пароля, присвоенного пользователю.
        Если пароль верен, присваивается соответствующая роль и генерируется JWT-токен.
        """
        connection = await create_async_connection()
        if not connection:
            return {"status": "error", "message": "Ошибка подключения к базе данных"}
        
        try:
            # Получение user_id и role по паролю
            async with connection.cursor() as cursor:
                sql = "SELECT user_id, role_id FROM UsersTelegaBot WHERE password = %s"
                await cursor.execute(sql, (user_password,))
                user = await cursor.fetchone()
                
                if user:
                    role = await get_user_role(connection, user['user_id'])
                    logger.info(f"Пользователь с паролем {user_password} успешно найден.")
                    token = self.generate_token(user['user_id'], role)
                    return {"status": "success", "token": token}
                else:
                    logger.warning(f"Пароль {user_password} не найден в базе данных.")
                    return {"status": "error", "message": "Invalid password"}
        except Exception as e:
            logger.error(f"Ошибка при проверке пароля {user_password}: {e}")
            return {"status": "error", "message": f"Verification failed: {e}"}
        finally:
            await connection.ensure_closed()

    def generate_token(self, user_id, role):
        """
        Генерация JWT-токена для пользователя.
        """
        payload = {
            "user_id": user_id,
            "role": role,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=12)
        }
        token = jwt.encode(payload, self.SECRET_KEY, algorithm=self.ALGORITHM)
        logger.info(f"JWT-токен сгенерирован для пользователя {user_id}.")
        return token


auth_manager = AuthManager()

# Команда /start
async def start_handle(update: Update, context: CallbackContext):
    """Приветственное сообщение."""
    await update.message.reply_text(
        "Привет! Добро пожаловать. Для регистрации введите команду /register."
    )

# Команда для начала регистрации
async def register_handle(update: Update, context: CallbackContext):
    """Начало регистрации: бот запрашивает ФИО."""
    await update.message.reply_text("Введите ваше ФИО:")
    return ASK_NAME

# Обработка имени пользователя
async def ask_name_handle(update: Update, context: CallbackContext):
    """Получение ФИО пользователя."""
    context.user_data['full_name'] = update.message.text
    await update.message.reply_text("Теперь введите вашу роль (например, Operator):")
    return ASK_ROLE

# Обработка роли пользователя
async def ask_role_handle(update: Update, context: CallbackContext):
    """Получение роли пользователя."""
    role = update.message.text
    context.user_data['role'] = role

    # Регистрация пользователя и генерация пароля
    full_name = context.user_data['full_name']
    registration_result = await auth_manager.register_user(full_name, role)

    if registration_result["status"] == "success":
        password = registration_result["password"]
        await update.message.reply_text(f"Пользователь зарегистрирован! Ваш пароль: {password}. Передайте этот пароль руководителю.")
        return ConversationHandler.END
    else:
        await update.message.reply_text(f"Ошибка регистрации: {registration_result['message']}")
        return ConversationHandler.END

# Ввод пароля пользователем
async def password_handle(update: Update, context: CallbackContext):
    """Проверка пароля пользователя."""
    user_password = update.message.text
    verification_result = await auth_manager.verify_password(user_password)

    if verification_result["status"] == "success":
        await update.message.reply_text(f"Регистрация завершена! Ваш JWT-токен: {verification_result['token']}")
    else:
        await update.message.reply_text(f"Ошибка: {verification_result['message']}")
    return ConversationHandler.END

# Настройка ConversationHandler для регистрации
registration_handler = ConversationHandler(
    entry_points=[CommandHandler('register', register_handle)],
    states={
        ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name_handle)],
        ASK_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_role_handle)],
        ASK_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, password_handle)],
    },
    fallbacks=[]
)

# Команда для ввода пароля и проверки
async def password_command_handle(update: Update, context: CallbackContext):
    """Начало процесса проверки пароля."""
    await update.message.reply_text("Введите ваш пароль для завершения регистрации:")
    return ASK_PASSWORD

# Регистрация всех команд
def register_commands(application):
    application.add_handler(CommandHandler("start", start_handle))
    application.add_handler(registration_handler)
    application.add_handler(CommandHandler("password", password_command_handle))
