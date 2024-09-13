import logging
import secrets  # Используем для безопасной генерации паролей
import string   # Используем для генерации алфавита пароля
import time  # Для замера времени
from db_setup import create_async_connection, get_user_role, add_user, get_user_password
from db_helpers import find_operator_by_id  # Импорт функции для поиска оператора по ID
from permissions_manager import PermissionsManager
from logger_utils import setup_logging
from telegram import Update
from telegram.ext import CallbackContext, CommandHandler, ConversationHandler, MessageHandler, filters

# Инициализация логирования
logger = setup_logging()

# Этапы диалога для регистрации
ASK_NAME, ASK_ROLE, ASK_OPERATOR_ID = range(3)

class AuthManager:
    def __init__(self):
        self.permissions_manager = PermissionsManager()

    def generate_password(self, length=12):
        """Генерация безопасного случайного пароля."""
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(length))

    async def register_user(self, user_id, full_name, role, operator_id):
        """
        Регистрация пользователя, генерация пароля и сохранение в базе данных.
        Используем Telegram user_id как уникальный идентификатор.
        """
        connection = await create_async_connection()
        if not connection:
            return {"status": "error", "message": "Ошибка подключения к базе данных"}
        
        try:
            start_time = time.time()

            # Проверка, существует ли пользователь в базе данных
            existing_user = await get_user_password(user_id)
            if existing_user:
                logger.warning(f"[КРОТ]: Пользователь {full_name} уже зарегистрирован с user_id {user_id}.")
                return {"status": "error", "message": "Пользователь уже зарегистрирован"}

            # Проверка, существует ли оператор с данным operator_id
            operator = await find_operator_by_id(operator_id)
            if not operator:
                logger.error(f"[КРОТ]: Оператор с ID {operator_id} не найден.")
                return {"status": "error", "message": f"Оператор с ID {operator_id} не найден."}

            # Генерация пароля
            password = self.generate_password()

            # Добавляем пользователя с ролью и ID оператора
            await add_user(connection, user_id=user_id, username=full_name, full_name=full_name, role_name=role, operator_id=operator_id)
            
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Пользователь {full_name} зарегистрирован с ролью {role}, операторским ID {operator_id}. "
                        f"(Время выполнения: {elapsed_time:.4f} сек)")
            return {"status": "success", "password": password}
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при регистрации пользователя {full_name}: {e}")
            return {"status": "error", "message": f"Ошибка регистрации: {e}"}
        finally:
            await connection.ensure_closed()

    async def verify_password(self, user_password):
        """
        Проверка пароля, присвоенного пользователю.
        """
        connection = await create_async_connection()
        if not connection:
            return {"status": "error", "message": "Ошибка подключения к базе данных"}
        
        try:
            start_time = time.time()
            async with connection.cursor() as cursor:
                sql = "SELECT user_id, role_id FROM UsersTelegaBot WHERE password = %s"
                await cursor.execute(sql, (user_password,))
                user = await cursor.fetchone()
                
                elapsed_time = time.time() - start_time
                if user:
                    role = await get_user_role(connection, user['user_id'])
                    logger.info(f"[КРОТ]: Пользователь с паролем {user_password} успешно найден. "
                                f"(Время выполнения: {elapsed_time:.4f} сек)")
                    return {"status": "success", "role": role}
                else:
                    logger.warning(f"[КРОТ]: Пароль {user_password} не найден в базе данных. "
                                   f"(Время выполнения: {elapsed_time:.4f} сек)")
                    return {"status": "error", "message": "Неверный пароль"}
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при проверке пароля {user_password}: {e}")
            return {"status": "error", "message": f"Ошибка проверки: {e}"}
        finally:
            await connection.ensure_closed()

auth_manager = AuthManager()

# Команда для начала регистрации
async def register_handle(update: Update, context: CallbackContext):
    """Начало регистрации: бот запрашивает ФИО."""
    logger.info(f"[КРОТ]: Начало регистрации для пользователя {update.message.from_user.id}.")
    await update.message.reply_text("Введите ваше ФИО:")
    return ASK_NAME

# Обработка имени пользователя
async def ask_name_handle(update: Update, context: CallbackContext):
    """Получение ФИО пользователя."""
    context.user_data['full_name'] = update.message.text
    logger.info(f"[КРОТ]: Получено ФИО: {context.user_data['full_name']} от пользователя {update.message.from_user.id}.")
    await update.message.reply_text("Теперь введите вашу роль (например, Operator):")
    return ASK_ROLE

# Обработка роли пользователя
async def ask_role_handle(update: Update, context: CallbackContext):
    """Получение роли пользователя."""
    role = update.message.text
    context.user_data['role'] = role
    logger.info(f"[КРОТ]: Получена роль: {role} для пользователя {update.message.from_user.id}.")
    
    # Запрашиваем ID оператора
    await update.message.reply_text("Введите ваш ID оператора:")
    return ASK_OPERATOR_ID

# Обработка ID оператора
async def ask_operator_id_handle(update: Update, context: CallbackContext):
    """Получение ID оператора и завершение регистрации."""
    operator_id = update.message.text
    if not operator_id.isdigit():
        logger.warning(f"[КРОТ]: Некорректный ввод ID оператора от пользователя {update.message.from_user.id}.")
        await update.message.reply_text("Ошибка: ID оператора должен содержать только цифры. Попробуйте снова.")
        return ASK_OPERATOR_ID

    context.user_data['operator_id'] = operator_id

    full_name = context.user_data['full_name']
    role = context.user_data['role']
    user_id = update.message.from_user.id

    logger.info(f"[КРОТ]: Попытка регистрации пользователя с ID {user_id}, ролью {role} и операторским ID {operator_id}.")

    # Регистрация пользователя и генерация пароля
    registration_result = await auth_manager.register_user(user_id, full_name, role, operator_id)

    if registration_result["status"] == "success":
        password = registration_result["password"]
        logger.info(f"[КРОТ]: Пользователь с ID {user_id} успешно зарегистрирован.")
        await update.message.reply_text(f"Пользователь зарегистрирован! Ваш пароль: {password}. Передайте этот пароль руководителю.")
        return ConversationHandler.END
    else:
        logger.error(f"[КРОТ]: Ошибка регистрации пользователя с ID {user_id}: {registration_result['message']}")
        await update.message.reply_text(f"Ошибка регистрации: {registration_result['message']}")
        return ConversationHandler.END

# Ввод пароля пользователем
async def password_handle(update: Update, context: CallbackContext):
    """Проверка пароля пользователя."""
    user_password = update.message.text
    logger.info(f"[КРОТ]: Проверка пароля пользователя с ID {update.message.from_user.id}.")
    
    verification_result = await auth_manager.verify_password(user_password)

    if verification_result["status"] == "success":
        logger.info(f"[КРОТ]: Пароль успешно проверен для пользователя {update.message.from_user.id}. "
                    f"Роль: {verification_result['role']}")
        await update.message.reply_text(f"Пароль успешно проверен! Ваша роль: {verification_result['role']}")
    else:
        logger.error(f"[КРОТ]: Ошибка при проверке пароля пользователя {update.message.from_user.id}: {verification_result['message']}")
        await update.message.reply_text(f"Ошибка: {verification_result['message']}")
    return ConversationHandler.END

# Настройка ConversationHandler для регистрации
registration_handler = ConversationHandler(
    entry_points=[CommandHandler('register', register_handle)],
    states={
        ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name_handle)],
        ASK_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_role_handle)],
        ASK_OPERATOR_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_operator_id_handle)],
    },
    fallbacks=[]
)
