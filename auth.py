import logging
import secrets  # Для безопасной генерации паролей
import string   # Для алфавита паролей
import time
import asyncio
import bcrypt
from permissions_manager import PermissionsManager
from logger_utils import setup_logging
from dotenv import load_dotenv  # Для загрузки переменных окружения
from telegram import Update
from telegram.ext import (
    CallbackContext,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters
)
from functools import partial

# Загрузка переменных окружения
load_dotenv()

# Инициализация логирования
logger = setup_logging()

# Стадии диалога для регистрации
ASK_NAME, ASK_ROLE, ASK_OPERATOR_ID = range(3)

class AuthManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.permissions_manager = PermissionsManager(db_manager)

    def generate_password(self, length=12):
        """Генерация безопасного случайного пароля."""
        alphabet = string.ascii_letters + string.digits + string.punctuation
        while True:
            password = ''.join(secrets.choice(alphabet) for _ in range(length))
            # Убедимся, что пароль содержит хотя бы один символ из каждой категории
            if (any(c.islower() for c in password) and
                any(c.isupper() for c in password) and
                any(c.isdigit() for c in password) and
                any(c in string.punctuation for c in password)):
                return password

    async def register_user(self, user_id, full_name, role, operator_id=None):
        """
        Регистрация пользователя, генерация пароля и сохранение в базе данных.
        Использует Telegram user_id как уникальный идентификатор.
        """
        try:
            start_time = time.time()

            # Если роль - оператор, проверяем существование operator_id
            if role.lower() == "operator":
                operator = await self.db_manager.find_operator_by_id(operator_id)
                if not operator:
                    logger.error(f"Оператор с ID {operator_id} не найден.")
                    return {"status": "error", "message": f"Оператор с ID {operator_id} не найден."}
            else:
                operator_id = None  # Для других ролей operator_id не требуется

            # Генерация пароля
            password = self.generate_password()

            # Хешируем пароль асинхронно
            hashed_password = await asyncio.to_thread(
                bcrypt.hashpw, password.encode('utf-8'), bcrypt.gensalt()
            )

            # Маппинг роли на role_id
            role_id = await self.db_manager.get_role_id_by_name(role)
            if role_id is None:
                logger.error(f"Роль '{role}' не найдена.")
                return {"status": "error", "message": f"Роль '{role}' не найдена."}

            # Регистрация пользователя в базе данных
            await self.db_manager.register_user_if_not_exists(
                user_id=user_id,
                username=None,  # Можно добавить получение username из context при необходимости
                full_name=full_name,
                operator_id=operator_id,
                password=hashed_password,
                role_id=role_id
            )

            elapsed_time = time.time() - start_time
            logger.info(f"Пользователь {full_name} зарегистрирован с ролью {role}, operator ID {operator_id}. (Время выполнения: {elapsed_time:.4f} сек)")
            return {"status": "success", "password": password}
        except Exception as e:
            logger.error(f"Ошибка при регистрации пользователя {full_name}: {e}")
            return {"status": "error", "message": f"Ошибка регистрации: {e}"}

    async def verify_password(self, user_id, input_password):
        """
        Проверка пароля пользователя.
        """
        try:
            start_time = time.time()
            # Получаем хэшированный пароль пользователя по user_id
            hashed_password = await self.db_manager.get_user_password(user_id)

            if hashed_password:
                # Проверяем пароль асинхронно
                is_correct = await asyncio.to_thread(
                    bcrypt.checkpw, input_password.encode('utf-8'), hashed_password
                )
                elapsed_time = time.time() - start_time
                if is_correct:
                    role_id = await self.db_manager.get_user_role(user_id)
                    role_name = await self.db_manager.get_role_name_by_id(role_id)
                    logger.info(f"Пользователь {user_id} успешно аутентифицирован. (Время выполнения: {elapsed_time:.4f} сек)")
                    return {"status": "success", "role": role_name}
                else:
                    logger.warning(f"Неверный пароль для пользователя {user_id}. (Время выполнения: {elapsed_time:.4f} сек)")
                    return {"status": "error", "message": "Неверный пароль"}
            else:
                logger.warning(f"Пользователь {user_id} не найден. (Время выполнения: {elapsed_time:.4f} сек)")
                return {"status": "error", "message": "Пользователь не найден"}
        except Exception as e:
            logger.error(f"Ошибка при проверке пароля для пользователя {user_id}: {e}")
            return {"status": "error", "message": f"Ошибка проверки: {e}"}

# Команда для начала регистрации
async def register_handle(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Начало регистрации: бот запрашивает полное имя."""
    user = update.effective_user
    logger.info(f"Регистрация начата для пользователя {user.id} ({user.full_name}).")
    await update.message.reply_text("Пожалуйста, введите ваше полное имя:")
    return ASK_NAME

# Обработка полного имени пользователя
async def ask_name_handle(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Получение полного имени пользователя."""
    full_name = update.message.text.strip()
    context.user_data['full_name'] = full_name
    logger.info(f"Получено полное имя: {full_name} от пользователя {update.effective_user.id}.")
    await update.message.reply_text("Теперь, пожалуйста, укажите вашу роль (например, Operator или Supervisor):")
    return ASK_ROLE

# Обработка роли пользователя
async def ask_role_handle(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Получение роли пользователя."""
    role = update.message.text.strip()
    context.user_data['role'] = role
    logger.info(f"Получена роль: {role} для пользователя {update.effective_user.id}.")

    # Если роль - оператор, запросить operator_id, иначе завершить регистрацию
    if role.lower() == "operator":
        await update.message.reply_text("Пожалуйста, введите ваш operator ID:")
        return ASK_OPERATOR_ID
    else:
        # Регистрация пользователя без operator_id
        full_name = context.user_data['full_name']
        user_id = update.effective_user.id

        registration_result = await auth_manager.register_user(
            user_id=user_id,
            full_name=full_name,
            role=role
        )

        if registration_result["status"] == "success":
            password = registration_result["password"]
            logger.info(f"Пользователь с ID {user_id} успешно зарегистрирован.")
            await update.message.reply_text(f"Регистрация прошла успешно! Ваш пароль: {password}. Пожалуйста, сохраните его в безопасном месте.")
            return ConversationHandler.END
        else:
            logger.error(f"Ошибка при регистрации пользователя с ID {user_id}: {registration_result['message']}")
            await update.message.reply_text(f"Ошибка при регистрации: {registration_result['message']}")
            return ConversationHandler.END

# Обработка operator_id (для операторов)
async def ask_operator_id_handle(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Получение operator_id и завершение регистрации."""
    operator_id_input = update.message.text.strip()
    if not operator_id_input.isdigit():
        logger.warning(f"Неверный ввод operator ID от пользователя {update.effective_user.id}: {operator_id_input}")
        await update.message.reply_text("Ошибка: Operator ID должен содержать только цифры. Пожалуйста, попробуйте снова:")
        return ASK_OPERATOR_ID

    operator_id = int(operator_id_input)
    context.user_data['operator_id'] = operator_id

    full_name = context.user_data['full_name']
    role = context.user_data['role']
    user_id = update.effective_user.id

    logger.info(f"Попытка регистрации пользователя с ID {user_id}, роль {role}, operator ID {operator_id}.")

    # Регистрация пользователя
    registration_result = await auth_manager.register_user(
        user_id=user_id,
        full_name=full_name,
        role=role,
        operator_id=operator_id
    )

    if registration_result["status"] == "success":
        password = registration_result["password"]
        logger.info(f"Пользователь с ID {user_id} успешно зарегистрирован.")
        await update.message.reply_text(f"Регистрация прошла успешно! Ваш пароль: {password}. Пожалуйста, сохраните его в безопасном месте.")
        return ConversationHandler.END
    else:
        logger.error(f"Ошибка при регистрации пользователя с ID {user_id}: {registration_result['message']}")
        await update.message.reply_text(f"Ошибка при регистрации: {registration_result['message']}")
        return ConversationHandler.END

# Команда для сброса пароля
async def reset_password_handle(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Команда для сброса пароля."""
    user_id = update.effective_user.id
    logger.info(f"Запрос на сброс пароля от пользователя {user_id}.")

    # Генерация нового пароля
    new_password = auth_manager.generate_password()

    # Хешируем пароль асинхронно
    hashed_password = await asyncio.to_thread(
        bcrypt.hashpw, new_password.encode('utf-8'), bcrypt.gensalt()
    )

    # Обновление пароля в базе данных
    try:
        await auth_manager.db_manager.update_user_password(user_id, hashed_password)
        await update.message.reply_text(f"Ваш новый пароль: {new_password}. Пожалуйста, сохраните его в безопасном месте.")
        logger.info(f"Пароль для пользователя {user_id} успешно сброшен.")
    except Exception as e:
        logger.error(f"Ошибка при сбросе пароля для пользователя {user_id}: {e}")
        await update.message.reply_text("Произошла ошибка при сбросе пароля. Пожалуйста, попробуйте позже.")

# Команда для проверки пароля (пример использования)
async def verify_password_handle(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Команда для проверки пароля пользователя."""
    args = context.args
    if not args:
        await update.message.reply_text("Пожалуйста, укажите пароль после команды, например: /verify_password your_password")
        return

    user_id = update.effective_user.id
    input_password = ' '.join(args).strip()

    verification_result = await auth_manager.verify_password(user_id, input_password)
    if verification_result["status"] == "success":
        role = verification_result["role"]
        await update.message.reply_text(f"Пароль верный. Ваша роль: {role}.")
    else:
        await update.message.reply_text(f"Ошибка: {verification_result['message']}")

# Завершение диалога при отмене
async def cancel_handle(update: Update, context: CallbackContext):
    """Отмена регистрации."""
    user = update.effective_user
    logger.info(f"Пользователь {user.id} отменил регистрацию.")
    await update.message.reply_text("Регистрация отменена.", reply_markup=None)
    return ConversationHandler.END

def setup_auth_handlers(application, db_manager):
    """Функция для добавления всех обработчиков аутентификации в приложение."""
    auth_manager = AuthManager(db_manager)

    # Используем partial, чтобы передать auth_manager в обработчики
    registration_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('register', partial(register_handle, auth_manager=auth_manager))],
        states={
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, partial(ask_name_handle, auth_manager=auth_manager))],
            ASK_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, partial(ask_role_handle, auth_manager=auth_manager))],
            ASK_OPERATOR_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, partial(ask_operator_id_handle, auth_manager=auth_manager))],
        },
        fallbacks=[CommandHandler('cancel', cancel_handle)],
    )

    # Добавляем обработчики в приложение
    application.add_handler(registration_conv_handler)
    application.add_handler(CommandHandler('reset_password', partial(reset_password_handle, auth_manager=auth_manager)))
    application.add_handler(CommandHandler('verify_password', partial(verify_password_handle, auth_manager=auth_manager)))
