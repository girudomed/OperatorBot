"""
Telegram хендлеры авторизации и регистрации.
"""

import time
from functools import partial

from telegram import Update
from telegram.ext import (
    CallbackContext,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

# Стадии диалога для регистрации
ASK_NAME, ASK_ROLE, ASK_OPERATOR_ID = range(3)


class AuthManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.user_repo = UserRepository(db_manager)
        self.permissions_manager = PermissionsManager(db_manager)

    async def register_user(
        self,
        user_id: int,
        full_name: str,
        role: str,
        input_password: str,
        operator_id: int | None = None
    ):
        """
        Регистрация пользователя, генерация пароля и сохранение в базе данных.
        Использует Telegram user_id как уникальный идентификатор.
        """
        try:
            start_time = time.time()

            role_id_result = await self.user_repo.get_role_id_by_name(role)
            if role_id_result is None:
                logger.error(f"Роль '{role}' не найдена.")
                return {"status": "error", "message": f"Роль '{role}' не найдена."}
            
            role_id = role_id_result.get('id')

            # Проверка пароля роли
            stored_role_password = await self.user_repo.get_role_password_by_id(role_id)
            if input_password != stored_role_password:
                logger.error(f"Неверный пароль для роли '{role}'.")
                return {"status": "error", "message": "Неверный пароль для выбранной роли."}

            # Регистрация пользователя в базе данных
            logger.debug(f"Регистрация в БД: user_id={user_id}, full_name={full_name}, role_id={role_id}, operator_id={operator_id}")
            await self.user_repo.register_user_if_not_exists(
                user_id=user_id,
                username=None,
                full_name=full_name,
                operator_id=operator_id,
                password=stored_role_password,
                role_id=role_id
            )

            elapsed_time = time.time() - start_time
            logger.info(f"Пользователь {full_name} зарегистрирован с ролью {role}, operator ID {operator_id}. (Время выполнения: {elapsed_time:.4f} сек)")
            return {"status": "success", "password": stored_role_password}
        except Exception as e:
            logger.error(f"Ошибка при регистрации пользователя {full_name}: {e}")
            return {"status": "error", "message": f"Ошибка регистрации: {e}"}

    async def verify_password(self, user_id: int, input_password: str):
        """
        Проверка пароля пользователя.
        """
        try:
            start_time = time.time()
            # Получаем роль пользователя по user_id
            role_id = await self.user_repo.get_user_role(user_id)
            if not role_id:
                logger.warning(f"Роль для пользователя {user_id} не найдена. (Время выполнения: {time.time() - start_time:.4f} сек)")
                return {"status": "error", "message": "Роль пользователя не найдена."}

            # Получаем пароль, связанный с этой ролью
            role_password = await self.user_repo.get_role_password_by_id(role_id)
            if not role_password:
                logger.warning(f"Пароль для роли с ID {role_id} не найден. (Время выполнения: {time.time() - start_time:.4f} сек)")
                return {"status": "error", "message": "Пароль для роли не найден."}

            # Простое сравнение пароля с сохраненным в базе
            if input_password == role_password:
                role_name_result = await self.user_repo.get_role_name_by_id(role_id)
                role_name = role_name_result.get('role_name') if role_name_result else None
                elapsed_time = time.time() - start_time
                logger.info(f"Пользователь {user_id} успешно аутентифицирован как {role_name}. (Время выполнения: {elapsed_time:.4f} сек)")
                return {"status": "success", "role": role_name}
            else:
                elapsed_time = time.time() - start_time
                logger.warning(f"Неверный пароль для пользователя {user_id}. (Время выполнения: {elapsed_time:.4f} сек)")
                return {"status": "error", "message": "Неверный пароль"}

        except Exception as e:
            logger.error(f"Ошибка при проверке пароля для пользователя {user_id}: {e}")
            return {"status": "error", "message": f"Ошибка проверки: {e}"}


async def verify_password_handle(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Команда для проверки пароля пользователя."""
    args = context.args
    if not args:
        await update.message.reply_text("Пожалуйста, укажите пароль после команды, например: /verify_password ваш_пароль")
        return

    user_id = update.effective_user.id
    input_password = ' '.join(args).strip()

    # Вызов метода проверки пароля из AuthManager
    verification_result = await auth_manager.verify_password(user_id, input_password)
    if verification_result["status"] == "success":
        role = verification_result["role"]
        await update.message.reply_text(f"Пароль верный. Ваша роль: {role}.")
    else:
        await update.message.reply_text(f"Ошибка: {verification_result['message']}")


# Команда для начала регистрации
async def register_handler(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Начало регистрации: бот запрашивает позывной оператора."""
    user = update.effective_user
    logger.info(f"Регистрация начата для пользователя {user.id} ({user.full_name}).")
    await update.message.reply_text("Пожалуйста, введите ваш позывной оператора из приветственного сообщения:")
    return ASK_NAME


# Обработка полного имени пользователя
async def ask_name_handler(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Получение полного имени пользователя."""
    full_name = update.message.text.strip()
    context.user_data['full_name'] = full_name
    logger.info(f"Получено полное имя: {full_name} от пользователя {update.effective_user.id}.")
    await update.message.reply_text("Теперь, пожалуйста, укажите вашу роль (например, Operator или Supervisor):")
    return ASK_ROLE


# Обработка роли пользователя
async def ask_role_handler(update: Update, context: CallbackContext, auth_manager: AuthManager):
    """Получение роли пользователя."""
    role = update.message.text.strip()
    context.user_data['role'] = role
    logger.info(f"Получена роль: {role} для пользователя {update.effective_user.id}.")

    await update.message.reply_text("Пожалуйста, введите пароль для выбранной роли:")
    return ASK_OPERATOR_ID if role.lower() == "operator" else ASK_ROLE


# Обработка operator_id (для операторов)
async def ask_operator_id_handler(update: Update, context: CallbackContext, auth_manager: AuthManager):
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
    password = context.user_data.get('password', '')
    user_id = update.effective_user.id

    logger.info(f"Попытка регистрации пользователя с ID {user_id}, роль {role}, operator ID {operator_id}.")

    # Регистрация пользователя
    registration_result = await auth_manager.register_user(
        user_id=user_id,
        full_name=full_name,
        role=role,
        input_password=password,
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

    try:
        # Получаем роль пользователя по user_id
        role_id = await auth_manager.user_repo.get_user_role(user_id)
        if not role_id:
            await update.message.reply_text("Роль пользователя не найдена. Пожалуйста, проверьте свои данные.")
            logger.warning(f"Роль для пользователя {user_id} не найдена.")
            return

        # Получаем пароль для этой роли
        role_password = await auth_manager.user_repo.get_role_password_by_id(role_id)
        if not role_password:
            await update.message.reply_text("Не удалось найти пароль для вашей роли. Обратитесь к администратору.")
            logger.warning(f"Пароль для роли с ID {role_id} не найден.")
            return

        # Обновляем пароль пользователя на тот, который привязан к его роли
        await auth_manager.user_repo.update_user_password(user_id, role_password)

        await update.message.reply_text(
            f"Ваш пароль был успешно сброшен. Новый пароль для вашей роли: {role_password}. Пожалуйста, сохраните его в безопасном месте."
        )
        logger.info(f"Пароль для пользователя {user_id} успешно сброшен и установлен в соответствии с ролью {role_id}.")
    except Exception as e:
        logger.error(f"Ошибка при сбросе пароля для пользователя {user_id}: {e}")
        await update.message.reply_text("Произошла ошибка при сбросе пароля. Пожалуйста, попробуйте позже.")


# Завершение диалога при отмене
async def cancel_handle(update: Update, context: CallbackContext):
    """Отмена регистрации."""
    user = update.effective_user
    logger.info(f"Пользователь {user.id} отменил регистрацию.")
    await update.message.reply_text("Регистрация отменена.", reply_markup=None)
    return ConversationHandler.END


async def start_command(update: Update, context: CallbackContext, permissions: PermissionsManager):
    """Команда /start с проверкой статуса и роли пользователя."""
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return
    
    if permissions.is_supreme_admin(user.id, user.username) or permissions.is_dev_admin(user.id, user.username):
        await message.reply_text(
            "Вы вошли как superadmin. Вам доступны все команды и управление ролями."
        )
        return
    
    status = await permissions.get_user_status(user.id)
    if status is None:
        await message.reply_text(
            "Привет! Ты не зарегистрирован в системе. Используй /register, чтобы отправить заявку."
        )
        return
    
    if status == 'pending':
        await message.reply_text(
            "Ваш аккаунт ожидает подтверждения администратором."
        )
        return
    
    if status == 'blocked':
        await message.reply_text(
            "Ваш доступ ограничён. Обратитесь к администратору."
        )
        return
    
    role = await permissions.get_effective_role(user.id, user.username)
    if role == 'operator':
        text = (
            "Добро пожаловать! Ваш статус подтверждён. "
            "Используйте рабочие команды бота для получения отчётов и метрик."
        )
    elif role == 'admin':
        text = (
            "Вы вошли как администратор. Вам доступна админ-панель, утверждение пользователей "
            "и просмотр статистики."
        )
    else:
        text = (
            "Вы вошли как superadmin. Вам доступны все функции, включая управление ролями."
        )
    await message.reply_text(text)


def setup_auth_handlers(application, db_manager: DatabaseManager, permissions_manager: PermissionsManager):
    """Функция для добавления всех обработчиков аутентификации в приложение."""
    auth_manager = AuthManager(db_manager)

    # Используем partial, чтобы передать auth_manager в обработчики
    registration_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('register', partial(register_handler, auth_manager=auth_manager))],
        states={
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, partial(ask_name_handler, auth_manager=auth_manager))],
            ASK_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, partial(ask_role_handler, auth_manager=auth_manager))],
            ASK_OPERATOR_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, partial(ask_operator_id_handler, auth_manager=auth_manager))],
        },
        fallbacks=[CommandHandler('cancel', cancel_handle)],
    )

    application.add_handler(registration_conv_handler)
    application.add_handler(CommandHandler('start', partial(start_command, permissions=permissions_manager)))
    application.add_handler(CommandHandler('verify_password', partial(verify_password_handle, auth_manager=auth_manager)))
    application.add_handler(CommandHandler('reset_password', partial(reset_password_handle, auth_manager=auth_manager)))

    logger.info("Хендлеры авторизации зарегистрированы.")
