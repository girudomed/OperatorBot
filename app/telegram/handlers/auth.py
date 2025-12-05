"""
Telegram хендлеры авторизации и регистрации.
"""

import time
from functools import partial

from telegram import Update, ReplyKeyboardMarkup
from typing import List, Optional

from telegram.ext import (
    CallbackContext,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

try:  # PyTelegramBot v20+
    from telegram.ext import ApplicationHandlerStop as HandlerStop
except ImportError:  # Legacy Dispatcher API
    try:
        from telegram.ext import DispatcherHandlerStop as HandlerStop  # type: ignore
    except ImportError:
        class HandlerStop(Exception):
            """Fallback для совместимости, не блокирует обработчики PTB."""
            pass

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

COMMON_COMMANDS = ["start", "help"]
OPERATOR_COMMANDS = COMMON_COMMANDS + ["weekly_quality", "report"]
ADMIN_COMMANDS = OPERATOR_COMMANDS + ["call_lookup", "admin", "approve", "make_admin", "admins"]
SUPERADMIN_COMMANDS = ADMIN_COMMANDS + ["make_superadmin", "register"]

COMMAND_DESCRIPTIONS = {
    "start": "Перезапустить диалог с ботом",
    "help": "Показать список доступных команд",
    "register": "Отправить заявку на доступ",
    "weekly_quality": "Получить еженедельный отчёт качества",
    "report": "Сформировать AI-отчёт",
    "call_lookup": "Найти звонки по номеру",
    "admin": "Открыть админ-панель",
    "approve": "Утвердить пользователя по ID",
    "make_admin": "Назначить администратора",
    "make_superadmin": "Назначить супер-админа",
    "admins": "Показать текущий список админов",
}

# Стадии диалога для регистрации
ASK_NAME, ASK_ROLE, ASK_OPERATOR_ID = range(3)
ALLOWED_PRE_AUTH_COMMANDS = {"/start", "/help", "/register", "/cancel"}


class AuthManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.user_repo = UserRepository(db_manager)
        self.permissions_manager = PermissionsManager(db_manager)

    async def register_user(
        self,
        user_id: int,
        username: str | None,
        full_name: str,
        role: str,
        operator_id: int | None = None
    ):
        """
        Регистрация пользователя и сохранение его данных в базе.
        Использует Telegram user_id как уникальный идентификатор.
        """
        try:
            start_time = time.time()

            role_id_result = await self.user_repo.get_role_id_by_name(role)
            if role_id_result is None:
                logger.error(f"Роль '{role}' не найдена.")
                return {"status": "error", "message": f"Роль '{role}' не найдена."}
            role_id = role_id_result.get('id')

            logger.debug(
                "Регистрация в БД: user_id=%s, username=%s, full_name=%s, role_id=%s, operator_id=%s",
                user_id,
                username,
                full_name,
                role_id,
                operator_id,
            )
            await self.user_repo.register_user_if_not_exists(
                user_id=user_id,
                username=username,
                full_name=full_name,
                operator_id=operator_id,
                password=None,
                role_id=role_id
            )

            elapsed_time = time.time() - start_time
            logger.info(
                "Пользователь %s зарегистрирован с ролью %s, operator ID %s. (Время выполнения: %.4f сек)",
                full_name,
                role,
                operator_id,
                elapsed_time,
            )
            return {"status": "success"}
        except Exception as e:
            logger.error(f"Ошибка при регистрации пользователя {full_name}: {e}")
            return {"status": "error", "message": f"Ошибка регистрации: {e}"}

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

    if role.lower() == "operator":
        await update.message.reply_text(
            "Теперь укажите ваш Operator ID — он помогает связать ваш профиль с АТС."
        )
        return ASK_OPERATOR_ID

    return await _complete_registration(update, context, auth_manager)


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
    user_id = update.effective_user.id

    logger.info(f"Попытка регистрации пользователя с ID {user_id}, роль {role}, operator ID {operator_id}.")

    return await _complete_registration(
        update,
        context,
        auth_manager,
        operator_id=operator_id,
    )


async def _complete_registration(
    update: Update,
    context: CallbackContext,
    auth_manager: AuthManager,
    operator_id: int | None = None
):
    """Финализирует регистрацию и отправляет ответ пользователю."""
    message = update.message
    user = update.effective_user

    full_name = context.user_data.get('full_name')
    role = context.user_data.get('role')

    if not full_name or not role:
        logger.error(
            "Нехватает данных для регистрации пользователя %s (full_name=%s, role=%s)",
            user.id,
            full_name,
            role,
        )
        if message:
            await message.reply_text("Не удалось завершить регистрацию. Попробуйте начать заново с /register.")
        return ConversationHandler.END

    registration_result = await auth_manager.register_user(
        user_id=user.id,
        username=user.username,
        full_name=full_name,
        role=role,
        operator_id=operator_id,
    )

    if registration_result["status"] == "success":
        logger.info("Регистрация пользователя %s завершена.", user.id)
        if message:
            await message.reply_text(
                "Регистрация прошла успешно! Заявка отправлена администраторам, "
                "и мы уведомим вас после подтверждения доступа."
            )
    else:
        logger.error(
            "Ошибка при регистрации пользователя %s: %s",
            user.id,
            registration_result["message"],
        )
        if message:
            await message.reply_text(f"Ошибка при регистрации: {registration_result['message']}")

    return ConversationHandler.END


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
    
    role = 'operator'
    status = None
    is_super = permissions.is_supreme_admin(user.id, user.username) or permissions.is_dev_admin(user.id, user.username)
    
    if is_super:
        role = 'superadmin'
        status = 'approved'
    else:
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
            "Добро пожаловать! Ваш статус подтверждён.\n"
            "Используйте команды ниже, чтобы получить отчёты и справочную информацию."
        )
    elif role == 'admin':
        text = (
            "Вы вошли как администратор. Вам доступна админ-панель, утверждение пользователей "
            "и просмотр статистики. Используйте команды ниже."
        )
    else:
        text = (
            "Вы вошли как superadmin. Вам доступны все функции, включая управление ролями.\n"
            "Список команд и кнопки приведены ниже."
        )
    
    keyboard = _build_keyboard_for_role(role)
    commands_text = _format_commands_for_role(role)
    
    await message.reply_text(
        f"{text}\n\n<b>Доступные команды:</b>\n{commands_text}",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


async def help_command(update: Update, context: CallbackContext, permissions: PermissionsManager):
    """Команда /help: выводит список команд и доступных функций."""
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return
    
    role = 'operator'
    status = None
    
    if permissions.is_supreme_admin(user.id, user.username) or permissions.is_dev_admin(user.id, user.username):
        role = 'superadmin'
        status = 'approved'
    else:
        status = await permissions.get_user_status(user.id)
    
    if status is None:
        await message.reply_text("Вы ещё не зарегистрированы. Используйте /register, чтобы получить доступ.")
        return
    
    if status == 'pending':
        await message.reply_text("Ваш аккаунт пока ожидает подтверждения. Как только статус станет approved, команды появятся автоматически.")
        return
    
    if status == 'blocked':
        await message.reply_text("Ваш аккаунт заблокирован. Свяжитесь с администратором для разблокировки.")
        return
    
    if role != 'superadmin':
        role = await permissions.get_effective_role(user.id, user.username)
    
    keyboard = _build_keyboard_for_role(role)
    commands_text = _format_commands_for_role(role)
    
    await message.reply_text(
        f"<b>Доступные команды ({role}):</b>\n{commands_text}",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


def setup_auth_handlers(application, db_manager: DatabaseManager, permissions_manager: PermissionsManager):
    """Функция для добавления всех обработчиков аутентификации в приложение."""
    auth_manager = AuthManager(db_manager)

    application.add_handler(
        MessageHandler(
            filters.COMMAND,
            partial(registration_guard_command, permissions=permissions_manager),
        ),
        group=0,
    )
    application.add_handler(
        CallbackQueryHandler(
            partial(registration_guard_callback, permissions=permissions_manager)
        ),
        group=0,
    )

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
    application.add_handler(CommandHandler('help', partial(help_command, permissions=permissions_manager)))
    logger.info("Хендлеры авторизации зарегистрированы.")


async def registration_guard_command(update: Update, context: CallbackContext, permissions: PermissionsManager) -> bool:
    """Блокирует любые команды до завершения регистрации/апрува."""
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return False

    text = (message.text or "").strip()
    if not text:
        return False

    command_token: Optional[str] = None
    if text.startswith("/"):
        command_token = text.split()[0]
    else:
        for token in text.split():
            if token.startswith("/"):
                command_token = token
                break

    if not command_token:
        return False

    command_base = command_token.lower().split("@")[0]
    if command_base in ALLOWED_PRE_AUTH_COMMANDS:
        return False

    if permissions.is_supreme_admin(user.id, user.username) or permissions.is_dev_admin(user.id, user.username):
        return False

    status = await permissions.get_user_status(user.id)
    if status is None:
        await message.reply_text("Вы ещё не зарегистрированы. Используйте /start и /register, чтобы подать заявку.")
        raise HandlerStop()
    if status == 'pending':
        await message.reply_text("Ваша заявка ожидает подтверждения администратором. Пожалуйста, дождитесь одобрения.")
        raise HandlerStop()
    if status == 'blocked':
        await message.reply_text("Ваш доступ временно ограничен. Обратитесь к администратору.")
        raise HandlerStop()

    return False


async def registration_guard_callback(update: Update, context: CallbackContext, permissions: PermissionsManager) -> bool:
    """Блокирует callback-запросы для незарегистрированных пользователей."""
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return False

    if permissions.is_supreme_admin(user.id, user.username) or permissions.is_dev_admin(user.id, user.username):
        return False

    status = await permissions.get_user_status(user.id)
    if status is None:
        await query.answer("Сначала зарегистрируйтесь через /start → /register.", show_alert=True)
        raise HandlerStop()
    if status == 'pending':
        await query.answer("Ваша заявка ещё не одобрена.", show_alert=True)
        raise HandlerStop()
    if status == 'blocked':
        await query.answer("Доступ заблокирован. Свяжитесь с администратором.", show_alert=True)
        raise HandlerStop()

    return False


def _commands_for_role(role: str) -> List[str]:
    if role == 'superadmin':
        return SUPERADMIN_COMMANDS
    if role == 'admin':
        return ADMIN_COMMANDS
    return OPERATOR_COMMANDS


def _build_keyboard_for_role(role: str) -> Optional[ReplyKeyboardMarkup]:
    # Для админов и суперадминов отключаем Reply клавиатуру, 
    # так как они должны использовать Inline меню (/admin)
    if role in ('admin', 'superadmin'):
        return None
        
    commands = _commands_for_role(role)
    buttons = [f"/{cmd}" for cmd in commands]
    rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


def _format_commands_for_role(role: str) -> str:
    commands = _commands_for_role(role)
    lines = []
    for cmd in commands:
        description = COMMAND_DESCRIPTIONS.get(cmd, "Команда")
        lines.append(f"/{cmd} — {description}")
    return "\n".join(lines)
