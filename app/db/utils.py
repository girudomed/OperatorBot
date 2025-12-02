import logging
import time  # Для замера времени
from typing import Any, Optional

from app.db.setup import add_user, get_user_role, get_user_password
from app.db.connection import execute_query
from app.logging_config import get_watchdog_logger

# Настройка логирования
logger = get_watchdog_logger(__name__)

# Функция для регистрации пользователя, если он еще не зарегистрирован
async def register_user_if_not_exists(update, context):
    """
    Регистрирует пользователя в базе данных, если он не существует.
    """
    user_id = update.message.from_user.id
    username = update.message.from_user.username or "unknown"
    full_name = update.message.from_user.full_name or "Без имени"

    try:
        start_time = time.time()

        existing_role = await get_user_role(user_id)
        elapsed_time = time.time() - start_time
        logger.info(
            f"[КРОТ]: Получена роль для пользователя {user_id}: {existing_role} "
            f"(Время выполнения: {elapsed_time:.4f} сек)"
        )

        if existing_role:
            await update.message.reply_text(
                f"Вы уже зарегистрированы с ролью '{existing_role}'."
            )
        else:
            await add_user(
                user_id=user_id,
                username=username,
                full_name=full_name,
                role_name="Operator",
            )
            await update.message.reply_text("Вы успешно зарегистрированы как 'Operator'.")
            logger.info(f"[КРОТ]: Новый пользователь {user_id} зарегистрирован.")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при регистрации пользователя: {e}")
        await update.message.reply_text("Произошла ошибка при регистрации. Пожалуйста, попробуйте позже.")

# Функция для получения роли пользователя
async def get_user_role_from_db(user_id):
    """
    Получает роль пользователя из базы данных.
    """
    try:
        start_time = time.time()
        role = await get_user_role(user_id)
        elapsed_time = time.time() - start_time
        logger.info(
            f"[КРОТ]: Роль пользователя с ID {user_id}: {role} "
            f"(Время выполнения: {elapsed_time:.4f} сек)"
        )
        return role
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при получении роли пользователя: {e}")
        return None

# Функция для добавления пользователя
async def add_user_to_db(user_id, username, full_name, role_name="Operator"):
    """
    Добавляет пользователя в базу данных.
    """
    try:
        start_time = time.time()
        await add_user(
            user_id=user_id,
            username=username,
            full_name=full_name,
            role_name=role_name,
        )
        elapsed_time = time.time() - start_time
        logger.info(
            f"[КРОТ]: Пользователь {user_id} добавлен с ролью {role_name} "
            f"(Время выполнения: {elapsed_time:.4f} сек)."
        )
        return True
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при добавлении пользователя: {e}")
        return False

# Функция для получения пароля пользователя
async def get_user_password_from_db(user_id):
    """
    Получает пароль пользователя из базы данных.
    """
    try:
        start_time = time.time()
        password = await get_user_password(user_id)
        elapsed_time = time.time() - start_time
        if password:
            logger.info(
                f"[КРОТ]: Пароль для пользователя с ID {user_id} успешно получен "
                f"(Время выполнения: {elapsed_time:.4f} сек)."
            )
        else:
            logger.warning(
                f"[КРОТ]: Пароль для пользователя с ID {user_id} не найден "
                f"(Время выполнения: {elapsed_time:.4f} сек)."
            )
        return password
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при получении пароля пользователя: {e}")
        return None

async def execute_async_query(
    query: str,
    params: Optional[tuple[Any, ...]] = None,
    *,
    fetchone: bool = False,
    fetchall: bool = True,
    retries: int = 3,
) -> Optional[list[dict[str, Any]]]:
    """
    Выполнение SQL-запроса с обработкой ошибок и повторными попытками.

    :param query: SQL-запрос для выполнения.
    :param params: Параметры для запроса.
    :param retries: Количество попыток при ошибке.
    :return: Результат выполнения запроса в виде списка словарей или None при ошибке.
    """
    try:
        start_time = time.time()
        result = await execute_query(
            query,
            params=params,
            fetchone=fetchone,
            fetchall=fetchall,
            retries=retries,
        )
        elapsed_time = time.time() - start_time
        if fetchone:
            logger.info(
                f"[КРОТ]: Запрос выполнен (одна запись). Время: {elapsed_time:.4f} сек"
            )
        else:
            size = len(result) if isinstance(result, list) else 0
            logger.info(
                f"[КРОТ]: Запрос выполнен. Записей получено: {size} "
                f"(Время выполнения: {elapsed_time:.4f} сек)"
            )
        return result
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при выполнении запроса '{query}': {e}")
        return None
