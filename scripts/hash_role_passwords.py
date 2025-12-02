# **хеш не используем сейчас ибо это геморно пока настраивать хеширование,
# надо писать скрипт еще один для замены реальных паролей на их хеш версии
import bcrypt
import asyncio
import asyncpg  # используем asyncpg для работы с PostgreSQL

from app.config import DB_CONFIG

# Конфигурация подключения к базе данных через модуль config
pg_config = {
    "host": DB_CONFIG["host"],
    "port": int(DB_CONFIG.get("port", 5432)),
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "database": DB_CONFIG["db"],
}

# Пароли для ролей, которые нужно захэшировать
roles = {
    1: "Z_]L@kjsT7C\"",
    2: "sY%|KOEy~Op|",
    3: "fhuB2os6~#c7",
    4: "u.0k`Py11t\\;",
    5: "~\\?hi%yK{_[1",
    6: "Tp$qlv6MO[H7",
    7: "+M$'T}]$<7YW"
}

async def hash_role_passwords():
    try:
        # Подключение к базе данных
        conn = await asyncpg.connect(**pg_config)
        for role_id, password in roles.items():
            # Хэшируем пароль с использованием bcrypt
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            # Обновляем пароль в базе данных
            query = "UPDATE RolesTelegaBot SET role_password = $1 WHERE id = $2"
            await conn.execute(query, hashed_password, role_id)
            print(f"Роль {role_id} успешно обновлена с хэшированным паролем.")
        await conn.close()
    except Exception as e:
        print(f"Ошибка при хэшировании паролей: {e}")

# Запуск хэширования
asyncio.run(hash_role_passwords())
