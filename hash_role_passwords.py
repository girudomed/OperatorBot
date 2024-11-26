# **хеш не используем сейчас ибо это геморно пока настраивать хеширование, 
# надо писать скрипт еще один для замены реальных паролей на их хеш версии
import bcrypt
import asyncio
import asyncpg  # используем asyncpg для работы с PostgreSQL
from dotenv import load_dotenv
import os

# Загружаем переменные окружения из .env файла
load_dotenv()

# Конфигурация подключения к базе данных через переменные окружения
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
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
        conn = await asyncpg.connect(**db_config)
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
