#bot.py
import os
import sys
import json
import base64
import hmac
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters
from supabase import create_client, Client
from PIL import Image
from pyzbar.pyzbar import decode
from pathlib import Path

# Добавляем корневую директорию проекта в sys.path для корректных импортов
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

# Теперь импортируем utils
import utils.httpx_proxy_patch

# Загрузка переменных окружения (только если файл .env доступен)
try:
    load_dotenv()
except (PermissionError, FileNotFoundError) as e:
    logging.info(f"Не удалось загрузить .env файл: {e}. Используем переменные окружения из Docker.")

# Московское время (UTC+3)
MOSCOW_TZ = timezone(timedelta(hours=3))

def get_moscow_time():
    """Получить текущее время в Москве"""
    return datetime.now(MOSCOW_TZ)

def get_moscow_timestamp():
    """Получить timestamp московского времени"""
    return int(get_moscow_time().timestamp())

# Настройки логирования
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO
)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
QR_SECRET = os.environ.get("QR_SECRET")
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")

if not all([TELEGRAM_TOKEN, QR_SECRET, SUPABASE_URL, SUPABASE_KEY]):
    raise Exception("Не хватает переменных окружения в .env для запуска бота и подключения к Supabase!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def verify_signature(branch_id, time_window, signature):
    if not QR_SECRET:
        logging.error("QR_SECRET не установлен")
        return False
    
    try:
        msg = f"{branch_id}:{time_window}".encode()
        secret = QR_SECRET.encode()
        expected = hmac.new(secret, msg, hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)
    except Exception as e:
        logging.exception("Ошибка проверки подписи QR-кода:")
        return False

from telegram import InlineKeyboardMarkup, InlineKeyboardButton

ADMIN_USERNAME = "gayazking"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id
    first_name = getattr(user, "first_name", "")
    last_name = getattr(user, "last_name", "")
    username = getattr(user, "username", "")
    chat_id = update.message.chat.id

    # Проверка: есть ли пользователь в users и одобрен ли он
    user_query = supabase.table("users").select("*").eq("telegram_id", user_id).execute()
    user_data = user_query.data[0] if user_query.data and len(user_query.data) > 0 else None

    # Если это суперпользователь (по username), всегда разрешаем и обновляем/создаём запись
    if username == ADMIN_USERNAME:
        if not user_data:
            supabase.table("users").insert({
                "telegram_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "username": username,
                "chat_id": chat_id,
                "status": "approved",
                "role": "superuser",
                "is_superuser": True,
                "can_approve_registrations": True
            }).execute()
        else:
            supabase.table("users").update({
                "first_name": first_name,
                "last_name": last_name,
                "chat_id": chat_id,
                "status": "approved",
                "role": "superuser",
                "is_superuser": True,
                "can_approve_registrations": True
            }).eq("telegram_id", user_id).execute()
        
        # Меню для суперпользователя
        from telegram import ReplyKeyboardMarkup, KeyboardButton
        keyboard = ReplyKeyboardMarkup([
            ["📊 Моя статистика", "📋 Меню"],
            ["👑 Админ-панель", "❓ Помощь"]
        ], resize_keyboard=True)
        await update.message.reply_text(
            "Вы авторизованы как суперпользователь.",
            reply_markup=keyboard
        )
        return

    # Если пользователь есть и одобрен
    if user_data and user_data.get("status") == "approved":
        from telegram import ReplyKeyboardMarkup, KeyboardButton
        keyboard = ReplyKeyboardMarkup([
            ["📊 Моя статистика", "📋 Меню"],
            ["❓ Помощь"]
        ], resize_keyboard=True)
        await update.message.reply_text(
            "Вы авторизованы. Можете сканировать QR-коды или использовать меню.",
            reply_markup=keyboard
        )
        return

    # Если пользователь есть, но не одобрен
    if user_data and user_data.get("status") == "pending":
        await update.message.reply_text("Ваша заявка на регистрацию ожидает подтверждения администратора.")
        return
    if user_data and user_data.get("status") == "declined":
        await update.message.reply_text("Ваша заявка на регистрацию была отклонена администратором.")
        return

    # Если пользователя нет — запрашиваем номер телефона
    from telegram import ReplyKeyboardMarkup, KeyboardButton
    keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("Отправить номер телефона", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await update.message.reply_text(
        "Для регистрации отправьте свой номер телефона, нажав на кнопку ниже.",
        reply_markup=keyboard
    )

async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id
    first_name = getattr(user, "first_name", "")
    last_name = getattr(user, "last_name", "")
    username = getattr(user, "username", "")
    chat_id = update.message.chat.id
    phone = update.message.contact.phone_number if update.message.contact else None

    # Проверка: есть ли пользователь в users
    user_query = supabase.table("users").select("*").eq("telegram_id", user_id).execute()
    user_data = user_query.data[0] if user_query.data and len(user_query.data) > 0 else None

    if user_data:
        await update.message.reply_text("Вы уже отправили заявку или зарегистрированы.")
        return

    # Сохранить данные телефона в контексте и запросить ФИО
    context.user_data['registration_data'] = {
        "telegram_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "username": username,
        "phone": phone,
        "chat_id": chat_id,
        "status": "pending",
        "role": "user"
    }
    
    # Установить состояние ожидания ФИО
    context.user_data['waiting_for_full_name'] = True
    
    from telegram import ReplyKeyboardRemove
    await update.message.reply_text(
        "📝 **Регистрация - Шаг 2 из 3**\n\n"
        "Теперь введите ваше полное ФИО (Фамилия Имя Отчество):\n\n"
        "Пример: Иванов Иван Иванович",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode='Markdown'
    )

async def handle_full_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Обработка ввода ФИО"""
    try:
        # Проверка наличия данных регистрации
        if 'registration_data' not in context.user_data:
            await update.message.reply_text(
                "❌ Данные регистрации не найдены. Начните регистрацию заново с команды /start"
            )
            # Очистить состояние
            context.user_data.pop('waiting_for_full_name', None)
            return
        
        # Проверка формата ФИО (минимум 2 слова)
        name_parts = text.strip().split()
        if len(name_parts) < 2:
            await update.message.reply_text(
                "❌ Пожалуйста, введите полное ФИО (минимум Фамилия и Имя).\n\n"
                "Пример: Иванов Иван Иванович"
            )
            return
        
        # Проверка на корректность (только буквы, дефисы и пробелы)
        import re
        if not re.match(r'^[а-яёА-ЯЁa-zA-Z\s\-]+$', text.strip()):
            await update.message.reply_text(
                "❌ ФИО должно содержать только буквы, пробелы и дефисы.\n\n"
                "Пожалуйста, введите корректное ФИО:"
            )
            return
        
        # Сохранить ФИО и запросить дату рождения
        context.user_data['registration_data']['full_name'] = text.strip()
        context.user_data.pop('waiting_for_full_name', None)
        context.user_data['waiting_for_birth_date'] = True
        
        await update.message.reply_text(
            "📅 **Регистрация - Шаг 3 из 3**\n\n"
            "Введите вашу дату рождения в формате ДД.ММ.ГГГГ:\n\n"
            "Пример: 15.03.1990",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logging.exception("Ошибка обработки ФИО:")
        # Очистить состояние при ошибке
        context.user_data.pop('waiting_for_full_name', None)
        await update.message.reply_text(
            "❌ Ошибка обработки данных. Начните регистрацию заново с команды /start"
        )

async def handle_birth_date_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Обработка ввода даты рождения"""
    try:
        # Проверка формата даты
        import re
        from datetime import datetime, date
        
        date_pattern = r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$'
        match = re.match(date_pattern, text.strip())
        
        if not match:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используйте формат ДД.ММ.ГГГГ\n\n"
                "Пример: 15.03.1990"
            )
            return
        
        day, month, year = map(int, match.groups())
        
        # Проверка корректности даты
        try:
            birth_date = date(year, month, day)
        except ValueError:
            await update.message.reply_text(
                "❌ Некорректная дата. Проверьте правильность введенной даты.\n\n"
                "Пример: 15.03.1990"
            )
            return
        
        # Проверка возраста (должен быть от 14 до 100 лет)
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        
        if age < 14:
            await update.message.reply_text(
                "❌ Возраст должен быть не менее 14 лет."
            )
            return
        
        if age > 100:
            await update.message.reply_text(
                "❌ Проверьте правильность введенной даты рождения."
            )
            return
        
        # Сохранить дату рождения и показать подтверждение
        context.user_data['registration_data']['birth_date'] = birth_date.isoformat()
        context.user_data.pop('waiting_for_birth_date', None)
        
        # Показать данные для подтверждения
        reg_data = context.user_data['registration_data']
        
        # Функция для безопасного экранирования текста для Markdown (без точек для дат)
        def escape_markdown_text(text, is_date=False):
            """Экранирует специальные символы Markdown, исключая точки для дат"""
            if not text:
                return ""
            
            # Базовые символы для экранирования
            escaped = text.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('!', '\\!')
            
            # Экранируем точки только если это НЕ дата
            if not is_date:
                escaped = escaped.replace('.', '\\.')
            
            return escaped
        
        # Экранируем данные с учетом типа
        full_name_escaped = escape_markdown_text(reg_data['full_name'])
        phone_escaped = escape_markdown_text(reg_data['phone'])
        username_escaped = escape_markdown_text(reg_data['username'])
        date_escaped = escape_markdown_text(text.strip(), is_date=True)  # НЕ экранируем точки в дате
        
        confirmation_text = f"""
✅ *Проверьте введенные данные:*

👤 *ФИО:* {full_name_escaped}
📅 *Дата рождения:* {date_escaped}
📱 *Телефон:* {phone_escaped}
🆔 *Username:* @{username_escaped}

Все данные указаны верно?
        """
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Все верно", callback_data="confirm_registration"),
                InlineKeyboardButton("❌ Есть ошибки", callback_data="restart_registration")
            ]
        ])
        
        await update.message.reply_text(
            confirmation_text,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logging.exception("Ошибка обработки даты рождения:")
        await update.message.reply_text("❌ Ошибка обработки данных. Попробуйте еще раз.")

async def handle_registration_confirmation(query, context):
    """Обработка подтверждения регистрации"""
    try:
        reg_data = context.user_data.get('registration_data')
        if not reg_data:
            await query.edit_message_text("❌ Данные регистрации не найдены. Начните регистрацию заново с команды /start")
            return
        
        # Проверить, не существует ли уже пользователь с таким telegram_id
        existing_user = supabase.table("users").select("*").eq("telegram_id", reg_data['telegram_id']).execute()
        if existing_user.data:
            await query.edit_message_text("❌ Пользователь с таким ID уже зарегистрирован. Ожидайте подтверждения администратора.")
            context.user_data.pop('registration_data', None)
            return
        
        # Сохранить пользователя в базу данных
        try:
            supabase.table("users").insert(reg_data).execute()
        except Exception as db_error:
            logging.exception("Ошибка вставки пользователя в базу данных:")
            await query.edit_message_text("❌ Ошибка сохранения данных в базе. Попробуйте зарегистрироваться заново с команды /start")
            context.user_data.pop('registration_data', None)
            return
        
        # Найти chat_id админа (по username)
        admin_query = supabase.table("users").select("*").eq("username", ADMIN_USERNAME).execute()
        admin = admin_query.data[0] if admin_query.data and len(admin_query.data) > 0 else None
        admin_chat_id = admin["chat_id"] if admin and admin.get("chat_id") else None

        # Кнопки для подтверждения/отклонения
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Принять", callback_data=f"approve_{reg_data['telegram_id']}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"decline_{reg_data['telegram_id']}")
            ]
        ])
        
        if admin_chat_id and admin_chat_id != 0:
            # Функция для безопасного экранирования текста для Markdown (без точек для дат)
            def escape_markdown_admin(text, is_date=False):
                """Экранирует специальные символы Markdown, исключая точки для дат"""
                if not text:
                    return ""
                
                # Базовые символы для экранирования
                escaped = text.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('!', '\\!')
                
                # Экранируем точки только если это НЕ дата
                if not is_date:
                    escaped = escaped.replace('.', '\\.')
                
                return escaped
            
            # Конвертировать дату из ISO формата в пользовательский формат
            try:
                from datetime import date
                birth_date_iso = reg_data['birth_date']  # Формат: "2000-11-02"
                birth_date_obj = date.fromisoformat(birth_date_iso)
                birth_date_display = birth_date_obj.strftime('%d.%m.%Y')  # Формат: "02.11.2000"
            except Exception as e:
                logging.exception("Ошибка конвертации даты:")
                birth_date_display = reg_data['birth_date']  # Fallback к исходному формату
            
            # Экранируем данные с учетом типа
            full_name_escaped = escape_markdown_admin(reg_data['full_name'])
            birth_date_escaped = escape_markdown_admin(birth_date_display, is_date=True)  # НЕ экранируем точки в дате
            phone_escaped = escape_markdown_admin(reg_data['phone'])
            username_escaped = escape_markdown_admin(reg_data['username'])
            
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=f"""📋 *Новая заявка на регистрацию:*

👤 *ФИО:* {full_name_escaped}
📅 *Дата рождения:* {birth_date_escaped}
📱 *Телефон:* {phone_escaped}
🆔 *Username:* @{username_escaped}
📝 *Telegram ID:* {reg_data['telegram_id']}
💬 *Chat ID:* {reg_data['chat_id']}""",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            
            await query.edit_message_text(
                "✅ **Регистрация завершена!**\n\n"
                "Ваша заявка отправлена администратору. Ожидайте подтверждения.",
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                "✅ **Регистрация завершена!**\n\n"
                "Ваша заявка создана, но администратор ещё не активировал бота. "
                "Попросите администратора (username: gayazking) запустить /start в этом боте для получения заявок."
            )
        
        # Очистить данные регистрации
        context.user_data.pop('registration_data', None)
        
    except Exception as e:
        logging.exception("Ошибка подтверждения регистрации:")
        await query.edit_message_text("❌ Ошибка при сохранении данных. Попробуйте еще раз.")

async def handle_registration_restart(query, context):
    """Обработка перезапуска регистрации"""
    try:
        # Сохранить базовые данные пользователя
        user = query.from_user
        user_id = user.id
        first_name = getattr(user, "first_name", "")
        last_name = getattr(user, "last_name", "")
        username = getattr(user, "username", "")
        chat_id = query.message.chat.id
        
        # Получить номер телефона из старых данных регистрации
        old_reg_data = context.user_data.get('registration_data', {})
        phone = old_reg_data.get('phone', '')
        
        # Пересоздать структуру данных регистрации с базовой информацией
        context.user_data['registration_data'] = {
            "telegram_id": user_id,
            "first_name": first_name,
            "last_name": last_name,
            "username": username,
            "phone": phone,
            "chat_id": chat_id,
            "status": "pending",
            "role": "user"
        }
        
        # Установить состояние ожидания ФИО
        context.user_data['waiting_for_full_name'] = True
        
        await query.edit_message_text(
            "🔄 **Начинаем заново**\n\n"
            "📝 Введите ваше полное ФИО (Фамилия Имя Отчество):\n\n"
            "Пример: Иванов Иван Иванович",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logging.exception("Ошибка перезапуска регистрации:")
        await query.edit_message_text("❌ Ошибка. Попробуйте еще раз.")

async def get_last_event_type(user_id):
    """Получить тип последнего события пользователя"""
    try:
        result = supabase.table("time_events").select("event_type").eq("telegram_id", user_id).order("event_time", desc=True).limit(1).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]["event_type"]
        return None
    except Exception as e:
        logging.exception("Ошибка получения последнего события:")
        return None

async def get_last_arrival_branch(user_id):
    """Получить филиал последнего прихода без соответствующего ухода"""
    try:
        result = supabase.table("time_events").select("*").eq("telegram_id", user_id).eq("event_type", "arrival").order("event_time", desc=True).limit(1).execute()
        if result.data and len(result.data) > 0:
            arrival_event = result.data[0]
            # Проверить, есть ли событие ухода после этого прихода
            departure_result = supabase.table("time_events").select("*").eq("telegram_id", user_id).eq("event_type", "departure").gt("event_time", arrival_event["event_time"]).limit(1).execute()
            if not departure_result.data:
                return arrival_event["branch_id"]
        return None
    except Exception as e:
        logging.exception("Ошибка получения филиала последнего прихода:")
        return None

async def check_user_authorization(user_id):
    """Проверить авторизацию пользователя"""
    try:
        user_query = supabase.table("users").select("*").eq("telegram_id", user_id).execute()
        if not user_query.data:
            logging.info(f"Пользователь {user_id} не найден в базе данных")
            return False
        
        user_data = user_query.data[0]
        is_authorized = user_data.get("status") == "approved"
        logging.info(f"Проверка авторизации пользователя {user_id}: статус={user_data.get('status')}, авторизован={is_authorized}")
        return is_authorized
        
    except Exception as e:
        logging.exception(f"Ошибка проверки авторизации пользователя {user_id}: {e}")
        # В случае ошибки базы данных, не блокируем пользователя полностью
        # Возвращаем False, но логируем ошибку для отладки
        return False

async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка главного меню"""
    user_id = update.message.from_user.id
    
    # Проверка авторизации
    if not await check_user_authorization(user_id):
        await update.message.reply_text("Вы не авторизованы для работы с системой.")
        return
    
    # Получить последнее событие для показа статуса
    last_event_type = await get_last_event_type(user_id)
    status_text = ""
    if last_event_type == "arrival":
        status_text = "🟢 Статус: На работе"
    elif last_event_type == "departure":
        status_text = "🔴 Статус: Не на работе"
    else:
        status_text = "⚪ Статус: Нет записей"
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Статистика", callback_data="menu_stats"),
            InlineKeyboardButton("📋 Мой статус", callback_data="menu_status")
        ],
        [
            InlineKeyboardButton("❓ Помощь", callback_data="menu_help"),
            InlineKeyboardButton("🔄 Обновить", callback_data="menu_refresh")
        ]
    ])
    
    await update.message.reply_text(
        f"📋 Главное меню\n\n{status_text}\n\nВыберите действие:",
        reply_markup=keyboard
    )

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка запроса помощи"""
    help_text = """
❓ **Справка по системе учета времени**

**Как работать с системой:**

🔹 **Отметка прихода/ухода:**
   • Сфотографируйте QR-код с терминала
   • Отправьте фото в этот чат
   • Убедитесь, что QR-код четко виден и не размыт

🔹 **Требования к фото:**
   • Хорошее освещение
   • QR-код полностью в кадре
   • Четкое изображение без размытия
   • Держите телефон ровно

🔹 **Логика прихода/ухода:**
   • Первое сканирование - только "Пришел"
   • После прихода - только "Ушел"
   • Уход возможен только с того же филиала

🔹 **Статистика:**
   • Просмотр отработанных часов за период
   • Детальная информация по дням
   • Отметки об автозакрытии

🔹 **Автозакрытие:**
   • Если не отсканировали уход до 21:00
   • Система автоматически закроет день
   • Будет учтено 8 часов работы

**Команды:**
• 📊 Моя статистика - отчеты за период
• 📋 Меню - главное меню системы
• ❓ Помощь - эта справка

**Поддержка:**
При проблемах обратитесь к администратору.
    """
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📞 Написать разработчику", callback_data="contact_developer"),
            InlineKeyboardButton("🐛 Сообщить об ошибке", callback_data="report_bug")
        ]
    ])
    
    await update.message.reply_text(help_text, reply_markup=keyboard, parse_mode='Markdown')

async def handle_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка админ-панели для суперпользователя"""
    user_id = update.message.from_user.id
    username = getattr(update.message.from_user, "username", "")
    
    # Проверка прав суперпользователя
    if username != ADMIN_USERNAME:
        await update.message.reply_text("У вас нет прав доступа к админ-панели.")
        return
    
    # Получить статистику пользователей
    users_result = supabase.table("users").select("*").execute()
    total_users = len(users_result.data) if users_result.data else 0
    pending_users = len([u for u in users_result.data if u.get("status") == "pending"]) if users_result.data else 0
    approved_users = len([u for u in users_result.data if u.get("status") == "approved"]) if users_result.data else 0
    admins = len([u for u in users_result.data if u.get("role") == "admin"]) if users_result.data else 0
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 LIVE Dashboard", callback_data="admin_dashboard"),
            InlineKeyboardButton("👥 Управление пользователями", callback_data="admin_users")
        ],
        [
            InlineKeyboardButton("👑 Управление админами", callback_data="admin_admins"),
            InlineKeyboardButton("📈 Статистика системы", callback_data="admin_stats")
        ],
        [
            InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")
        ]
    ])
    
    admin_text = f"""
👑 **Админ-панель**

📈 **Статистика:**
• Всего пользователей: {total_users}
• Ожидают подтверждения: {pending_users}
• Одобренных: {approved_users}
• Администраторов: {admins}

Выберите действие:
    """
    
    await update.message.reply_text(admin_text, reply_markup=keyboard, parse_mode='Markdown')

async def handle_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка запроса статистики"""
    user_id = update.message.from_user.id
    
    # Проверка авторизации
    if not await check_user_authorization(user_id):
        await update.message.reply_text("Вы не авторизованы для работы с системой.")
        return
    
    # Кнопки выбора периода
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 3 дня", callback_data="stats_3"),
            InlineKeyboardButton("📅 7 дней", callback_data="stats_7")
        ],
        [
            InlineKeyboardButton("📅 14 дней", callback_data="stats_14"),
            InlineKeyboardButton("📅 30 дней", callback_data="stats_30")
        ]
    ])
    await update.message.reply_text(
        "📊 Выберите период для отчета:",
        reply_markup=keyboard
    )

async def generate_statistics_report(user_id, days):
    """Генерация отчета по статистике пользователя"""
    from datetime import timedelta
    
    try:
        # Период для отчета
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Получить все события пользователя за период
        events_result = supabase.table("time_events").select("*").eq("telegram_id", user_id).gte("event_time", start_date.isoformat()).lte("event_time", end_date.isoformat()).order("event_time", desc=False).execute()
        
        if not events_result.data:
            return f"📊 Отчет за {days} дней\n\nДанных за указанный период не найдено."
        
        events = events_result.data
        
        # Группировка по дням
        daily_stats = {}
        total_hours = 0
        
        for event in events:
            event_date = datetime.fromisoformat(event["event_time"]).date()
            date_str = event_date.strftime("%d.%m.%Y")
            
            if date_str not in daily_stats:
                daily_stats[date_str] = {
                    "arrival": None,
                    "departure": None,
                    "branch_name": None,
                    "hours": 0,
                    "auto_closed": False
                }
            
            if event["event_type"] == "arrival":
                daily_stats[date_str]["arrival"] = datetime.fromisoformat(event["event_time"])
                daily_stats[date_str]["branch_name"] = event["branch_name"]
            elif event["event_type"] == "departure":
                daily_stats[date_str]["departure"] = datetime.fromisoformat(event["event_time"])
                if event.get("work_hours"):
                    daily_stats[date_str]["hours"] = float(event["work_hours"])
                    total_hours += daily_stats[date_str]["hours"]
                if event.get("is_auto_closed"):
                    daily_stats[date_str]["auto_closed"] = True
        
        # Формирование отчета
        report = f"📊 Отчет за {days} дней\n"
        report += f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
        
        if not daily_stats:
            report += "Данных за указанный период не найдено."
            return report
        
        # Сортировка по дате (новые сверху)
        sorted_dates = sorted(daily_stats.keys(), key=lambda x: datetime.strptime(x, "%d.%m.%Y"), reverse=True)
        
        for date_str in sorted_dates:
            day_data = daily_stats[date_str]
            report += f"📅 {date_str}\n"
            
            if day_data["branch_name"]:
                report += f"🏢 Филиал: {day_data['branch_name']}\n"
            
            if day_data["arrival"]:
                report += f"🟢 Приход: {day_data['arrival'].strftime('%H:%M')}\n"
            else:
                report += "🟢 Приход: не зафиксирован\n"
            
            if day_data["departure"]:
                auto_text = " (автозакрытие)" if day_data["auto_closed"] else ""
                report += f"🔴 Уход: {day_data['departure'].strftime('%H:%M')}{auto_text}\n"
            else:
                report += "🔴 Уход: не зафиксирован\n"
            
            if day_data["hours"] > 0:
                report += f"⏱ Часов: {day_data['hours']}\n"
            else:
                report += "⏱ Часов: 0\n"
            
            report += "\n"
        
        report += f"📈 Итого часов за период: {total_hours}\n"
        report += f"📊 Среднее в день: {round(total_hours / len([d for d in daily_stats.values() if d['hours'] > 0]) if any(d['hours'] > 0 for d in daily_stats.values()) else 0, 2)}"
        
        return report
        
    except Exception as e:
        logging.exception("Ошибка генерации отчета:")
        return "Ошибка при генерации отчета. Сообщите администратору."

async def handle_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.message.from_user
    user_id = user.id
    first_name = getattr(user, "first_name", "")
    last_name = getattr(user, "last_name", "")
    username = getattr(user, "username", "")
    chat_id = update.message.chat.id

    # Проверка на команды меню
    if text == "📊 Моя статистика":
        await handle_statistics(update, context)
        return
    elif text == "📋 Меню":
        await handle_menu(update, context)
        return
    elif text == "❓ Помощь":
        await handle_help(update, context)
        return
    elif text == "👑 Админ-панель":
        await handle_admin_panel(update, context)
        return

    # Проверка на ожидание сообщения разработчику
    if context.user_data.get('waiting_for_developer_message'):
        await handle_developer_message(update, context, text)
        return
    
    # Проверка на ожидание сообщения об ошибке
    if context.user_data.get('waiting_for_bug_report'):
        await handle_bug_report(update, context, text)
        return

    # Проверка на ожидание ФИО
    if context.user_data.get('waiting_for_full_name'):
        await handle_full_name_input(update, context, text)
        return
    
    # Проверка на ожидание даты рождения
    if context.user_data.get('waiting_for_birth_date'):
        await handle_birth_date_input(update, context, text)
        return

    # Проверка авторизации пользователя
    if not await check_user_authorization(user_id):
        await update.message.reply_text("Вы не авторизованы для работы с системой. Обратитесь к администратору.")
        return

    if not text.startswith("/qr_"):
        await update.message.reply_text("Отправьте QR-код, полученный на терминале.")
        return

    base64_data = text[len("/qr_"):]
    try:
        json_str = base64.urlsafe_b64decode(base64_data.encode()).decode()
        data = json.loads(json_str)
    except Exception as e:
        logging.exception("Ошибка декодирования QR:")
        await update.message.reply_text("Ошибка в QR-коде (не удалось декодировать данные).")
        return

    branch_id = data.get("branch_id")
    branch_name = data.get("branch_name")
    timestamp = data.get("timestamp")
    expires = data.get("expires")
    signature = data.get("signature")

    # Проверка подписи
    if not verify_signature(branch_id, timestamp, signature):
        await update.message.reply_text("QR-код недействителен (ошибка подписи).")
        return

    # Проверка срока действия (60 секунд) - используем московское время
    now_ts = get_moscow_timestamp()
    logging.info(f"Проверка времени (МСК): текущее={now_ts}, истекает={expires}, разница={now_ts - expires}")
    logging.info(f"QR данные: branch_id={branch_id}, timestamp={timestamp}, expires={expires}, signature={signature}")
    
    if now_ts > expires:
        await update.message.reply_text(f"Этот QR-код уже истёк. Попробуйте еще раз.\nТекущее время (МСК): {now_ts}, код истёк: {expires}, разница: {now_ts - expires} сек")
        return

    # Сохранить данные QR в контексте для последующего использования
    context.user_data['pending_qr'] = {
        "telegram_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "username": username,
        "chat_id": chat_id,
        "branch_id": branch_id,
        "branch_name": branch_name,
        "event_time": get_moscow_time().isoformat(),
        "qr_timestamp": timestamp,
        "signature": signature,
        "raw_json": json.dumps(data, ensure_ascii=False)
    }

    # Определить следующий возможный тип события
    last_event_type = await get_last_event_type(user_id)
    
    if last_event_type is None:
        # Первое событие - только приход
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить приход", callback_data="event_arrival_confirm"),
                InlineKeyboardButton("❌ Отклонить", callback_data="event_arrival_cancel")
            ]
        ])
        await update.message.reply_text(
            f"📍 QR-код филиала '{branch_name}' успешно отсканирован.\n\n"
            f"🟢 **Отметить приход?**\n"
            f"Филиал: {branch_name}\n"
            f"Время: {get_moscow_time().strftime('%H:%M:%S')}",
            reply_markup=keyboard
        )
    elif last_event_type == "departure":
        # Последнее событие - уход, значит следующее - приход
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить приход", callback_data="event_arrival_confirm"),
                InlineKeyboardButton("❌ Отклонить", callback_data="event_arrival_cancel")
            ]
        ])
        await update.message.reply_text(
            f"📍 QR-код филиала '{branch_name}' успешно отсканирован.\n\n"
            f"🟢 **Отметить приход?**\n"
            f"Филиал: {branch_name}\n"
            f"Время: {get_moscow_time().strftime('%H:%M:%S')}",
            reply_markup=keyboard
        )
    elif last_event_type == "arrival":
        # Последнее событие - приход, значит следующее - уход
        # Проверить, что филиал совпадает с филиалом прихода
        last_arrival_branch = await get_last_arrival_branch(user_id)
        if last_arrival_branch and last_arrival_branch != branch_id:
            # Получить название филиала прихода
            branches_result = supabase.table("branches").select("name").eq("id", last_arrival_branch).execute()
            arrival_branch_name = branches_result.data[0]["name"] if branches_result.data else f"филиал {last_arrival_branch}"
            await update.message.reply_text(
                f"❌ Ошибка: Вы пришли в филиал '{arrival_branch_name}', поэтому уход должен быть зафиксирован с QR-кода того же филиала.\n\n"
                f"Текущий QR-код от филиала '{branch_name}' не подходит для ухода."
            )
            return
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить уход", callback_data="event_departure_confirm"),
                InlineKeyboardButton("❌ Отклонить", callback_data="event_departure_cancel")
            ]
        ])
        await update.message.reply_text(
            f"📍 QR-код филиала '{branch_name}' успешно отсканирован.\n\n"
            f"🔴 **Отметить уход?**\n"
            f"Филиал: {branch_name}\n"
            f"Время: {get_moscow_time().strftime('%H:%M:%S')}",
            reply_markup=keyboard
        )

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Улучшенная обработка фото с QR-кодами"""
    if not update.message.photo:
        return
    
    user_id = update.message.from_user.id
    photo_path = None
    
    try:
        # Проверка авторизации пользователя
        if not await check_user_authorization(user_id):
            await update.message.reply_text("Вы не авторизованы для работы с системой. Обратитесь к администратору.")
            return
        
        # Отправить сообщение о начале обработки
        processing_message = await update.message.reply_text("🔍 Обрабатываю фото QR-кода...")
        
        # Получить файл фото (берем самое большое разрешение)
        photo_file = await update.message.photo[-1].get_file()
        
        # Создать уникальное имя файла для избежания конфликтов
        import uuid
        import tempfile
        
        # Использовать системную временную папку для избежания проблем с правами доступа
        temp_dir = tempfile.gettempdir()
        photo_filename = f"temp_qr_{uuid.uuid4().hex[:8]}.jpg"
        photo_path = os.path.join(temp_dir, photo_filename)
        
        # Скачать фото
        await photo_file.download_to_drive(photo_path)
        logging.info(f"Фото скачано: {photo_path}, размер: {os.path.getsize(photo_path)} байт")
        
        # Открыть и обработать изображение
        img = Image.open(photo_path)
        logging.info(f"Изображение открыто: {img.size}, режим: {img.mode}")
        
        # Попробовать улучшить качество изображения для лучшего распознавания
        # Конвертировать в RGB если нужно
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Попробовать декодировать QR-код
        decoded = decode(img)
        
        if not decoded:
            # Если QR не найден, попробовать улучшить изображение
            logging.info("QR-код не найден, пробуем улучшить изображение...")
            
            # Увеличить контрастность
            from PIL import ImageEnhance
            enhancer = ImageEnhance.Contrast(img)
            img_enhanced = enhancer.enhance(2.0)
            decoded = decode(img_enhanced)
            
            if not decoded:
                # Попробовать увеличить яркость
                enhancer = ImageEnhance.Brightness(img)
                img_bright = enhancer.enhance(1.5)
                decoded = decode(img_bright)
                
                if not decoded:
                    # Попробовать изменить размер
                    width, height = img.size
                    img_resized = img.resize((width * 2, height * 2), Image.Resampling.LANCZOS)
                    decoded = decode(img_resized)
        
        # Удалить сообщение о обработке
        try:
            await processing_message.delete()
        except Exception:
            pass
        
        if not decoded:
            await update.message.reply_text(
                "❌ QR-код не найден на фото.\n\n"
                "📸 **Советы для лучшего сканирования:**\n"
                "• Убедитесь, что QR-код полностью в кадре\n"
                "• Держите камеру ровно и на достаточном расстоянии\n"
                "• Обеспечьте хорошее освещение\n"
                "• Избегайте размытия - держите телефон неподвижно\n"
                "• Попробуйте сфотографировать еще раз"
            )
            return
        
        # Получить данные QR-кода
        qr_data = decoded[0].data.decode("utf-8")
        logging.info(f"QR-код успешно декодирован: {qr_data[:50]}...")
        
        # Проверить, что это наш QR-код
        if qr_data.startswith("/qr_"):
            # Создать фейковый update для обработки QR-кода
            class FakeUser:
                def __init__(self, orig):
                    self.id = orig.id
                    self.first_name = getattr(orig, "first_name", "")
                    self.last_name = getattr(orig, "last_name", "")
                    self.username = getattr(orig, "username", "")
            
            class FakeMessage:
                def __init__(self, orig, text):
                    self.text = text
                    self.from_user = FakeUser(orig.from_user)
                    self.chat = orig.chat
                    self.chat_id = orig.chat.id if hasattr(orig.chat, "id") else None
                    self.message_id = orig.message_id
                    self._orig = orig
                
                async def reply_text(self, text, reply_markup=None, parse_mode=None):
                    await self._orig.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            
            class FakeUpdate:
                def __init__(self, orig, text):
                    self.message = FakeMessage(orig.message, text)
            
            fake_update = FakeUpdate(update, qr_data)
            await handle_qr(fake_update, context)
        else:
            await update.message.reply_text(
                "❌ QR-код не содержит ожидаемых данных.\n\n"
                "Убедитесь, что вы сканируете QR-код с терминала учета времени."
            )
            
    except Exception as e:
        logging.exception("Ошибка при обработке фото QR-кода:")
        
        # Удалить сообщение о обработке если оно есть
        try:
            if 'processing_message' in locals():
                await processing_message.delete()
        except Exception:
            pass
        
        # Отправить детальное сообщение об ошибке
        error_message = "❌ Ошибка при обработке фото QR-кода.\n\n"
        
        if "cannot identify image file" in str(e).lower():
            error_message += "Файл не является изображением или поврежден."
        elif "no module named" in str(e).lower():
            error_message += "Ошибка системы. Сообщите администратору."
        else:
            error_message += "Попробуйте сфотографировать QR-код еще раз с лучшим освещением."
        
        await update.message.reply_text(error_message)
        
    finally:
        # Удалить временный файл
        if photo_path and os.path.exists(photo_path):
            try:
                os.remove(photo_path)
                logging.info(f"Временный файл удален: {photo_path}")
            except Exception as e:
                logging.warning(f"Не удалось удалить временный файл {photo_path}: {e}")

if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, contact_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_qr))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    async def calculate_work_hours(arrival_time, departure_time):
        """Рассчитать количество рабочих часов"""
        try:
            if isinstance(arrival_time, str):
                arrival_time = datetime.fromisoformat(arrival_time.replace('Z', '+00:00'))
            if isinstance(departure_time, str):
                departure_time = datetime.fromisoformat(departure_time.replace('Z', '+00:00'))
            
            # Убираем timezone info для корректного вычитания
            if arrival_time.tzinfo is not None:
                arrival_time = arrival_time.replace(tzinfo=None)
            if departure_time.tzinfo is not None:
                departure_time = departure_time.replace(tzinfo=None)
            
            delta = departure_time - arrival_time
            hours = delta.total_seconds() / 3600
            return round(hours, 2)
        except Exception as e:
            logging.exception("Ошибка расчета рабочих часов:")
            return 0.0

    async def get_last_arrival_event(user_id):
        """Получить последнее событие прихода без соответствующего ухода"""
        try:
            result = supabase.table("time_events").select("*").eq("telegram_id", user_id).eq("event_type", "arrival").order("event_time", desc=True).limit(1).execute()
            if result.data and len(result.data) > 0:
                arrival_event = result.data[0]
                # Проверить, есть ли событие ухода после этого прихода
                departure_result = supabase.table("time_events").select("*").eq("telegram_id", user_id).eq("event_type", "departure").gt("event_time", arrival_event["event_time"]).limit(1).execute()
                if not departure_result.data:
                    return arrival_event
            return None
        except Exception as e:
            logging.exception("Ошибка получения последнего прихода:")
            return None

    # Callback handler для кнопок
    async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        user_id = query.from_user.id

        # Обработка меню
        if data.startswith("menu_"):
            action = data.split("_")[1]
            if action == "stats":
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("📅 3 дня", callback_data="stats_3"),
                        InlineKeyboardButton("📅 7 дней", callback_data="stats_7")
                    ],
                    [
                        InlineKeyboardButton("📅 14 дней", callback_data="stats_14"),
                        InlineKeyboardButton("📅 30 дней", callback_data="stats_30")
                    ]
                ])
                await query.edit_message_text("📊 Выберите период для отчета:", reply_markup=keyboard)
            elif action == "help":
                help_text = """
❓ **Справка по системе учета времени**

**Как работать с системой:**

🔹 **Сканирование QR-кода:**
   • Отправьте фото QR-кода с терминала
   • Или скопируйте текст QR-кода и отправьте

🔹 **Логика прихода/ухода:**
   • Первое сканирование - только "Пришел"
   • После прихода - только "Ушел"
   • Уход возможен только с того же филиала

🔹 **Статистика:**
   • Просмотр отработанных часов за период
   • Детальная информация по дням
   • Отметки об автозакрытии

🔹 **Автозакрытие:**
   • Если не отсканировали уход до 21:00
   • Система автоматически закроет день
   • Будет учтено 8 часов работы

**Команды:**
• 📊 Моя статистика - отчеты за период
• 📋 Меню - главное меню системы
• ❓ Помощь - эта справка

**Поддержка:**
При проблемах обратитесь к администратору.
                """
                await query.edit_message_text(help_text, parse_mode='Markdown')
            elif action == "status":
                last_event_type = await get_last_event_type(user_id)
                if last_event_type == "arrival":
                    status_text = "🟢 Статус: На работе"
                elif last_event_type == "departure":
                    status_text = "🔴 Статус: Не на работе"
                else:
                    status_text = "⚪ Статус: Нет записей"
                await query.edit_message_text(f"📋 Ваш текущий статус:\n\n{status_text}")
            elif action == "refresh":
                # Обновить меню
                last_event_type = await get_last_event_type(user_id)
                if last_event_type == "arrival":
                    status_text = "🟢 Статус: На работе"
                elif last_event_type == "departure":
                    status_text = "🔴 Статус: Не на работе"
                else:
                    status_text = "⚪ Статус: Нет записей"
                
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("📊 Статистика", callback_data="menu_stats"),
                        InlineKeyboardButton("📋 Мой статус", callback_data="menu_status")
                    ],
                    [
                        InlineKeyboardButton("❓ Помощь", callback_data="menu_help"),
                        InlineKeyboardButton("🔄 Обновить", callback_data="menu_refresh")
                    ]
                ])
                await query.edit_message_text(f"📋 Главное меню\n\n{status_text}\n\nВыберите действие:", reply_markup=keyboard)
            return

        # Обработка статистики
        if data.startswith("stats_"):
            days = int(data.split("_")[1])
            report = await generate_statistics_report(user_id, days)
            await query.edit_message_text(report)
            return

        # Обработка событий прихода/ухода
        if data.startswith("event_"):
            parts = data.split("_")
            event_type = parts[1]  # arrival или departure
            action = parts[2]  # confirm или cancel
            
            # Получить сохраненные данные QR
            pending_qr = context.user_data.get('pending_qr')
            if not pending_qr:
                await query.edit_message_text("Ошибка: данные QR-кода не найдены. Отсканируйте код заново.")
                return
            
            # Если пользователь отклонил действие
            if action == "cancel":
                await query.edit_message_text("❌ Действие отменено. QR-код не обработан.")
                context.user_data.pop('pending_qr', None)
                return
            
            # Если пользователь подтвердил действие
            if action == "confirm":
                # Подготовить данные события
                event_data = pending_qr.copy()
                event_data["event_type"] = event_type
                event_data["mood"] = 3  # Нейтральное настроение по умолчанию
            
            # Если это уход, рассчитать рабочие часы
            work_hours = None
            work_duration_text = ""
            if event_type == "departure":
                last_arrival = await get_last_arrival_event(user_id)
                if last_arrival:
                    work_hours = await calculate_work_hours(last_arrival["event_time"], event_data["event_time"])
                    event_data["work_hours"] = work_hours
                    
                    # Рассчитать точное время пребывания
                    arrival_time = datetime.fromisoformat(last_arrival["event_time"])
                    departure_time = datetime.fromisoformat(event_data["event_time"])
                    
                    # Убираем timezone info для корректного вычитания
                    if arrival_time.tzinfo is not None:
                        arrival_time = arrival_time.replace(tzinfo=None)
                    if departure_time.tzinfo is not None:
                        departure_time = departure_time.replace(tzinfo=None)
                    
                    duration = departure_time - arrival_time
                    
                    hours = int(duration.total_seconds() // 3600)
                    minutes = int((duration.total_seconds() % 3600) // 60)
                    work_duration_text = f"\n⏱ Время пребывания: {hours} ч {minutes} мин"

            # Сохранить событие в базу
            try:
                res = supabase.table("time_events").insert(event_data).execute()
                if res.data:
                    if event_type == "arrival":
                        message = f"✅ **Приход зафиксирован!**\n\n📍 Филиал: {pending_qr['branch_name']}\n🕐 Время: {datetime.fromisoformat(pending_qr['event_time']):%d.%m.%Y %H:%M:%S} МСК\n\n✨ Хорошего рабочего дня!"
                    else:
                        # Получить прогноз погоды для ухода
                        weather_forecast = await get_weather_forecast()
                        
                        hours_text = f"\n⏱ Отработано часов: {work_hours}" if work_hours else ""
                        message = f"✅ **Уход зафиксирован!**\n\n📍 Филиал: {pending_qr['branch_name']}\n🕐 Время: {datetime.fromisoformat(pending_qr['event_time']):%d.%m.%Y %H:%M:%S} МСК{work_duration_text}{hours_text}\n\n{weather_forecast}\n\n🌟 Отличной работы! До свидания!"
                    
                    await query.edit_message_text(message, parse_mode='Markdown')
                    # Очистить сохраненные данные
                    context.user_data.pop('pending_qr', None)
                else:
                    await query.edit_message_text("Ошибка сохранения данных в базе. Сообщите администратору.")
            except Exception as e:
                logging.exception("Ошибка записи события:")
                await query.edit_message_text("Ошибка сохранения данных. Сообщите администратору.")
            return

        # Обработка админ-панели
        if data.startswith("admin_"):
            admin_user = query.from_user
            if admin_user.username != ADMIN_USERNAME:
                await query.edit_message_text("У вас нет прав доступа к админ-панели.")
                return
            
            action = data.split("_")[1]
            
            if action == "users":
                await handle_admin_users(query, context)
            elif action == "admins":
                await handle_admin_admins(query, context)
            elif action == "stats":
                await handle_admin_system_stats(query, context)
            elif action == "settings":
                await handle_admin_settings(query, context)
            elif action == "dashboard":
                await handle_admin_dashboard(query, context)
            return

        # Обработка управления пользователями
        if data.startswith("user_"):
            admin_user = query.from_user
            if admin_user.username != ADMIN_USERNAME:
                await query.edit_message_text("У вас нет прав доступа.")
                return
            
            parts = data.split("_")
            action = parts[1]
            
            if action == "list":
                page = int(parts[2]) if len(parts) > 2 else 1
                await show_users_list(query, context, page)
            elif action == "promote":
                user_id = int(parts[2])
                await promote_user_to_admin(query, context, user_id)
            elif action == "demote":
                user_id = int(parts[2])
                await demote_admin_to_user(query, context, user_id)
            elif action == "delete":
                user_id = int(parts[2])
                await delete_user(query, context, user_id)
            elif action == "approve":
                user_id = int(parts[2])
                await approve_user(query, context, user_id)
            elif action == "decline":
                user_id = int(parts[2])
                await decline_user(query, context, user_id)
            return


        # Обработка связи с разработчиком и сообщений об ошибках
        if data == "contact_developer":
            await query.edit_message_text(
                "📞 **Связь с разработчиком**\n\n"
                "Для связи с разработчиком напишите @gayazking\n\n"
                "Или опишите вашу проблему здесь, и мы передадим её разработчику.",
                parse_mode='Markdown'
            )
            # Установить состояние ожидания сообщения разработчику
            context.user_data['waiting_for_developer_message'] = True
            return
        
        if data == "report_bug":
            await query.edit_message_text(
                "🐛 **Сообщить об ошибке**\n\n"
                "Опишите подробно ошибку, которую вы обнаружили:\n"
                "• Что вы делали когда произошла ошибка?\n"
                "• Какое сообщение об ошибке появилось?\n"
                "• В какое время это произошло?\n\n"
                "Напишите ваше сообщение:",
                parse_mode='Markdown'
            )
            # Установить состояние ожидания сообщения об ошибке
            context.user_data['waiting_for_bug_report'] = True
            return

        # Обработка подтверждения и перезапуска регистрации
        if data == "confirm_registration":
            await handle_registration_confirmation(query, context)
            return
        
        if data == "restart_registration":
            await handle_registration_restart(query, context)
            return

        # Обработка заявок на регистрацию (только для админа)
        admin_user = query.from_user
        if admin_user.username != ADMIN_USERNAME:
            await query.edit_message_text("Только администратор может подтверждать заявки.")
            return
            
        if data.startswith("approve_"):
            user_id = int(data.split("_")[1])
            # Обновить статус пользователя
            supabase.table("users").update({"status": "approved"}).eq("telegram_id", user_id).execute()
            # Получить chat_id пользователя
            user_query = supabase.table("users").select("*").eq("telegram_id", user_id).execute()
            user_data = user_query.data[0] if user_query.data and len(user_query.data) > 0 else None
            if user_data and user_data.get("chat_id"):
                await context.bot.send_message(
                    chat_id=user_data["chat_id"],
                    text="""✅ **Ваша заявка на регистрацию одобрена!**

📱 **Как отмечать приход и уход:**
1. Найдите QR-код на терминале в вашем филиале
2. Сфотографируйте QR-код камерой телефона
3. Отправьте фото в этот чат

📸 **Требования к фото:**
• Хорошее освещение
• QR-код полностью в кадре  
• Четкое изображение без размытия
• Держите телефон ровно

Теперь вы можете отмечать свой приход и уход!""",
                    parse_mode='Markdown'
                )
            await query.edit_message_text("Пользователь одобрен.")
        elif data.startswith("decline_"):
            user_id = int(data.split("_")[1])
            supabase.table("users").update({"status": "declined"}).eq("telegram_id", user_id).execute()
            user_query = supabase.table("users").select("*").eq("telegram_id", user_id).execute()
            user_data = user_query.data[0] if user_query.data and len(user_query.data) > 0 else None
            if user_data and user_data.get("chat_id"):
                await context.bot.send_message(
                    chat_id=user_data["chat_id"],
                    text="Ваша заявка на регистрацию отклонена администратором."
                )
            await query.edit_message_text("Пользователь отклонён.")

    # Функции админ-панели
    async def handle_admin_users(query, context):
        """Управление пользователями"""
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📋 Список пользователей", callback_data="user_list_1"),
                InlineKeyboardButton("⏳ Ожидающие подтверждения", callback_data="user_pending")
            ],
            [
                InlineKeyboardButton("🔍 Поиск пользователя", callback_data="user_search"),
                InlineKeyboardButton("📊 Статистика пользователей", callback_data="user_stats")
            ],
            [
                InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="admin_back")
            ]
        ])
        
        await query.edit_message_text(
            "👥 **Управление пользователями**\n\nВыберите действие:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )

    async def handle_admin_admins(query, context):
        """Управление администраторами"""
        # Получить список всех админов
        admins_result = supabase.table("users").select("*").eq("role", "admin").execute()
        admins_count = len(admins_result.data) if admins_result.data else 0
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("👑 Список админов", callback_data="admin_list"),
                InlineKeyboardButton("➕ Назначить админа", callback_data="admin_promote")
            ],
            [
                InlineKeyboardButton("➖ Снять права админа", callback_data="admin_demote"),
                InlineKeyboardButton("⚙️ Настройки прав", callback_data="admin_permissions")
            ],
            [
                InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="admin_back")
            ]
        ])
        
        await query.edit_message_text(
            f"👑 **Управление администраторами**\n\nТекущих админов: {admins_count}\n\nВыберите действие:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )

    async def handle_admin_system_stats(query, context):
        """Статистика системы"""
        try:
            # Получить статистику
            users_result = supabase.table("users").select("*").execute()
            events_result = supabase.table("time_events").select("*").gte("event_time", get_moscow_time().replace(hour=0, minute=0, second=0).isoformat()).execute()
            
            total_users = len(users_result.data) if users_result.data else 0
            pending_users = len([u for u in users_result.data if u.get("status") == "pending"]) if users_result.data else 0
            approved_users = len([u for u in users_result.data if u.get("status") == "approved"]) if users_result.data else 0
            admins = len([u for u in users_result.data if u.get("role") in ["admin", "superuser"]]) if users_result.data else 0
            
            # События сегодня
            today_events = len(events_result.data) if events_result.data else 0
            arrivals_today = len([e for e in events_result.data if e.get("event_type") == "arrival"]) if events_result.data else 0
            departures_today = len([e for e in events_result.data if e.get("event_type") == "departure"]) if events_result.data else 0
            currently_at_work = arrivals_today - departures_today
            
            stats_text = f"""
📊 **Статистика системы**

👥 **Пользователи:**
• Всего: {total_users}
• Одобренных: {approved_users}
• Ожидают подтверждения: {pending_users}
• Администраторов: {admins}

📈 **Активность сегодня:**
• Всего событий: {today_events}
• Приходов: {arrivals_today}
• Уходов: {departures_today}
• Сейчас на работе: {currently_at_work}

🕐 **Время:** {get_moscow_time().strftime('%d.%m.%Y %H:%M')} МСК
            """
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📊 Детальная статистика", callback_data="admin_detailed_stats"),
                    InlineKeyboardButton("📈 Экспорт отчета", callback_data="admin_export")
                ],
                [
                    InlineKeyboardButton("🔄 Обновить", callback_data="admin_stats"),
                    InlineKeyboardButton("🔙 Назад", callback_data="admin_back")
                ]
            ])
            
            await query.edit_message_text(stats_text, reply_markup=keyboard, parse_mode='Markdown')
            
        except Exception as e:
            logging.exception("Ошибка получения статистики:")
            await query.edit_message_text("Ошибка получения статистики системы.")

    async def handle_admin_settings(query, context):
        """Настройки системы"""
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⏰ Настройки времени", callback_data="settings_time"),
                InlineKeyboardButton("🔔 Уведомления", callback_data="settings_notifications")
            ],
            [
                InlineKeyboardButton("🏢 Управление филиалами", callback_data="settings_branches"),
                InlineKeyboardButton("🔐 Безопасность", callback_data="settings_security")
            ],
            [
                InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="admin_back")
            ]
        ])
        
        await query.edit_message_text(
            "⚙️ **Настройки системы**\n\nВыберите раздел для настройки:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )

    async def show_users_list(query, context, page=1):
        """Показать список пользователей с пагинацией"""
        try:
            per_page = 5
            offset = (page - 1) * per_page
            
            # Получить пользователей с пагинацией
            users_result = supabase.table("users").select("*").order("created_at", desc=True).range(offset, offset + per_page - 1).execute()
            total_result = supabase.table("users").select("id", count="exact").execute()
            
            if not users_result.data:
                await query.edit_message_text("Пользователи не найдены.")
                return
            
            total_users = total_result.count if hasattr(total_result, 'count') else len(users_result.data)
            total_pages = (total_users + per_page - 1) // per_page
            
            text = f"👥 **Список пользователей** (стр. {page}/{total_pages})\n\n"
            
            keyboard_buttons = []
            
            for user in users_result.data:
                status_emoji = {"approved": "✅", "pending": "⏳", "declined": "❌"}.get(user.get("status"), "❓")
                role_emoji = {"superuser": "👑", "admin": "👨‍💼", "user": "👤"}.get(user.get("role"), "👤")
                
                name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                username = user.get('username', 'нет')
                
                # Экранируем специальные символы для Markdown
                name_escaped = name.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
                username_escaped = username.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
                
                text += f"{status_emoji} {role_emoji} *{name_escaped}*\n"
                text += f"   @{username_escaped} \\| ID: {user.get('telegram_id')}\n"
                text += f"   Статус: {user.get('status')} \\| Роль: {user.get('role')}\n\n"
                
                # Кнопки действий для каждого пользователя
                user_buttons = []
                if user.get("status") == "pending":
                    user_buttons.extend([
                        InlineKeyboardButton("✅", callback_data=f"user_approve_{user['telegram_id']}"),
                        InlineKeyboardButton("❌", callback_data=f"user_decline_{user['telegram_id']}")
                    ])
                
                if user.get("role") == "user":
                    user_buttons.append(InlineKeyboardButton("👨‍💼", callback_data=f"user_promote_{user['telegram_id']}"))
                elif user.get("role") == "admin":
                    user_buttons.append(InlineKeyboardButton("👤", callback_data=f"user_demote_{user['telegram_id']}"))
                
                user_buttons.append(InlineKeyboardButton(f"🗑 {name[:10]}", callback_data=f"user_delete_{user['telegram_id']}"))
                
                if user_buttons:
                    keyboard_buttons.append(user_buttons)
            
            # Навигация
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"user_list_{page-1}"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"user_list_{page+1}"))
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            keyboard_buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
            
            keyboard = InlineKeyboardMarkup(keyboard_buttons)
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode='Markdown')
            
        except Exception as e:
            logging.exception("Ошибка показа списка пользователей:")
            await query.edit_message_text("Ошибка получения списка пользователей.")

    async def promote_user_to_admin(query, context, user_id):
        """Повысить пользователя до админа"""
        try:
            # Обновить роль пользователя
            result = supabase.table("users").update({
                "role": "admin",
                "can_approve_registrations": True
            }).eq("telegram_id", user_id).execute()
            
            if result.data:
                # Получить данные пользователя для уведомления
                user_data = result.data[0]
                user_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
                
                # Уведомить пользователя
                if user_data.get("chat_id"):
                    await context.bot.send_message(
                        chat_id=user_data["chat_id"],
                        text="🎉 Поздравляем! Вы назначены администратором системы. Теперь вы можете подтверждать заявки на регистрацию."
                    )
                
                await query.edit_message_text(f"✅ Пользователь {user_name} назначен администратором.")
            else:
                await query.edit_message_text("❌ Ошибка назначения администратора.")
                
        except Exception as e:
            logging.exception("Ошибка назначения админа:")
            await query.edit_message_text("❌ Ошибка назначения администратора.")

    async def demote_admin_to_user(query, context, user_id):
        """Понизить админа до обычного пользователя"""
        try:
            # Проверить, что это не суперпользователь
            user_result = supabase.table("users").select("*").eq("telegram_id", user_id).execute()
            if user_result.data and user_result.data[0].get("role") == "superuser":
                await query.edit_message_text("❌ Нельзя снять права у суперпользователя.")
                return
            
            # Обновить роль пользователя
            result = supabase.table("users").update({
                "role": "user",
                "can_approve_registrations": False
            }).eq("telegram_id", user_id).execute()
            
            if result.data:
                user_data = result.data[0]
                user_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
                
                # Уведомить пользователя
                if user_data.get("chat_id"):
                    await context.bot.send_message(
                        chat_id=user_data["chat_id"],
                        text="📢 Ваши права администратора были отозваны. Теперь у вас обычные права пользователя."
                    )
                
                await query.edit_message_text(f"✅ У пользователя {user_name} отозваны права администратора.")
            else:
                await query.edit_message_text("❌ Ошибка снятия прав администратора.")
                
        except Exception as e:
            logging.exception("Ошибка снятия прав админа:")
            await query.edit_message_text("❌ Ошибка снятия прав администратора.")

    async def delete_user(query, context, user_id):
        """Удалить пользователя"""
        try:
            # Проверить, что это не суперпользователь
            user_result = supabase.table("users").select("*").eq("telegram_id", user_id).execute()
            if user_result.data and user_result.data[0].get("role") == "superuser":
                await query.edit_message_text("❌ Нельзя удалить суперпользователя.")
                return
            
            user_data = user_result.data[0] if user_result.data else None
            if not user_data:
                await query.edit_message_text("❌ Пользователь не найден.")
                return
            
            user_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
            
            # Удалить все события пользователя
            supabase.table("time_events").delete().eq("telegram_id", user_id).execute()
            
            # Удалить пользователя
            result = supabase.table("users").delete().eq("telegram_id", user_id).execute()
            
            if result.data:
                await query.edit_message_text(f"✅ Пользователь {user_name} удален из системы.")
            else:
                await query.edit_message_text("❌ Ошибка удаления пользователя.")
                
        except Exception as e:
            logging.exception("Ошибка удаления пользователя:")
            await query.edit_message_text("❌ Ошибка удаления пользователя.")

    async def approve_user(query, context, user_id):
        """Одобрить пользователя"""
        try:
            result = supabase.table("users").update({"status": "approved"}).eq("telegram_id", user_id).execute()
            
            if result.data:
                user_data = result.data[0]
                user_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
                
                # Уведомить пользователя
                if user_data.get("chat_id"):
                    await context.bot.send_message(
                        chat_id=user_data["chat_id"],
                        text="✅ Ваша заявка на регистрацию одобрена! Теперь вы можете сканировать QR-коды."
                    )
                
                await query.edit_message_text(f"✅ Пользователь {user_name} одобрен.")
            else:
                await query.edit_message_text("❌ Ошибка одобрения пользователя.")
                
        except Exception as e:
            logging.exception("Ошибка одобрения пользователя:")
            await query.edit_message_text("❌ Ошибка одобрения пользователя.")

    async def decline_user(query, context, user_id):
        """Отклонить пользователя"""
        try:
            result = supabase.table("users").update({"status": "declined"}).eq("telegram_id", user_id).execute()
            
            if result.data:
                user_data = result.data[0]
                user_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
                
                # Уведомить пользователя
                if user_data.get("chat_id"):
                    await context.bot.send_message(
                        chat_id=user_data["chat_id"],
                        text="❌ Ваша заявка на регистрацию отклонена администратором."
                    )
                
                await query.edit_message_text(f"❌ Пользователь {user_name} отклонен.")
            else:
                await query.edit_message_text("❌ Ошибка отклонения пользователя.")
                
        except Exception as e:
            logging.exception("Ошибка отклонения пользователя:")
            await query.edit_message_text("❌ Ошибка отклонения пользователя.")

    async def handle_admin_dashboard(query, context):
        """LIVE Dashboard - мониторинг в реальном времени"""
        try:
            moscow_now = get_moscow_time()
            today_start = moscow_now.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Получить всех пользователей, которые пришли сегодня
            arrivals_today = supabase.table("time_events").select("*").eq("event_type", "arrival").gte("event_time", today_start.isoformat()).execute()
            
            # Получить всех пользователей, которые ушли сегодня
            departures_today = supabase.table("time_events").select("*").eq("event_type", "departure").gte("event_time", today_start.isoformat()).execute()
            
            # Получить филиалы
            branches_result = supabase.table("branches").select("*").execute()
            branches = {b["id"]: b["name"] for b in branches_result.data} if branches_result.data else {}
            
            # Анализ кто сейчас на работе
            currently_at_work = []
            late_arrivals = []
            
            # Группировка приходов по пользователям
            user_arrivals = {}
            for arrival in arrivals_today.data if arrivals_today.data else []:
                user_id = arrival["telegram_id"]
                if user_id not in user_arrivals or arrival["event_time"] > user_arrivals[user_id]["event_time"]:
                    user_arrivals[user_id] = arrival
            
            # Группировка уходов по пользователям
            user_departures = {}
            for departure in departures_today.data if departures_today.data else []:
                user_id = departure["telegram_id"]
                if user_id not in user_departures or departure["event_time"] > user_departures[user_id]["event_time"]:
                    user_departures[user_id] = departure
            
            # Определение кто сейчас на работе и кто опоздал
            for user_id, arrival in user_arrivals.items():
                # Проверить, есть ли уход после последнего прихода
                if user_id not in user_departures or user_departures[user_id]["event_time"] < arrival["event_time"]:
                    currently_at_work.append(arrival)
                    
                    # Проверить опоздание (после 9:00)
                    arrival_time = datetime.fromisoformat(arrival["event_time"])
                    work_start = arrival_time.replace(hour=9, minute=0, second=0, microsecond=0)
                    if arrival_time > work_start:
                        late_minutes = int((arrival_time - work_start).total_seconds() / 60)
                        arrival["late_minutes"] = late_minutes
                        late_arrivals.append(arrival)
            
            # Статистика по филиалам
            branch_stats = {}
            for arrival in currently_at_work:
                branch_id = arrival["branch_id"]
                branch_name = branches.get(branch_id, f"Филиал {branch_id}")
                if branch_name not in branch_stats:
                    branch_stats[branch_name] = 0
                branch_stats[branch_name] += 1
            
            # Формирование отчета
            dashboard_text = f"""
📊 **LIVE DASHBOARD**
🕐 **{moscow_now.strftime('%d.%m.%Y %H:%M')} МСК**

🟢 **Сейчас на работе: {len(currently_at_work)}**
🔴 **Опозданий сегодня: {len(late_arrivals)}**
📈 **Всего приходов: {len(user_arrivals)}**
📉 **Всего уходов: {len(user_departures)}**

🏢 **По филиалам:**
"""
            
            for branch_name, count in branch_stats.items():
                dashboard_text += f"• {branch_name}: {count} чел.\n"
            
            if not branch_stats:
                dashboard_text += "• Никого нет на работе\n"
            
            dashboard_text += "\n⚠️ **Опоздания сегодня:**\n"
            
            if late_arrivals:
                for late in sorted(late_arrivals, key=lambda x: x["late_minutes"], reverse=True)[:5]:
                    name = f"{late.get('first_name', '')} {late.get('last_name', '')}".strip()
                    branch_name = branches.get(late["branch_id"], f"Филиал {late['branch_id']}")
                    arrival_time = datetime.fromisoformat(late["event_time"])
                    dashboard_text += f"• {name} - {late['late_minutes']} мин\n"
                    dashboard_text += f"  {branch_name}, {arrival_time.strftime('%H:%M')}\n"
                
                if len(late_arrivals) > 5:
                    dashboard_text += f"• ... и еще {len(late_arrivals) - 5} опозданий\n"
            else:
                dashboard_text += "• Опозданий нет 🎉\n"
            
            # Последние события
            dashboard_text += "\n📋 **Последние события:**\n"
            all_events = []
            if arrivals_today.data:
                all_events.extend(arrivals_today.data)
            if departures_today.data:
                all_events.extend(departures_today.data)
            
            # Сортировка по времени (новые сверху)
            all_events.sort(key=lambda x: x["event_time"], reverse=True)
            
            for event in all_events[:5]:
                name = f"{event.get('first_name', '')} {event.get('last_name', '')}".strip()
                branch_name = branches.get(event["branch_id"], f"Филиал {event['branch_id']}")
                event_time = datetime.fromisoformat(event["event_time"])
                event_emoji = "🟢" if event["event_type"] == "arrival" else "🔴"
                event_text = "пришел" if event["event_type"] == "arrival" else "ушел"
                dashboard_text += f"{event_emoji} {name} {event_text}\n"
                dashboard_text += f"  {branch_name}, {event_time.strftime('%H:%M')}\n"
            
            if not all_events:
                dashboard_text += "• Событий сегодня нет\n"
            
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔄 Обновить", callback_data="admin_dashboard"),
                    InlineKeyboardButton("📊 Детали", callback_data="dashboard_details")
                ],
                [
                    InlineKeyboardButton("⚠️ Алерты", callback_data="dashboard_alerts"),
                    InlineKeyboardButton("📈 Экспорт", callback_data="dashboard_export")
                ],
                [
                    InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="admin_back")
                ]
            ])
            
            await query.edit_message_text(dashboard_text, reply_markup=keyboard, parse_mode='Markdown')
            
        except Exception as e:
            logging.exception("Ошибка LIVE Dashboard:")
            await query.edit_message_text("Ошибка загрузки LIVE Dashboard. Попробуйте еще раз.")

    # Функции тамагочи
    async def get_or_create_tamagotchi(user_id):
        """Получить или создать тамагочи для пользователя"""
        try:
            result = supabase.table("tamagotchi").select("*").eq("telegram_id", user_id).execute()
            if result.data:
                return result.data[0]
            else:
                # Создать нового тамагочи
                new_tamagotchi = {
                    "telegram_id": user_id,
                    "name": "Тамагочи",
                    "hunger": 100,
                    "happiness": 100,
                    "health": 100,
                    "level": 1,
                    "experience": 0,
                    "is_alive": True,
                    "last_fed": get_moscow_time().isoformat()
                }
                create_result = supabase.table("tamagotchi").insert(new_tamagotchi).execute()
                return create_result.data[0] if create_result.data else new_tamagotchi
        except Exception as e:
            logging.exception("Ошибка получения тамагочи:")
            return None

    async def update_tamagotchi_stats(user_id):
        """Обновить статистику тамагочи на основе времени"""
        try:
            tamagotchi = await get_or_create_tamagotchi(user_id)
            if not tamagotchi:
                return None
            
            # Рассчитать время с последнего кормления
            last_fed = datetime.fromisoformat(tamagotchi["last_fed"])
            if last_fed.tzinfo is not None:
                last_fed = last_fed.replace(tzinfo=None)
            
            now = get_moscow_time().replace(tzinfo=None)
            hours_since_fed = (now - last_fed).total_seconds() / 3600
            
            # Обновить показатели
            hunger = max(0, tamagotchi["hunger"] - int(hours_since_fed * 5))  # -5 за час
            happiness = max(0, tamagotchi["happiness"] - int(hours_since_fed * 3))  # -3 за час
            health = max(0, tamagotchi["health"] - int(hours_since_fed * 2))  # -2 за час
            
            # Проверить смерть (3 дня без кормления = 72 часа)
            is_alive = hours_since_fed < 72
            
            # Обновить в базе
            updated_data = {
                "hunger": hunger,
                "happiness": happiness,
                "health": health,
                "is_alive": is_alive,
                "updated_at": now.isoformat()
            }
            
            supabase.table("tamagotchi").update(updated_data).eq("telegram_id", user_id).execute()
            
            # Вернуть обновленные данные
            tamagotchi.update(updated_data)
            return tamagotchi
            
        except Exception as e:
            logging.exception("Ошибка обновления тамагочи:")
            return None

    async def feed_tamagotchi(user_id):
        """Покормить тамагочи (при сканировании QR)"""
        try:
            tamagotchi = await update_tamagotchi_stats(user_id)
            if not tamagotchi:
                return None, None
            
            # Если мертв, не кормим
            if not tamagotchi["is_alive"]:
                return tamagotchi, await get_tamagotchi_message("dead")
            
            # Покормить
            new_hunger = min(100, tamagotchi["hunger"] + 25)
            new_happiness = min(100, tamagotchi["happiness"] + 20)
            new_health = min(100, tamagotchi["health"] + 15)
            new_experience = tamagotchi["experience"] + 10
            
            # Проверить повышение уровня
            new_level = tamagotchi["level"]
            if new_experience >= new_level * 100:
                new_level += 1
                new_experience = 0
            
            # Обновить в базе
            updated_data = {
                "hunger": new_hunger,
                "happiness": new_happiness,
                "health": new_health,
                "experience": new_experience,
                "level": new_level,
                "last_fed": get_moscow_time().isoformat(),
                "updated_at": get_moscow_time().isoformat()
            }
            
            supabase.table("tamagotchi").update(updated_data).eq("telegram_id", user_id).execute()
            
            # Обновить локальные данные
            tamagotchi.update(updated_data)
            
            # Получить сообщение
            if new_level > tamagotchi["level"]:
                message = await get_tamagotchi_message("happy")
            else:
                message = await get_tamagotchi_message("feed")
            
            return tamagotchi, message
            
        except Exception as e:
            logging.exception("Ошибка кормления тамагочи:")
            return None, None

    async def get_tamagotchi_message(message_type):
        """Получить случайное сообщение тамагочи"""
        try:
            result = supabase.table("tamagotchi_messages").select("*").eq("message_type", message_type).execute()
            if result.data:
                import random
                message_data = random.choice(result.data)
                return f"{message_data['emoji']} {message_data['message']}"
            return "🐾 Тамагочи что-то говорит..."
        except Exception as e:
            logging.exception("Ошибка получения сообщения тамагочи:")
            return "🐾 Тамагочи молчит..."

    async def get_tamagotchi_status(user_id):
        """Получить статус тамагочи для отображения"""
        try:
            tamagotchi = await update_tamagotchi_stats(user_id)
            if not tamagotchi:
                return "🐾 Тамагочи не найден"
            
            if not tamagotchi["is_alive"]:
                return f"💀 {tamagotchi['name']} (Уровень {tamagotchi['level']}) - МЕРТВ\n⚰️ Воскреси меня сканированием QR!"
            
            # Определить состояние
            hunger = tamagotchi["hunger"]
            happiness = tamagotchi["happiness"]
            health = tamagotchi["health"]
            
            status_emoji = "😄"
            if hunger < 30 or happiness < 30 or health < 30:
                status_emoji = "😰"
            elif hunger < 50 or happiness < 50 or health < 50:
                status_emoji = "😐"
            
            status_text = f"""
{status_emoji} **{tamagotchi['name']}** (Уровень {tamagotchi['level']})
🍎 Сытость: {hunger}/100
😊 Счастье: {happiness}/100
❤️ Здоровье: {health}/100
⭐ Опыт: {tamagotchi['experience']}/{tamagotchi['level'] * 100}
            """.strip()
            
            return status_text
            
        except Exception as e:
            logging.exception("Ошибка получения статуса тамагочи:")
            return "🐾 Ошибка получения статуса тамагочи"

    async def revive_tamagotchi(user_id):
        """Воскресить тамагочи"""
        try:
            updated_data = {
                "hunger": 50,
                "happiness": 50,
                "health": 50,
                "is_alive": True,
                "last_fed": get_moscow_time().isoformat(),
                "updated_at": get_moscow_time().isoformat()
            }
            
            supabase.table("tamagotchi").update(updated_data).eq("telegram_id", user_id).execute()
            message = await get_tamagotchi_message("revive")
            return message
            
        except Exception as e:
            logging.exception("Ошибка воскрешения тамагочи:")
            return "🐾 Ошибка воскрешения"

    async def get_weather_forecast():
        """Получить прогноз погоды на завтра для Казани"""
        try:
            import aiohttp
            import asyncio
            
            # --- OpenWeatherMap ---------------------------------
            api_key = os.getenv("OPENWEATHER_API_KEY") or ""
            city = "Kazan,RU"

            # Если ключ не указан, сразу сообщаем о недоступности сервиса
            if not api_key:
                tomorrow = (datetime.now() + timedelta(days=1)).strftime('%d.%m')
                return f"🌤 Прогноз погоды на {tomorrow}: сервис недоступен (нет API-ключа)"
            
            async with aiohttp.ClientSession() as session:
                # Получаем прогноз на завтра
                url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric&lang=ru"
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Найти данные на завтра
                        tomorrow = datetime.now() + timedelta(days=1)
                        tomorrow_date = tomorrow.strftime('%Y-%m-%d')
                        
                        tomorrow_forecasts = []
                        for item in data['list']:
                            if tomorrow_date in item['dt_txt']:
                                tomorrow_forecasts.append(item)
                        
                        if tomorrow_forecasts:
                            # Анализ данных
                            temps = [item['main']['temp'] for item in tomorrow_forecasts]
                            feels_like = [item['main']['feels_like'] for item in tomorrow_forecasts]
                            rain_periods = []
                            
                            for item in tomorrow_forecasts:
                                if 'rain' in item or item['weather'][0]['main'] in ['Rain', 'Drizzle']:
                                    time_str = item['dt_txt'].split(' ')[1][:5]
                                    rain_periods.append(time_str)
                            
                            min_temp = int(min(temps))
                            max_temp = int(max(temps))
                            avg_feels = int(sum(feels_like) / len(feels_like))
                            
                            # --- второй запрос для восхода/заката ---
                            try:
                                lat = data["city"]["coord"]["lat"]
                                lon = data["city"]["coord"]["lon"]
                                oc_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=hourly,minutely,current,alerts&appid={api_key}"
                                async with session.get(oc_url) as resp2:
                                    if resp2.status == 200:
                                        oc = await resp2.json()
                                        tz_offset = oc.get("timezone_offset", 0)
                                        daily = oc.get("daily", [])
                                        if len(daily) > 1:
                                            sr_ts = daily[1]["sunrise"] + tz_offset
                                            ss_ts = daily[1]["sunset"] + tz_offset
                                            sunrise = datetime.utcfromtimestamp(sr_ts).strftime("%H:%M")
                                            sunset  = datetime.utcfromtimestamp(ss_ts).strftime("%H:%M")
                                        else:
                                            sunrise = sunset = "--"
                                    else:
                                        sunrise = sunset = "--"
                            except Exception:
                                sunrise = sunset = "--"
                            
                            rain_text = "без осадков"
                            if rain_periods:
                                rain_text = f"возможны с {rain_periods[0]} до {rain_periods[-1]}"
                            
                            return f"""
🌤 **Прогноз погоды на завтра ({tomorrow.strftime('%d.%m')})**
🌡 Температура: {min_temp}°C...{max_temp}°C
🌡 Ощущается как: {avg_feels}°C
🌧 Осадки: {rain_text}
🌅 Восход: {sunrise}
🌇 Закат: {sunset}

*Данные предоставлены OpenWeatherMap*
                            """.strip()
                        
        except Exception as e:
            logging.exception("Ошибка получения прогноза погоды:")

        # Падёжный fallback при недоступности сервиса
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%d.%m')
        return f"🌤 Прогноз погоды на {tomorrow}: сервис временно недоступен"

    # Функции для обработки сообщений разработчику и об ошибках
    async def handle_developer_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка сообщения разработчику"""
        try:
            user = update.message.from_user
            user_id = user.id
            first_name = getattr(user, "first_name", "")
            last_name = getattr(user, "last_name", "")
            username = getattr(user, "username", "")
            
            # Сохранить сообщение в базу данных
            message_data = {
                "telegram_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "username": username,
                "message_type": "developer_contact",
                "message_text": text,
                "created_at": get_moscow_time().isoformat(),
                "status": "new"
            }
            
            supabase.table("feedback_messages").insert(message_data).execute()
            
            # Отправить уведомление админу и суперадмину
            admin_query = supabase.table("users").select("*").in_("role", ["admin", "superuser"]).execute()
            
            for admin in admin_query.data if admin_query.data else []:
                if admin.get("chat_id"):
                    try:
                        await context.bot.send_message(
                            chat_id=admin["chat_id"],
                            text=f"""📞 **Сообщение разработчику**

👤 **От:** {first_name} {last_name} (@{username})
🆔 **ID:** {user_id}
📝 **Сообщение:**
{text}

🕐 **Время:** {get_moscow_time().strftime('%d.%m.%Y %H:%M')} МСК""",
                            parse_mode='Markdown'
                        )
                    except Exception as e:
                        logging.exception(f"Ошибка отправки уведомления админу {admin['telegram_id']}:")
            
            await update.message.reply_text(
                "✅ Ваше сообщение отправлено разработчику!\n\n"
                "Мы рассмотрим ваше обращение и свяжемся с вами в ближайшее время.",
                parse_mode='Markdown'
            )
            
            # Сбросить состояние
            context.user_data.pop('waiting_for_developer_message', None)
            
        except Exception as e:
            logging.exception("Ошибка обработки сообщения разработчику:")
            await update.message.reply_text("❌ Ошибка отправки сообщения. Попробуйте еще раз.")

    async def handle_bug_report(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка сообщения об ошибке"""
        try:
            user = update.message.from_user
            user_id = user.id
            first_name = getattr(user, "first_name", "")
            last_name = getattr(user, "last_name", "")
            username = getattr(user, "username", "")
            
            # Сохранить сообщение в базу данных
            bug_data = {
                "telegram_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "username": username,
                "message_type": "bug_report",
                "message_text": text,
                "created_at": get_moscow_time().isoformat(),
                "status": "new",
                "priority": "medium"
            }
            
            supabase.table("feedback_messages").insert(bug_data).execute()
            
            # Отправить уведомление админу и суперадмину
            admin_query = supabase.table("users").select("*").in_("role", ["admin", "superuser"]).execute()
            
            for admin in admin_query.data if admin_query.data else []:
                if admin.get("chat_id"):
                    try:
                        await context.bot.send_message(
                            chat_id=admin["chat_id"],
                            text=f"""🐛 **СООБЩЕНИЕ ОБ ОШИБКЕ**

👤 **От:** {first_name} {last_name} (@{username})
🆔 **ID:** {user_id}
🐛 **Описание ошибки:**
{text}

🕐 **Время:** {get_moscow_time().strftime('%d.%m.%Y %H:%M')} МСК
⚠️ **Приоритет:** Средний""",
                            parse_mode='Markdown'
                        )
                    except Exception as e:
                        logging.exception(f"Ошибка отправки уведомления об ошибке админу {admin['telegram_id']}:")
            
            await update.message.reply_text(
                "✅ Ваше сообщение об ошибке отправлено!\n\n"
                "🔧 Мы исправим эту проблему в ближайшее время.\n"
                "📧 При необходимости с вами свяжутся для уточнения деталей.\n\n"
                "Спасибо за помощь в улучшении системы! 🙏",
                parse_mode='Markdown'
            )
            
            # Сбросить состояние.
            context.user_data.pop('waiting_for_bug_report', None)
            
        except Exception as e:
            logging.exception("Ошибка обработки сообщения об ошибке:")
            await update.message.reply_text("❌ Ошибка отправки сообщения. Попробуйте еще раз.")

    app.add_handler(MessageHandler(filters.CONTACT, contact_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_qr))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(CommandHandler("start", start))
    from telegram.ext import CallbackQueryHandler
    app.add_handler(CallbackQueryHandler(callback_handler))

    app.run_polling()
