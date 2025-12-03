## auto_close_scheduler.py
import os
import asyncio
import schedule
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import utils.httpx_proxy_patch
from supabase import create_client, Client
from telegram import Bot
from pathlib import Path

# Московское время (UTC+3)
MOSCOW_TZ = timezone(timedelta(hours=3))

def get_moscow_time():
    """Получить текущее время в Москве"""
    return datetime.now(MOSCOW_TZ)

if Path('.env').is_file():
    load_dotenv()      # локальная разработка

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
ADMIN_USERNAME = "gayazking"

if not all([TELEGRAM_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    raise Exception("Не хватает переменных окружения!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
bot = Bot(token=TELEGRAM_TOKEN)

async def get_users_without_departure():
    """Получить пользователей, которые пришли, но не ушли сегодня"""
    try:
        moscow_now = get_moscow_time()
        today = moscow_now.date()
        today_start = datetime.combine(today, datetime.min.time(), MOSCOW_TZ)
        today_end = datetime.combine(today, datetime.max.time(), MOSCOW_TZ)
        
        # Получить всех пользователей с событием прихода сегодня
        arrivals_result = supabase.table("time_events").select("*").eq("event_type", "arrival").gte("event_time", today_start.isoformat()).lte("event_time", today_end.isoformat()).execute()
        
        users_without_departure = []
        
        for arrival in arrivals_result.data:
            user_id = arrival["telegram_id"]
            arrival_time = arrival["event_time"]
            
            # Проверить, есть ли событие ухода после этого прихода
            departure_result = supabase.table("time_events").select("*").eq("telegram_id", user_id).eq("event_type", "departure").gt("event_time", arrival_time).limit(1).execute()
            
            if not departure_result.data:
                users_without_departure.append(arrival)
        
        return users_without_departure
    except Exception as e:
        print(f"Ошибка получения пользователей без ухода: {e}")
        return []

async def auto_close_workday():
    """Автоматически закрыть рабочий день для пользователей без ухода"""
    print(f"Запуск автозакрытия рабочего дня в {datetime.now()}")
    
    users_without_departure = await get_users_without_departure()
    
    if not users_without_departure:
        print("Нет пользователей для автозакрытия")
        return
    
    # Получить chat_id админа для уведомлений
    admin_query = supabase.table("users").select("*").eq("username", ADMIN_USERNAME).execute()
    admin = admin_query.data[0] if admin_query.data and len(admin_query.data) > 0 else None
    admin_chat_id = admin["chat_id"] if admin and admin.get("chat_id") else None
    
    for arrival_event in users_without_departure:
        try:
            user_id = arrival_event["telegram_id"]
            user_name = f"{arrival_event['first_name']} {arrival_event['last_name']}"
            branch_name = arrival_event["branch_name"]
            arrival_time = datetime.fromisoformat(arrival_event["event_time"])
            
            # Время автозакрытия - 21:00 сегодня по московскому времени
            moscow_now = get_moscow_time()
            close_time = datetime.combine(moscow_now.date(), datetime.strptime("21:00", "%H:%M").time(), MOSCOW_TZ)
            
            # Рассчитать 8 часов работы
            work_hours = 8.0
            
            # Создать событие автоматического ухода
            auto_departure = {
                "telegram_id": user_id,
                "first_name": arrival_event["first_name"],
                "last_name": arrival_event["last_name"],
                "username": arrival_event["username"],
                "chat_id": arrival_event["chat_id"],
                "branch_id": arrival_event["branch_id"],
                "branch_name": branch_name,
                "event_time": close_time.isoformat(),
                "event_type": "departure",
                "is_auto_closed": True,
                "work_hours": work_hours,
                "qr_timestamp": None,
                "signature": "auto_close",
                "raw_json": '{"auto_closed": true}'
            }
            
            # Сохранить в базу
            supabase.table("time_events").insert(auto_departure).execute()
            
            # Отправить уведомление пользователю
            user_chat_id = arrival_event["chat_id"]
            if user_chat_id:
                message = (
                    "🔴 АВТОМАТИЧЕСКОЕ ЗАКРЫТИЕ РАБОЧЕГО ДНЯ\n\n"
                    f"Ваш рабочий день был автоматически закрыт в 21:00\n"
                    f"Филиал: {branch_name}\n"
                    f"Учтено рабочих часов: 8\n\n"
                    "⚠️ Информация о нарушении передана руководителю для проверки."
                )
                await bot.send_message(chat_id=user_chat_id, text=message)
            
            # Отправить уведомление админу
            if admin_chat_id:
                admin_message = (
                    "⚠️ АВТОМАТИЧЕСКОЕ ЗАКРЫТИЕ РАБОЧЕГО ДНЯ\n\n"
                    f"Сотрудник: {user_name}\n"
                    f"Username: @{arrival_event['username']}\n"
                    f"Филиал: {branch_name}\n"
                    f"Время прихода: {arrival_time:%d.%m.%Y %H:%M}\n"
                    f"Автозакрытие: 21:00\n"
                    f"Учтено часов: 8\n\n"
                    "Требуется проверка нарушения."
                )
                await bot.send_message(chat_id=admin_chat_id, text=admin_message)
            
            print(f"Автозакрытие для пользователя {user_name} выполнено")
            
        except Exception as e:
            print(f"Ошибка автозакрытия для пользователя {arrival_event.get('telegram_id')}: {e}")

def schedule_auto_close():
    """Запланировать автозакрытие на 21:00 каждый день"""
    schedule.every().day.at("21:00").do(lambda: asyncio.run(auto_close_workday()))
    
    print("Планировщик автозакрытия запущен. Автозакрытие в 21:00 каждый день.")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверять каждую минуту

if __name__ == "__main__":
    schedule_auto_close()
