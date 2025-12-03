# tamagotchi_scheduler.py
import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import utils.httpx_proxy_patch
from supabase import create_client, Client
from telegram import Bot
import random
from pathlib import Path

# Московское время (UTC+3)
MOSCOW_TZ = timezone(timedelta(hours=3))

def get_moscow_time():
    """Получить текущее время в Москве"""
    return datetime.now(MOSCOW_TZ)

def is_working_hours():
    """Проверить, рабочее ли время (9:00-18:00 по Москве)"""
    moscow_time = get_moscow_time()
    hour = moscow_time.hour
    # Рабочие часы: с 9:00 до 18:00
    return 9 <= hour <= 18

def should_send_notification(telegram_id, notification_type, last_notification_time=None):
    """Определить, нужно ли отправлять уведомление"""
    now = get_moscow_time().replace(tzinfo=None)
    
    # Критические уведомления отправляем только в рабочие часы
    if notification_type == "critical":
        if not is_working_hours():
            return False
        # Критические уведомления не чаще раза в 4 часа
        if last_notification_time:
            hours_since_last = (now - last_notification_time).total_seconds() / 3600
            return hours_since_last >= 4
        return True
    
    # Уведомления о смерти отправляем всегда, но не чаще раза в день
    elif notification_type == "death":
        if last_notification_time:
            hours_since_last = (now - last_notification_time).total_seconds() / 3600
            return hours_since_last >= 24
        return True
    
    # Обычные уведомления о голоде только в рабочие часы и не чаще раза в 6 часов
    elif notification_type == "hungry":
        if not is_working_hours():
            return False
        if last_notification_time:
            hours_since_last = (now - last_notification_time).total_seconds() / 3600
            return hours_since_last >= 6
        return True
    
    return False

async def get_last_notification(telegram_id, notification_type):
    """Получить время последнего уведомления"""
    try:
        result = supabase.table("tamagotchi_notifications").select("sent_at").eq("telegram_id", telegram_id).eq("notification_type", notification_type).order("sent_at", desc=True).limit(1).execute()
        if result.data:
            return datetime.fromisoformat(result.data[0]["sent_at"])
        return None
    except Exception as e:
        logging.warning(f"Ошибка получения последнего уведомления: {e}")
        return None

async def save_notification(telegram_id, notification_type):
    """Сохранить информацию об отправленном уведомлении"""
    try:
        now = get_moscow_time().replace(tzinfo=None)
        supabase.table("tamagotchi_notifications").insert({
            "telegram_id": telegram_id,
            "notification_type": notification_type,
            "sent_at": now.isoformat()
        }).execute()
    except Exception as e:
        logging.warning(f"Ошибка сохранения уведомления: {e}")

if Path('.env').is_file():
    load_dotenv()      # локальная разработка

# Настройки логирования
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO
)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")

if not all([TELEGRAM_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    raise Exception("Не хватает переменных окружения!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
bot = Bot(token=TELEGRAM_TOKEN)

async def get_tamagotchi_message(message_type):
    """Получить случайное сообщение тамагочи"""
    try:
        result = supabase.table("tamagotchi_messages").select("*").eq("message_type", message_type).execute()
        if result.data:
            message_data = random.choice(result.data)
            return f"{message_data['emoji']} {message_data['message']}"
        return "🐾 Тамагочи что-то говорит..."
    except Exception as e:
        logging.exception("Ошибка получения сообщения тамагочи:")
        return "🐾 Тамагочи молчит..."

async def check_hungry_tamagotchis():
    """Проверить голодных тамагочи и отправить уведомления"""
    try:
        logging.info("Проверка голодных тамагочи...")
        
        # Получить всех живых тамагочи
        result = supabase.table("tamagotchi").select("*").eq("is_alive", True).execute()
        
        if not result.data:
            logging.info("Тамагочи не найдены")
            return
        
        now = get_moscow_time().replace(tzinfo=None)
        
        for tamagotchi in result.data:
            try:
                # Рассчитать время с последнего кормления
                last_fed = datetime.fromisoformat(tamagotchi["last_fed"])
                if last_fed.tzinfo is not None:
                    last_fed = last_fed.replace(tzinfo=None)
                
                hours_since_fed = (now - last_fed).total_seconds() / 3600
                
                # Обновить показатели
                hunger = max(0, tamagotchi["hunger"] - int(hours_since_fed * 5))
                happiness = max(0, tamagotchi["happiness"] - int(hours_since_fed * 3))
                health = max(0, tamagotchi["health"] - int(hours_since_fed * 2))
                
                # Проверить смерть (3 дня = 72 часа)
                is_alive = hours_since_fed < 72
                
                # Обновить в базе
                updated_data = {
                    "hunger": hunger,
                    "happiness": happiness,
                    "health": health,
                    "is_alive": is_alive,
                    "updated_at": now.isoformat()
                }
                
                supabase.table("tamagotchi").update(updated_data).eq("telegram_id", tamagotchi["telegram_id"]).execute()
                
                # Получить chat_id пользователя
                user_result = supabase.table("users").select("chat_id").eq("telegram_id", tamagotchi["telegram_id"]).execute()
                if not user_result.data or not user_result.data[0].get("chat_id"):
                    continue
                
                chat_id = user_result.data[0]["chat_id"]
                
                # Отправить уведомления в зависимости от состояния
                if not is_alive:
                    # Тамагочи умер - проверяем, нужно ли отправлять уведомление
                    last_death_notification = await get_last_notification(tamagotchi["telegram_id"], "death")
                    if should_send_notification(tamagotchi["telegram_id"], "death", last_death_notification):
                        message = await get_tamagotchi_message("dead")
                        await bot.send_message(
                            chat_id=chat_id,
                            text=f"💀 **Ваш тамагочи умер!**\n\n{message}\n\nВы не сканировали QR-коды более 3 дней. Воскресите его следующим сканированием!",
                            parse_mode='Markdown'
                        )
                        await save_notification(tamagotchi["telegram_id"], "death")
                        logging.info(f"Отправлено уведомление о смерти тамагочи пользователю {tamagotchi['telegram_id']}")
                    
                elif hunger < 20 or happiness < 20 or health < 20:
                    # Тамагочи очень голоден/болен - критическое состояние
                    last_critical_notification = await get_last_notification(tamagotchi["telegram_id"], "critical")
                    if should_send_notification(tamagotchi["telegram_id"], "critical", last_critical_notification):
                        message = await get_tamagotchi_message("sick")
                        status_text = f"""
😰 **{tamagotchi['name']}** (Уровень {tamagotchi['level']}) - КРИТИЧЕСКОЕ СОСТОЯНИЕ!
🍎 Сытость: {hunger}/100
😊 Счастье: {happiness}/100
❤️ Здоровье: {health}/100

{message}

⚠️ Срочно отсканируйте QR-код на работе, иначе тамагочи умрет!
                        """.strip()
                        
                        await bot.send_message(
                            chat_id=chat_id,
                            text=status_text,
                            parse_mode='Markdown'
                        )
                        await save_notification(tamagotchi["telegram_id"], "critical")
                        logging.info(f"Отправлено критическое уведомление пользователю {tamagotchi['telegram_id']}")
                    
                elif hunger < 50 or happiness < 50 or health < 50:
                    # Тамагочи голоден - обычное уведомление
                    last_hungry_notification = await get_last_notification(tamagotchi["telegram_id"], "hungry")
                    if should_send_notification(tamagotchi["telegram_id"], "hungry", last_hungry_notification):
                        message = await get_tamagotchi_message("hungry")
                        status_text = f"""
😔 **{tamagotchi['name']}** (Уровень {tamagotchi['level']}) - голоден
🍎 Сытость: {hunger}/100
😊 Счастье: {happiness}/100
❤️ Здоровье: {health}/100

{message}

Не забывайте сканировать QR-коды на работе!
                        """.strip()
                        
                        await bot.send_message(
                            chat_id=chat_id,
                            text=status_text,
                            parse_mode='Markdown'
                        )
                        await save_notification(tamagotchi["telegram_id"], "hungry")
                        logging.info(f"Отправлено уведомление о голоде пользователю {tamagotchi['telegram_id']}")
                
            except Exception as e:
                logging.exception(f"Ошибка обработки тамагочи пользователя {tamagotchi['telegram_id']}:")
                continue
        
        logging.info("Проверка тамагочи завершена")
        
    except Exception as e:
        logging.exception("Ошибка проверки тамагочи:")

async def main():
    """Основной цикл проверки тамагочи"""
    logging.info("Запуск планировщика тамагочи...")
    
    while True:
        try:
            await check_hungry_tamagotchis()
            # Проверяем каждый час
            await asyncio.sleep(3600)  # 1 час = 3600 секунд
        except Exception as e:
            logging.exception("Ошибка в основном цикле:")
            # При ошибке ждем 5 минут и пробуем снова
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
