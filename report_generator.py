import logging
import openai
import config
from datetime import datetime
import aiomysql
from dotenv import load_dotenv
import os
import time  # Для замера времени

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
from logger_utils import setup_logging
logger = setup_logging()

# Настройки OpenAI API
openai.api_key = config.openai_api_key
openai.api_base = config.openai_api_base

# Опции для генерации текста OpenAI
OPENAI_COMPLETION_OPTIONS = config.openai_completion_options

# Асинхронное подключение к базе данных
async def create_async_connection():
    logger.info("[КРОТ]: Попытка асинхронного подключения к базе данных MySQL...")
    try:
        connection = await aiomysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT")),
            cursorclass=aiomysql.DictCursor,
            autocommit=True
        )
        logger.info("[КРОТ]: Подключено к серверу MySQL")
        return connection
    except aiomysql.Error as e:
        logger.error(f"[КРОТ]: Ошибка при подключении к базе данных: {e}")
        return None

# Выполнение запроса с повторными попытками
async def execute_async_query(connection, query, params=None, retries=3):
    for attempt in range(retries):
        try:
            start_time = time.time()
            async with connection.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchall()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Запрос выполнен: {query}, записей получено: {len(result)} (Время выполнения: {elapsed_time:.4f} сек)")
                return result
        except aiomysql.Error as e:
            logger.error(f"[КРОТ]: Ошибка при выполнении запроса '{query}': {e}")
            if e.args[0] in (2013, 2006):  # Ошибки потери соединения
                logger.info("[КРОТ]: Повторная попытка подключения...")
                await connection.ensure_closed()
                connection = await create_async_connection()
                if connection is None:
                    return None
            else:
                return None
    return None

class ChatGPT:
    def __init__(self, model="gpt-4o-mini"):
        self.model = model

    async def send_message(self, message, dialog_messages=[], chat_mode="assistant"):
        n_dialog_messages_before = len(dialog_messages)
        answer = None
        while answer is None:
            try:
                start_time = time.time()
                if self.model in {"gpt-4", "gpt-4o-mini"}:
                    messages = self._generate_prompt_messages(message, dialog_messages, chat_mode)
                    r = await openai.ChatCompletion.acreate(
                        model=self.model,
                        messages=messages,
                        **OPENAI_COMPLETION_OPTIONS
                    )
                    answer = r.choices[0].message["content"]
                else:
                    raise ValueError(f"Неизвестная модель: {self.model}")
                answer = self._postprocess_answer(answer)
                elapsed_time = time.time() - start_time
                n_input_tokens, n_output_tokens = r.usage.prompt_tokens, r.usage.completion_tokens
                logger.info(f"[КРОТ]: Сообщение успешно отправлено. Входных токенов: {n_input_tokens}, выходных токенов: {n_output_tokens} (Время выполнения: {elapsed_time:.4f} сек)")
            except openai.error.InvalidRequestError as e:
                if len(dialog_messages) == 0:
                    raise ValueError("Превышено количество токенов, даже после удаления всех сообщений диалога.") from e
                dialog_messages = dialog_messages[1:]
            except Exception as e:
                logger.error(f"[КРОТ]: Ошибка при отправке сообщения: {e}")
                raise
        return answer, (n_input_tokens, n_output_tokens), n_dialog_messages_before - len(dialog_messages)

    def _generate_prompt_messages(self, message, dialog_messages, chat_mode):
        messages = [{"role": "system", "content": config.chat_modes[chat_mode]["prompt_start"]}]
        for dialog_message in dialog_messages:
            messages.append({"role": "user", "content": dialog_message["user"]})
            messages.append({"role": "assistant", "content": dialog_message["bot"]})
        messages.append({"role": "user", "content": message})
        return messages

    def _postprocess_answer(self, answer):
        return answer.strip()

class ReportGenerator:
    def __init__(self, model="gpt-4o-mini"):
        self.gpt = ChatGPT(model=model)

    async def generate_recommendations(self, operator_data):
        connection = await create_async_connection()
        if connection:
            try:
                checklist_number, checklist_info = await self.get_checklist_data(operator_data.get('category_number'), connection)
                operator_data['checklist_info'] = checklist_info

                prompt = f"""
                На основе следующих данных оператора: {operator_data}, предоставь рекомендации по улучшению его работы.
                Данные включают количество звонков, длительность звонков, уровень конверсии и другие важные метрики.
                """
                start_time = time.time()
                response, _, _ = await self.gpt.send_message(prompt)
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Рекомендации успешно сгенерированы (Время выполнения: {elapsed_time:.4f} сек).")
                return response
            except Exception as e:
                logger.error(f"[КРОТ]: Ошибка при генерации рекомендаций: {e}")
                return "Не удалось сгенерировать рекомендации. Пожалуйста, попробуйте позже."
            finally:
                await connection.ensure_closed()
        else:
            logger.error("[КРОТ]: Ошибка подключения к базе данных для генерации рекомендаций.")
            return "Ошибка подключения к базе данных."

    async def create_report(self, operator_id, operator_data, recommendations):
        report = f"Отчет по оператору ID: {operator_id}\n"
        report += f"Данные оператора:\n{operator_data}\n"
        report += f"Рекомендации по улучшению:\n{recommendations}\n"
        connection = await create_async_connection()
        if connection:
            try:
                await self.save_call_score(
                    connection, 
                    call_id=operator_id, 
                    score=recommendations,
                    call_category=operator_data.get('call_category'),
                    call_date=operator_data.get('call_date'),
                    called_info=operator_data.get('called_info'),
                    caller_info=operator_data.get('caller_info'),
                    talk_duration=operator_data.get('talk_duration'),
                    transcript=operator_data.get('transcript'),
                    result=recommendations,
                    category_number=operator_data.get('category_number'),
                    checklist_number=operator_data.get('checklist_info'),
                    checklist_category=operator_data.get('checklist_category')
                )
                logger.info("[КРОТ]: Отчет успешно создан.")
            except Exception as e:
                logger.error(f"[КРОТ]: Ошибка при сохранении данных звонка: {e}")
            finally:
                await connection.ensure_closed()
        else:
            logger.error("[КРОТ]: Ошибка подключения к базе данных для сохранения отчета.")
            return "Ошибка подключения к базе данных."
        return report

    async def get_checklist_data(self, category_number, connection):
        if category_number is None:
            logger.warning("[КРОТ]: Категорийный номер не определен.")
            return None, "Не определено"
        logger.info(f"[КРОТ]: Получение чек-листа для категории: {category_number}")
        try:
            query = """
            SELECT Number_check_list, Check_list_categories, Info_check_list 
            FROM check_list 
            WHERE Check_list_categories = (
                SELECT Call_categories 
                FROM categories 
                WHERE Number = %s
            )
            """
            result = await execute_async_query(connection, query, (category_number,))
            
            if result and len(result) > 0:
                return result[0]['Number_check_list'], result[0]['Info_check_list']
            else:
                logger.warning(f"[КРОТ]: Чек-лист для категории {category_number} не найден.")
                return None, "Чек-лист не найден"
                
        except aiomysql.Error as e:
            logger.error(f"[КРОТ]: Ошибка при получении данных чек-листа: {e}")
        return None, "Не определено"

    async def save_call_score(self, connection, call_id, score, call_category, call_date, called_info, caller_info, talk_duration, transcript, result, category_number, checklist_number, checklist_category):
        if not all([call_id, score, call_date, called_info, caller_info, talk_duration, transcript, result, category_number, checklist_number, checklist_category]):
            logger.warning("[КРОТ]: Одно или несколько полей данных звонка отсутствуют.")
            return
        logger.info(f"[КРОТ]: Попытка сохранения данных звонка: {call_id}")
        try:
            score_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insert_score_query = """
            INSERT INTO call_scores (history_id, call_score, score_date, call_date, call_category, called_info, caller_info, talk_duration, transcript, result, number_category, number_checklist, category_checklist)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            await execute_async_query(connection, insert_score_query, (
                call_id, 
                str(score), 
                score_date, 
                call_date, 
                str(call_category), 
                str(called_info), 
                str(caller_info), 
                str(talk_duration), 
                str(transcript), 
                str(result), 
                str(category_number), 
                str(checklist_number), 
                str(checklist_category)
            ))
            logger.info("[КРОТ]: Данные звонка успешно сохранены в call_scores")
        except aiomysql.Error as e:
            logger.error(f"[КРОТ]: Ошибка при сохранении данных звонка: {e}")
