#opeanai_telebot.py
import asyncio
from datetime import datetime, timedelta, date
import logging
import time  # Для замера времени
import os
from operator_data import OperatorData
from logger_utils import setup_logging
import openai
import httpx
from openai import AsyncOpenAI, OpenAIError #импорт класса
import config
import aiomysql
from dotenv import load_dotenv
from permissions_manager import PermissionsManager


# Настройка логирования
logger = setup_logging()



class OpenAIReportGenerator:
    def __init__(self, db_manager, model="gpt-4o-mini"):
        # Настройка OpenAI API ключа из переменных окружения
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.error("OpenAI API ключ не найден. Пожалуйста, установите переменную окружения OPENAI_API_KEY.")
            raise EnvironmentError("OpenAI API ключ не найден.")
        
        self.client = AsyncOpenAI(api_key=api_key)
        self.db_manager = db_manager
        self.operator_data = OperatorData(db_manager)
        self.model = model  # Устанавливаем модель gpt-4o-mini 
        self.permissions_manager = PermissionsManager(db_manager)
     
    def get_date_range(self, period):
        """Возвращает начальную и конечную дату для указанного периода."""
        today = date.today()
        if period == 'daily':
            return today, today
        elif period == 'weekly':
            start_date = today - timedelta(days=today.weekday())
            return start_date, today
        elif period == "biweekly":
            start_biweek = today - timedelta(days=14)
            return start_biweek, today
        elif period == 'monthly':
            start_date = today.replace(day=1)
            return start_date, today
        elif period == "half_year":
            start_half_year = today - timedelta(days=183)
            return start_half_year, today
        elif period == "yearly":
            start_year = today - timedelta(days=365)
            return start_year, today
        else:
            raise ValueError("Неподдерживаемый период.")
        
        
    async def get_user_extension(self, connection, user_id):
        """
        Получение extension оператора по его user_id.
        """
        query = "SELECT extension FROM users WHERE user_id = %s"
        try:
            async with connection.cursor() as cursor:
                # Выполняем запрос
                await cursor.execute(query, (user_id,))
                result = await cursor.fetchone()

            # Проверяем, если результат существует и содержит extension
            if result and 'extension' in result:
                extension = result['extension']
                logger.info(f"[КРОТ]: Получен extension {extension} для user_id {user_id}")
                return extension
            else:
                logger.warning(f"[КРОТ]: Не найден extension для user_id {user_id}")
                return None
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении extension для user_id {user_id}: {e}")
            return None

        
    async def get_operator_data(self, connection, extension, start_date, end_date):
        """
        Извлечение данных оператора за указанный период.
        Параметры:
            - connection: соединение с базой данных.
            - extension: идентификатор оператора (extension).
            - start_date: начальная дата периода.
            - end_date: конечная дата периода.
        """
        query = """
        SELECT call_score, transcript, result, called_info, call_date, talk_duration 
        FROM call_scores 
        WHERE (SUBSTRING_INDEX(caller_info, ' ', 1) = %s OR SUBSTRING_INDEX(called_info, ' ', 1) = %s)
            AND DATE(call_date) BETWEEN %s AND %s;
        """
        try:
            # Преобразуем даты в нужный формат
            start_date = datetime.strptime(str(start_date), '%Y-%m-%d')
            end_date = datetime.strptime(str(end_date), '%Y-%m-%d')

            # Используем асинхронный курсор для выполнения запроса
            async with connection.cursor() as cursor:
                # Логируем запрос перед его выполнением
                logger.info(f"[КРОТ]: Выполнение запроса для extension {extension} за период {start_date} - {end_date}")
                await cursor.execute(query, (extension, extension, start_date, end_date))
                result = await cursor.fetchall()

                # Проверяем наличие результата и логируем это
                if result:
                    logger.info(f"[КРОТ]: Данные получены: {len(result)} записей для extension {extension}")
                    # Обработка данных: установка значений по умолчанию, если данные отсутствуют
                    operator_data = result
                    for data in operator_data:
                        data.setdefault('call_score', 'Нет данных')
                        data.setdefault('transcript', 'Нет данных')
                        data.setdefault('result', 'Нет данных')
                        data.setdefault('called_info', 'Нет данных')
                        data.setdefault('call_date', 'Нет данных')
                        data.setdefault('talk_duration', 'Нет данных')
                    return operator_data
                else:
                    logger.warning(f"[КРОТ]: Данные по оператору с extension {extension} не найдены.")
                    return []
        except ValueError as e:
            logger.error(f"[КРОТ]: Некорректный формат дат: {e}")
            return None
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при извлечении данных оператора с extension {extension}: {e}")
            return None
        
    async def get_operator_name(self, connection, extension):
        """
        Получает позывной оператора по его extension.
        """
        query = "SELECT name FROM users WHERE extension = %s"
        try:
            async with connection.cursor() as cursor:
                await cursor.execute(query, (extension,))
                result = await cursor.fetchone()
                if result and 'name' in result:
                    return result['name']
                else:
                    logger.warning(f"[КРОТ]: Позывной для оператора с extension {extension} не найден.")
                    return f"Оператор {extension}"
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении позывного для оператора с extension {extension}: {e}")
            return f"Оператор {extension}"


    def calculate_operator_metrics(self, operator_data, extension):
        """Расчет всех метрик оператора на основе данных звонков."""
        # Расчеты метрик с логированием
        total_calls = len(operator_data)
        logger.info(f"[КРОТ]: Всего звонков: {total_calls}")
        accepted_calls = sum(1 for call in operator_data if call.get('result') == 'успешный')
        logger.info(f"[КРОТ]: Принято звонков: {accepted_calls}")
        booked_services = sum(1 for call in operator_data if call.get('result') == 'забронировано')
        logger.info(f"[КРОТ]: Забронировано услуг: {booked_services}")
        conversion_rate = (accepted_calls / total_calls) * 100 if total_calls > 0 else 0
        logger.info(f"[КРОТ]: Конверсия в запись: {conversion_rate}%")
        avg_call_rating = self.calculate_avg_score(operator_data)
        logger.info(f"[КРОТ]: Средняя оценка звонков: {avg_call_rating}")
        total_cancellations = sum(1 for call in operator_data if call.get('result') == 'отменен')
        logger.info(f"[КРОТ]: Всего отмен: {total_cancellations}")
        cancellation_rate = (total_cancellations / total_calls) * 100 if total_calls > 0 else 0
        logger.info(f"[КРОТ]: Доля отмен: {cancellation_rate}%")
        total_conversation_time = self.calculate_total_duration(operator_data)
        logger.info(f"[КРОТ]: Общее время разговоров: {total_conversation_time} секунд")
        avg_conversation_time = total_conversation_time / total_calls if total_calls > 0 else 0
        logger.info(f"[КРОТ]: Среднее время разговора: {avg_conversation_time} секунд")
        total_spam_time = sum(float(call.get('talk_duration', 0)) for call in operator_data if call.get('category') == 'spam')
        logger.info(f"[КРОТ]: Общее время спама: {total_spam_time} секунд")
        avg_spam_time = total_spam_time / total_calls if total_calls > 0 else 0
        logger.info(f"[КРОТ]: Среднее время спама: {avg_spam_time} секунд")
        total_navigation_time = sum(float(call.get('talk_duration', 0)) for call in operator_data if call.get('category') == 'navigation')
        logger.info(f"[КРОТ]: Общее время навигации: {total_navigation_time} секунд")
        avg_navigation_time = total_navigation_time / total_calls if total_calls > 0 else 0
        logger.info(f"[КРОТ]: Среднее время навигации: {avg_navigation_time} секунд")
        total_talk_time = total_conversation_time
        logger.info(f"[КРОТ]: Общее время всех разговоров: {total_talk_time} секунд")
        complaint_calls = sum(1 for call in operator_data if call.get('result') == 'жалоба')
        logger.info(f"[КРОТ]: Всего звонков с жалобами: {complaint_calls}")
        complaint_rating = self.calculate_avg_score([call for call in operator_data if call.get('result') == 'жалоба'])
        logger.info(f"[КРОТ]: Средняя оценка звонков с жалобами: {complaint_rating}")
        logger.info(f"[КРОТ]: Метрики рассчитаны для оператора с extension {extension}")

        # Агрегируем метрики в словарь
        operator_metrics = {
            'extension': extension,
            'total_calls': total_calls,
            'accepted_calls': accepted_calls,
            'booked_services': booked_services,
            'conversion_rate': conversion_rate,
            'avg_call_rating': avg_call_rating,
            'total_cancellations': total_cancellations,
            'cancellation_rate': cancellation_rate,
            'total_conversation_time': total_conversation_time,
            'avg_conversation_time': avg_conversation_time,
            'total_spam_time': total_spam_time,
            'avg_spam_time': avg_spam_time,
            'total_navigation_time': total_navigation_time,
            'avg_navigation_time': avg_navigation_time,
            'total_talk_time': total_talk_time,
            'complaint_calls': complaint_calls,
            'complaint_rating': complaint_rating
        }

        # Добавляем значения по умолчанию для дополнительных метрик
        operator_metrics.setdefault('empathy_score', 0)
        operator_metrics.setdefault('understanding_score', 0)
        operator_metrics.setdefault('response_quality_score', 0)
        operator_metrics.setdefault('problem_solving_score', 0)
        operator_metrics.setdefault('call_closing_score', 0)
        operator_metrics.setdefault('total_call_score', 0)

        return operator_metrics
    
    async def generate_report(self, connection, user_id, period='daily'):
        """
        Генерация отчета для оператора по его user_id с использованием данных и OpenAI.
        Параметры:
            - extension: extension оператора, для которого нужно сгенерировать отчет.
            - period: Период отчета (daily, weekly, monthly, biweekly, half-year, yearly).
        """
        logger.info(f"[КРОТ]: Начата генерация отчета для оператора с extension {user_id} за период {period}.")
        try:
            start_time = time.time()
            # Преобразование периода в диапазон дат
            start_date, end_date = self.get_date_range(period)
            logger.info(f"[КРОТ]: Определен диапазон дат: {start_date} - {end_date}")
            # Получаем extension пользователя по его user_id
            extension = await self.get_user_extension(connection, user_id)
            if not extension:
                logger.error(f"[КРОТ]: Не удалось получить extension для пользователя с user_id {user_id}.")
                return "Ошибка: Не удалось получить данные пользователя."
            # Получаем имя оператора
            operator_name = await self.get_operator_name(connection, extension)
            logger.info(f"[КРОТ]: Имя оператора: {operator_name}")
            # Получение данных оператора из базы данных за указанный период
            operator_data = await self.get_operator_data(connection, extension, start_date, end_date)
            if operator_data is None:
                return "Ошибка при извлечении данных оператора или данных нет."
            if not operator_data:
                logger.warning(f"[КРОТ]: Нет данных по оператору с extension {extension} за период {start_date} - {end_date}")
                return f"Данные по оператору {operator_name} (extension {extension}) за период {start_date} - {end_date} не найдены."
            logger.info(f"[КРОТ]: Получено {len(operator_data)} записей для оператора с extension {extension}")
            # Используйте отдельно report_date
            report_date = f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
            
            # Добавляем report_date в каждый элемент operator_data для использования в отчете
            for call in operator_data:
                call['report_date'] = report_date
            
            # Убедитесь, что operator_data — это список, и обработайте его корректно
            if not isinstance(operator_data, list):
                logger.error("[КРОТ]: Ожидался список записей, но получен другой тип данных.")
                return "Ошибка в структуре данных оператора"
            
            # **Вызов нового метода для расчета метрик**
            operator_metrics = self.calculate_operator_metrics(operator_data, extension)
            logger.debug(f"operator_metrics: {operator_metrics}")

            # Генерация рекомендаций
            logger.info(f"[КРОТ]: Генерация рекомендаций для пользователя {extension}")
            recommendations = await self.generate_combined_recommendations(operator_metrics, operator_data, user_id=user_id, name=operator_name)
            # Проверка на None для рекомендаций
            if not recommendations:
                logger.error("[КРОТ]: Рекомендации не были сгенерированы.")
                return "Ошибка: Не удалось получить рекомендации."
            # Проверка на наличие ошибок в рекомендациях
            if "Ошибка" in recommendations:
                logger.error(f"[КРОТ]: {recommendations}")
                return recommendations
            logger.info(f"[КРОТ]: Рекомендации сгенерированы: {recommendations}")
            if recommendations is None:
                recommendations = "Не удалось получить рекомендации в данный момент."
            # Формирование отчета
            logger.info(f"[КРОТ]: Формирование отчета для пользователя {extension}")
            report_date = f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
            report = self.create_report(operator_metrics, recommendations, report_date, operator_name)
            logger.info(f"[КРОТ]: Отчет сформирован: {report}")
            
            async with self.db_manager.acquire() as connection:
                # Сохранение отчета в таблицу reports
                await self.save_report_to_db(
                    connection=connection,  # Подключение к базе данных
                    user_id=extension,  # Идентификатор пользователя (может быть extension)
                    name=operator_name,  # Имя оператора
                    report_text=report,  # Сформированный текст отчета
                    period=period,  # Период (daily, weekly, monthly и т.д.)
                    start_date=start_date,  # Начальная дата периода
                    end_date=end_date,  # Конечная дата периода
                    operator_metrics=operator_metrics,
                    recommendations=recommendations  # Рекомендации для улучшения
                )

            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: МЕТОД В КЛАССЕ ПЕРВЫЙ ЮЗАЕМ, ПЕРВАЯ ЛОВУШКА СРАБОТАЛА. Отчет успешно сгенерирован для оператора {user_id} за период {start_date} - {end_date}. (Время выполнения: {elapsed_time:.4f} сек)")
            return report
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при генерации отчета для оператора {user_id}: {e}")
            return f"Ошибка при генерации отчета: {e}"

    def calculate_avg_score(self, operator_data):
        """Расчет среднего рейтинга звонков."""
        scores = [float(call.get('call_score')) for call in operator_data if call.get('call_score') is not None]
        return sum(scores) / len(scores) if scores else 0

    def calculate_total_duration(self, operator_data):
        """Подсчет общей длительности звонков."""
        durations = [float(call.get('talk_duration', 0)) for call in operator_data if call.get('talk_duration') is not None]
        return sum(durations)


    def validate_metrics(self, operator_metrics):
        """
        Проверка на наличие обязательных полей в данных оператора.
        """
        required_fields = ['extension', 'empathy_score', 'understanding_score', 'response_quality_score',
                           'problem_solving_score', 'call_closing_score', 'total_call_score']
        for field in required_fields:
            if field not in operator_metrics:
                logger.error(f"[КРОТ]: Отсутствует обязательное поле {field} в метриках оператора.")
                return False
        return True
    
    async def generate_combined_recommendations(self, operator_metrics, operator_data, user_id, name, max_length=2000, max_retries=3):
        """
        Генерация рекомендаций для оператора на основе его метрик и данных из поля `result`,
        с поддержкой повторных попыток при ошибках.

        Параметры:
            - operator_metrics: Метрики оператора (словарь).
            - operator_data: Данные звонков оператора, включающие поле `result`.
            - user_id: Идентификатор пользователя.
            - name: Имя оператора.
            - max_length: Максимальная длина текста в символах для одного запроса к ChatGPT.
            - max_retries: Максимальное количество повторных попыток в случае ошибки.
        """
        # Логирование начального состояния
        logger.info("[РЕКОМЕНДАЦИИ]: Начало генерации рекомендаций для оператора.")
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Метрики оператора: {operator_metrics}")
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Данные звонков оператора (количество записей): {len(operator_data)}")

        # Расчетные метрики
        start_date = operator_data[0].get('call_date') if operator_data else "неизвестно"
        end_date = operator_data[-1].get('call_date') if operator_data else "неизвестно"
        total_calls = len(operator_data)
        avg_score = self.calculate_avg_score(operator_data)
        total_duration = self.calculate_total_duration(operator_data)
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Период звонков: {start_date} - {end_date}, Общее количество звонков: {total_calls}, "
                    f"Средняя оценка звонков: {avg_score}, Общая длительность звонков: {total_duration} секунд")
        
        # Создаем начальный запрос с метриками и расчетными данными
        coaching_prompt = f"""
        Оператор {name} с extension {operator_metrics.get('extension', 'Не указан')} выполнил следующие действия во время звонков:

        1. Приветливость и эмпатия сотрудника: {operator_metrics.get('empathy_score', 'Не указано')}
        2. Понимание запроса клиента: {operator_metrics.get('understanding_score', 'Не указано')}
        3. Качество ответов и информирование: {operator_metrics.get('response_quality_score', 'Не указано')}
        4. Эффективность общения и решение проблемы: {operator_metrics.get('problem_solving_score', 'Не указано')}
        5. Завершение звонка: {operator_metrics.get('call_closing_score', 'Не указано')}
        Общая оценка звонка: {operator_metrics.get('total_call_score', 'Не указано')}
        
        Дополнительные данные:
        - Период звонков: {start_date} - {end_date}
        - Общее количество звонков: {total_calls}
        - Средняя оценка звонков: {avg_score}
        - Общая длительность звонков: {total_duration} секунд

        ### Рекомендации:
        На основе вышеуказанных данных и сведений о звонках предоставь рекомендации для улучшения для пользователя {user_id} ({name}).
        """
        # Логируем начальный запрос
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Начальный запрос к ChatGPT: {coaching_prompt}")

        # Инициализация рекомендаций с начальным текстом
        recommendations = coaching_prompt
        result_texts = [call['result'] for call in operator_data if call.get('result')]
        current_packet = coaching_prompt
        logger.info(f"[РЕКОМЕНДАЦИИ]: Количество текстов result для обработки: {len(result_texts)}")

        # Разделение данных result на пакеты и запрос к ChatGPT
        for i, text in enumerate(result_texts):
            logger.debug(f"[РЕКОМЕНДАЦИИ]: Обработка текста result [{i+1}/{len(result_texts)}]: {text[:50]}...")

            if len(current_packet) + len(text) <= max_length:
                current_packet += f"\n{text}"
            else:
                # Если пакет переполнен, отправляем текущий пакет и начинаем новый
                logger.info("[РЕКОМЕНДАЦИИ]: Отправка пакета в ChatGPT для генерации рекомендаций.")
                response_text = await self.request_with_retries(current_packet, max_retries, max_tokens=max_length)
                recommendations += response_text
                logger.debug(f"[РЕКОМЕНДАЦИИ]: Результат от ChatGPT для текущего пакета: {response_text[:500]}...")
                
                # Обновляем current_packet новым текстом
                current_packet = text
                logger.debug("[РЕКОМЕНДАЦИИ]: Новый пакет начат.")

        # Отправляем остаток пакета, если он есть
        if current_packet:
            logger.info("[РЕКОМЕНДАЦИИ]: Отправка последнего пакета в ChatGPT.")
            response_text = await self.request_with_retries(current_packet, max_retries, max_tokens=max_length)
            recommendations += response_text
            logger.debug(f"[РЕКОМЕНДАЦИИ]: Результат от ChatGPT для последнего пакета: {response_text[:500]}...")

        # Финальный лог рекомендации перед возвратом
        logger.info("[РЕКОМЕНДАЦИИ]: Генерация рекомендаций завершена.")
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Итоговые рекомендации: {recommendations[:1000]}...")  # Логируем первые 1000 символов рекомендаций
        return recommendations


    async def request_with_retries(self, text_packet, max_retries=3, max_tokens=1000):
        """
        Запрос к ChatGPT с поддержкой динамической разбивки `text_packet` на подзапросы,
        поддержкой повторных попыток и лимитом по токенам.
        """
        logger.info("[РЕКОМЕНДАЦИИ]: Инициализация процесса отправки запросов с динамической подстройкой и обработкой ошибок.")
        
        # Разбиваем text_packet на подзапросы, чтобы каждый не превышал max_tokens
        sub_requests = self.split_text_into_chunks(text_packet, max_length=max_tokens)
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Текст разбит на {len(sub_requests)} блока(ов) для отправки. Размер блоков: max_tokens={max_tokens}")
        
        full_recommendations = []  # Список для всех рекомендаций

        for i, sub_request in enumerate(sub_requests):
            prompt = f"На основе данных звонков и метрик: {sub_request}\nПредоставьте рекомендации по улучшению работы оператора."
            logger.info(f"[РЕКОМЕНДАЦИИ]: Подготовка отправки для блока {i + 1}/{len(sub_requests)}. Длина блока: {len(sub_request)} символов.")
            
            for attempt in range(max_retries):
                try:
                    logger.debug(f"[РЕКОМЕНДАЦИИ]: Попытка {attempt + 1} для блока {i + 1}. Запрос отправляется к API ChatGPT.")
                    
                    # Отправка запроса к API с учетом лимита max_tokens
                    response = await self.client.chat.completions.create(
                        model=self.model,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=max_tokens,
                        temperature=0.5,
                    )
                    recommendation = response.choices[0].message.content.strip()
                    full_recommendations.append(recommendation)
                    
                    logger.info(f"[РЕКОМЕНДАЦИИ]: Рекомендация успешно получена с попытки {attempt + 1} для блока {i + 1}.")
                    logger.debug(f"[РЕКОМЕНДАЦИИ]: Ответ от ChatGPT для блока {i + 1} (первые 500 символов): {recommendation[:500]}...")
                    break  # Успешно обработанный блок, выходим из цикла повторов
                    
                except OpenAIError as e:
                    logger.warning(f"[РЕКОМЕНДАЦИИ]: [Попытка {attempt + 1} для блока {i + 1}] OpenAIError: {e}. Задержка перед повтором.")
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                    
                except Exception as e:
                    logger.error(f"[РЕКОМЕНДАЦИИ]: [Попытка {attempt + 1} для блока {i + 1}] Непредвиденная ошибка: {e}. Повтор через задержку.")
                    await asyncio.sleep(2 ** attempt)

            else:
                # Если все попытки завершились неудачей, фиксируем отсутствие результата для блока
                logger.error(f"[РЕКОМЕНДАЦИИ]: Не удалось получить рекомендацию для блока {i + 1} после всех {max_retries} попыток.")
                full_recommendations.append(f"Не удалось сгенерировать рекомендации для блока {i + 1}.")

        # Объединяем все рекомендации в один итоговый текст
        combined_recommendations = "\n".join(full_recommendations)
        logger.info("[РЕКОМЕНДАЦИИ]: Генерация рекомендаций завершена. Все блоки обработаны.")
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Итоговые рекомендации (первые 1000 символов): {combined_recommendations[:1000]}...")
        
        return combined_recommendations

    def split_text_into_chunks(self, text, max_length=1500):
        """
        Разделяет текст на блоки по длине с учетом лимита max_length (в символах).
        """
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Начало разделения текста на блоки с лимитом {max_length} символов.")
        sentences = text.split('. ')
        chunks = []
        current_chunk = ""

        for sentence in sentences:
            if len(current_chunk) + len(sentence) + 1 <= max_length:
                current_chunk += sentence + ". "
            else:
                chunks.append(current_chunk.strip())
                logger.debug(f"[РЕКОМЕНДАЦИИ]: Добавлен блок размером {len(current_chunk.strip())} символов.")
                current_chunk = sentence + ". "

        if current_chunk:
            chunks.append(current_chunk.strip())
            logger.debug(f"[РЕКОМЕНДАЦИИ]: Добавлен последний блок размером {len(current_chunk.strip())} символов.")

        logger.info(f"[РЕКОМЕНДАЦИИ]: Разделение завершено. Получено {len(chunks)} блок(ов).")
        return chunks


        
    def create_report(self, operator_metrics, recommendations, report_date, name):
            # Используем operator_metrics для создания отчета
            """
            Форматирование отчета на основе метрик оператора и рекомендаций.
            """
            report = f"""
            📊 Отчет за период: {report_date}
            Оператор {name} с extension {operator_metrics.get('extension', 'Не указан')} выполнил следующие действия во время звонков:
            
            1. Общая статистика по звонкам:
            - Всего звонков: {operator_metrics.get('total_calls', 'Нет данных')}
            - Принято звонков: {operator_metrics.get('accepted_calls', 'Нет данных')}
            - Записаны на услугу: {operator_metrics.get('booked_services', 'Нет данных')}
            - Конверсия в запись от общего числа звонков: {operator_metrics.get('conversion_rate', 'Нет данных')}%

            2. Качество обработки звонков:
            - Оценка разговоров: {operator_metrics.get('avg_call_rating', 'Нет данных')}

            3. Анализ отмен:
            - Совершено отмен: {operator_metrics.get('total_cancellations', 'Нет данных')}
            - Доля отмен: {operator_metrics.get('cancellation_rate', 'Нет данных')}%

            4. Время обработки звонков:
            - Среднее время разговора: {operator_metrics.get('avg_conversation_time', 'Нет данных')}
            - Среднее время навигации: {operator_metrics.get('avg_navigation_time', 'Нет данных')}
            - Общее время разговоров: {operator_metrics.get('total_talk_time', 'Нет данных')}

            5. Работа с жалобами:
            - Звонки с жалобами: {operator_metrics.get('complaint_calls', 'Нет данных')}
            - Оценка жалоб: {operator_metrics.get('complaint_rating', 'Нет данных')}

            6. Рекомендации:
            {recommendations}
            """
            logger.info(f"[КРОТ]: Отчет успешно отформатирован для оператора {name} с extension {operator_metrics.get('extension')}.")
            return report
        
    ##Тут все запросы в таблицу report. Метод отвечает за сохранение данных в таблицу. Метод сохранения данных в таблицу reports
    ## Метод сохранения данных в таблицу reports
    async def save_report_to_db(self, connection, user_id, name, report_text, period, start_date, end_date, operator_metrics, recommendations):
        """Сохранение отчета в таблицу reports."""
        # Преобразуем report_date в диапазон, если период не дневной
        if period != 'daily':
            report_date = f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
        else:
            report_date = start_date.strftime('%Y-%m-%d')
        try:
        # Приводим user_id к целому числу
            user_id = int(user_id)
        except ValueError as e:
            logger.error(f"[КРОТ]: Ошибка приведения user_id к целому числу: {e}")
            return "Ошибка: user_id должен быть целым числом."
        
        # Приведение числовых параметров к нужным типам и #**Извлекаем метрики из operator_metrics**
        try:
            total_calls = int(operator_metrics['total_calls'])
            accepted_calls = int(operator_metrics['accepted_calls'])
            booked_services = int(operator_metrics['booked_services'])
            conversion_rate = float(operator_metrics['conversion_rate'])
            avg_call_rating = float(operator_metrics['avg_call_rating'])
            total_cancellations = int(operator_metrics['total_cancellations'])
            cancellation_rate = float(operator_metrics['cancellation_rate'])
            total_conversation_time = float(operator_metrics['total_conversation_time'])
            avg_conversation_time = float(operator_metrics['avg_conversation_time'])
            avg_spam_time = float(operator_metrics['avg_spam_time'])
            total_spam_time = float(operator_metrics['total_spam_time'])
            total_navigation_time = float(operator_metrics['total_navigation_time'])
            avg_navigation_time = float(operator_metrics['avg_navigation_time'])
            total_talk_time = float(operator_metrics['total_talk_time'])
            complaint_calls = int(operator_metrics['complaint_calls'])
            complaint_rating = float(operator_metrics['complaint_rating'])
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"[КРОТ]: Ошибка приведения параметров к нужным типам: {e}")
            return "Ошибка: Некорректные данные для сохранения отчета."
    # Логирование параметров перед выполнением запроса
        logger.info(f"[КРОТ]: Начало сохранения отчета для пользователя {user_id} ({name}). Период: {period}, Дата отчета: {report_date}")
        logger.debug(f"[КРОТ]: Параметры перед SQL-запросом: "
                        f"user_id={user_id} ({type(user_id)}), name={name} ({type(name)}), "
                        f"report_text={report_text[:50]}... ({type(report_text)}), "
                        f"period={period} ({type(period)}), "
                        f"report_date={report_date} ({type(report_date)}), "
                        f"total_calls={total_calls} ({type(total_calls)}), "
                        f"accepted_calls={accepted_calls} ({type(accepted_calls)}), "
                        f"booked_services={booked_services} ({type(booked_services)}), "
                        f"conversion_rate={conversion_rate} ({type(conversion_rate)}), "
                        f"avg_call_rating={avg_call_rating} ({type(avg_call_rating)}), "
                        f"total_cancellations={total_cancellations} ({type(total_cancellations)}), "
                        f"cancellation_rate={cancellation_rate} ({type(cancellation_rate)}), "
                        f"total_conversation_time={total_conversation_time} ({type(total_conversation_time)}), "
                        f"avg_conversation_time={avg_conversation_time} ({type(avg_conversation_time)}), "
                        f"avg_spam_time={avg_spam_time} ({type(avg_spam_time)}), "
                        f"total_spam_time={total_spam_time} ({type(total_spam_time)}), "
                        f"total_navigation_time={total_navigation_time} ({type(total_navigation_time)}), "
                        f"avg_navigation_time={avg_navigation_time} ({type(avg_navigation_time)}), "
                        f"total_talk_time={total_talk_time} ({type(total_talk_time)}), "
                        f"complaint_calls={complaint_calls} ({type(complaint_calls)}), "
                        f"complaint_rating={complaint_rating} ({type(complaint_rating)}), "
                        f"recommendations={recommendations[:50]}... ({type(recommendations)})")    
        logger.info(f"[КРОТ]: Сохранение отчета в БД для пользователя {user_id} ({name}) с параметрами: "
                    f"report_date={report_date}, total_calls={total_calls}, accepted_calls={accepted_calls}, "
                    f"booked_services={booked_services}, conversion_rate={conversion_rate}, avg_call_rating={avg_call_rating}, "
                    f"total_cancellations={total_cancellations}, cancellation_rate={cancellation_rate}, "
                    f"total_conversation_time={total_conversation_time}, avg_conversation_time={avg_conversation_time}, "
                    f"total_spam_time={total_spam_time}, avg_spam_time={avg_spam_time}, "
                    f"total_navigation_time={total_navigation_time}, avg_navigation_time={avg_navigation_time}, "
                    f"complaint_calls={complaint_calls}, complaint_rating={complaint_rating}.")
        
        
        
        # Убедимся, что все данные корректны
        assert isinstance(user_id, int), f"user_id должен быть целым числом, получено {type(user_id)}"
        assert isinstance(total_calls, int), f"total_calls должен быть целым числом, получено {type(total_calls)}"
        assert isinstance(conversion_rate, float), f"conversion_rate должен быть числом с плавающей точкой, получено {type(conversion_rate)}"
        assert report_text is not None, "report_text не должен быть None"
        assert recommendations is not None, "recommendations не должны быть None"
        
        # Подготовка параметров для вставки в базу
        params = (
            user_id, name, report_text, period, report_date, total_calls, accepted_calls, booked_services, conversion_rate, 
            avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time, avg_conversation_time, 
            avg_spam_time, total_spam_time, total_navigation_time, avg_navigation_time, total_talk_time, 
            complaint_calls, complaint_rating, recommendations
        )
        # Добавляем логирование параметров перед выполнением запроса
        logger.debug(f"[КРОТ]: Параметры перед SQL-запросом: {params}")
        # SQL-запрос на вставку отчета
        try:
            insert_report_query = """
            INSERT INTO reports (
                user_id, name, report_text, period, report_date, total_calls, accepted_calls, booked_services, conversion_rate, 
                avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time, avg_conversation_time, 
                avg_spam_time, total_spam_time, total_navigation_time, avg_navigation_time, total_talk_time, 
                complaint_calls, complaint_rating, recommendations
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            logger.debug(f"[КРОТ]: SQL-запрос на вставку отчета: {insert_report_query}")
            logger.debug(f"[КРОТ]: Параметры для SQL-запроса: {params}")

            # Открываем курсор и выполняем запрос
            async with connection.cursor() as cursor:
            # Выполняем запрос на вставку отчета
                await cursor.execute(insert_report_query, params)
                await connection.commit()
            logger.info(f"[КРОТ]: Отчет для пользователя {user_id} ({name}) успешно сохранен в таблицу reports.")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при сохранении отчета: {e}")
            return "Ошибка при сохранении отчета."

async def create_async_connection():
    """Создание асинхронного подключения к базе данных."""
    logger.info("[КРОТ]: Попытка асинхронного подключения к базе данных MySQL...")
    try:
        connection = await aiomysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT")),
            cursorclass=aiomysql.DictCursor,
            autocommit=True,
            charset='utf8mb4'
        )
        logger.info("[КРОТ]: Подключено к серверу MySQL")
        return connection
    except aiomysql.Error as e:
        logger.error(f"[КРОТ]: Ошибка при подключении к базе данных: {e}")
        return None

async def execute_async_query(connection, query, params=None, retries=3):
    """Выполнение SQL-запроса с обработкой ошибок и повторными попытками."""
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