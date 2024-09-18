import asyncio
import logging
import time  # Для замера времени
import os
from operator_data import OperatorData
from logger_utils import setup_logging
import openai

# Настройка логирования
logger = setup_logging()

class OpenAIReportGenerator:
    def __init__(self, db_manager, model="gpt-3.5-turbo"):
        self.db_manager = db_manager
        self.operator_data = OperatorData(db_manager)
        self.model = model
        # Настройка OpenAI API ключа из переменных окружения
        openai.api_key = os.getenv('OPENAI_API_KEY')  # Убедитесь, что ключ установлен в переменных окружения
        if not openai.api_key:
            logger.error("OpenAI API ключ не найден. Пожалуйста, установите переменную окружения OPENAI_API_KEY.")

    async def generate_report(self, operator_id):
        """
        Генерация отчета для оператора по его ID с использованием данных и OpenAI.
        """
        try:
            start_time = time.time()
            # Получение данных оператора из базы данных
            operator_metrics = await self.operator_data.get_operator_metrics(operator_id)
            if not operator_metrics:
                logger.error(f"[КРОТ]: Данные по оператору с ID {operator_id} не найдены.")
                return f"Данные по оператору с ID {operator_id} не найдены."

            # Генерация коучинговой рекомендации с помощью OpenAI на основе данных оператора
            recommendations = await self.generate_with_retries(operator_metrics)

            if recommendations is None:
                recommendations = "Не удалось получить рекомендации в данный момент."

            # Формирование отчета в правильном формате
            report = self.format_report(operator_metrics, recommendations)
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Отчет успешно сгенерирован для оператора {operator_id}. (Время выполнения: {elapsed_time:.4f} сек)")
            return report
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при генерации отчета для оператора {operator_id}: {e}")
            return f"Ошибка при генерации отчета: {e}"

    async def generate_with_retries(self, operator_metrics, max_retries=3):
        """
        Генерация коучинговых рекомендаций с повторными попытками при ошибках.
        """
        for attempt in range(max_retries):
            try:
                # Попытка сгенерировать рекомендации с помощью OpenAI
                return await self.generate_coaching_recommendations(operator_metrics)
            except openai.error.OpenAIError as e:
                logger.warning(f"Попытка {attempt + 1} генерации рекомендаций не удалась: {e}")
                await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка перед следующей попыткой
            except Exception as e:
                logger.error(f"Непредвиденная ошибка при генерации рекомендаций: {e}")
                if attempt == max_retries - 1:
                    logger.error("[КРОТ]: Все попытки генерации рекомендаций исчерпаны.")
                    return None
        return None

    async def generate_coaching_recommendations(self, operator_metrics):
        """
        Создание коучинговой рекомендации для оператора на основе данных звонков.
        """
        coaching_prompt = f"""
Оператор с идентификатором {operator_metrics.get('operator_id', 'Не указан')} выполнил следующие действия во время звонков:

1. Приветливость и эмпатия сотрудника: {operator_metrics.get('empathy_score', 'Не указано')}
2. Понимание запроса клиента: {operator_metrics.get('understanding_score', 'Не указано')}
3. Качество ответов и информирование: {operator_metrics.get('response_quality_score', 'Не указано')}
4. Эффективность общения и решение проблемы: {operator_metrics.get('problem_solving_score', 'Не указано')}
5. Завершение звонка: {operator_metrics.get('call_closing_score', 'Не указано')}
Общая оценка звонка: {operator_metrics.get('total_call_score', 'Не указано')}

### Рекомендации:
На основе вышеуказанных данных, предоставь персонализированные рекомендации для улучшения работы оператора.
"""
        try:
            start_time = time.time()
            response = await openai.Completion.acreate(
                model=self.model,
                prompt=coaching_prompt,
                max_tokens=150,
                n=1,
                stop=None,
                temperature=0.7,
            )
            recommendations = response.choices[0].text.strip()
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Коучинговая рекомендация успешно сгенерирована (Время выполнения: {elapsed_time:.4f} сек).")
            return recommendations
        except openai.error.OpenAIError as e:
            logger.error(f"[КРОТ]: Ошибка при генерации рекомендаций через OpenAI API: {e}")
            raise
        except Exception as e:
            logger.error(f"[КРОТ]: Непредвиденная ошибка при генерации рекомендаций: {e}")
            raise

    def format_report(self, operator_metrics, recommendations):
        """
        Форматирование отчета на основе метрик оператора и рекомендаций.
        """
        start_time = time.time()
        report = f"""
📊 Ежедневный отчет за {operator_metrics.get('report_date', 'Не указано')}

1. Общая статистика по звонкам:
- Всего звонков за день: {operator_metrics.get('total_calls', 'Нет данных')}
- Принято звонков за день: {operator_metrics.get('accepted_calls', 'Нет данных')}
- Записаны на услугу: {operator_metrics.get('booked_services', 'Нет данных')}
- Конверсия в запись от общего числа звонков: {operator_metrics.get('conversion_rate', 'Нет данных')}%

2. Качество обработки звонков:
- Оценка разговоров (средняя по всем клиентам): {operator_metrics.get('avg_call_rating', 'Нет данных')}

3. Анализ отмен и ошибок:
- Совершено отмен: {operator_metrics.get('total_cancellations', 'Нет данных')}
- Доля отмен от всех звонков: {operator_metrics.get('cancellation_rate', 'Нет данных')}%

4. Время обработки и разговоров:
- Среднее время разговора: {operator_metrics.get('avg_conversation_time', 'Нет данных')}
- Среднее время со спамом: {operator_metrics.get('avg_spam_time', 'Нет данных')}
- Среднее время навигации: {operator_metrics.get('avg_navigation_time', 'Нет данных')}
- Общее время разговоров: {operator_metrics.get('total_talk_time', 'Нет данных')}

5. Работа с жалобами:
- Звонки с жалобами: {operator_metrics.get('complaint_calls', 'Нет данных')}
- Оценка обработки жалобы: {operator_metrics.get('complaint_rating', 'Нет данных')}

6. Рекомендации на основе данных:
{recommendations}
"""
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Отчет успешно отформатирован (Время выполнения: {elapsed_time:.4f} сек).")
        return report
