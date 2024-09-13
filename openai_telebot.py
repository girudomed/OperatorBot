import asyncio
import logging
import time  # Для замера времени
from report_generator import ReportGenerator
from operator_data import OperatorData
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

class OpenAIReportGenerator:
    def __init__(self, model="gpt-4o-mini"):
        self.report_generator = ReportGenerator(model=model)
        self.operator_data = OperatorData()

    async def generate_report(self, operator_id):
        """
        Генерация отчета для оператора по его ID с использованием данных и OpenAI.
        
        :param operator_id: ID оператора для получения данных
        :return: Сформированный отчет или сообщение об ошибке
        """
        try:
            start_time = time.time()
            # Получение данных оператора из базы данных
            operator_metrics = await self.operator_data.get_operator_metrics(operator_id)
            if not operator_metrics:
                logger.error(f"[КРОТ]: Данные по оператору с ID {operator_id} не найдены.")
                return f"Данные по оператору с ID {operator_id} не найдены."

            # Генерация коучинговой рекомендации с помощью OpenAI на основе данных оператора
            recommendations = await self.generate_coaching_recommendations(operator_metrics)
            
            # Формирование отчета в правильном формате
            report = self.format_report(operator_metrics, recommendations)
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Отчет успешно сгенерирован для оператора {operator_id}. (Время выполнения: {elapsed_time:.4f} сек)")
            return report
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при генерации отчета для оператора {operator_id}: {e}")
            return f"Ошибка при генерации отчета: {e}"

    async def generate_coaching_recommendations(self, operator_metrics, retries=3):
        """
        Создание коучинговой рекомендации для оператора с использованием данных звонков.
        
        :param operator_metrics: Данные оператора, включая оценки и метрики звонков
        :param retries: Количество повторных попыток при сбое запроса
        :return: Сгенерированная рекомендация
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
        
        for attempt in range(retries):
            try:
                start_time = time.time()
                recommendations = await self.report_generator.send_message(coaching_prompt)
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Коучинговая рекомендация успешно сгенерирована (Попытка {attempt + 1}, Время выполнения: {elapsed_time:.4f} сек).")
                return recommendations
            except Exception as e:
                logger.error(f"[КРОТ]: Попытка {attempt + 1} генерации коучинговой рекомендации не удалась: {e}")
                if attempt == retries - 1:
                    logger.error("[КРОТ]: Все попытки генерации коучинговой рекомендации исчерпаны.")
                    return "Не удалось сгенерировать рекомендации. Попробуйте позже."
                await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка перед повторной попыткой

    def format_report(self, operator_metrics, recommendations):
        """
        Форматирование отчета на основе метрик оператора и рекомендаций.
        
        :param operator_metrics: Метрики оператора из базы данных
        :param recommendations: Рекомендации, сгенерированные OpenAI
        :return: Сформированный отчет
        """
        start_time = time.time()
        report = f"""
        📊 Ежедневный отчет за {operator_metrics.get('report_date', 'Не указано')}
        
        1. Общая статистика по звонкам:
        - Всего звонков за день: {operator_metrics.get('total_calls', 'Нет данных')}
        - Принято звонков за день: {operator_metrics.get('answered_calls', 'Нет данных')}
        - Записаны на услугу: {operator_metrics.get('booked_services', 'Нет данных')}
        - Конверсия в запись от общего числа звонков: {operator_metrics.get('conversion_rate', 'Нет данных')}%
        
        2. Качество обработки звонков:
        - Оценка разговоров (средняя по всем клиентам): {operator_metrics.get('average_call_score', 'Нет данных')}
        
        3. Анализ отмен и ошибок:
        - Совершено отмен: {operator_metrics.get('cancellations', 'Нет данных')}
        - Доля отмен от всех звонков: {operator_metrics.get('cancellation_rate', 'Нет данных')}%
        
        4. Время обработки и разговоров:
        - Среднее время разговора при записи: {operator_metrics.get('avg_booking_time', 'Нет данных')}
        - Среднее время разговора со спамом: {operator_metrics.get('avg_spam_time', 'Нет данных')}
        - Среднее время навигации звонков: {operator_metrics.get('avg_navigation_time', 'Нет данных')}
        - Общее время разговоров по телефону: {operator_metrics.get('total_talk_time', 'Нет данных')}
        
        5. Работа с жалобами:
        - Звонки с жалобами: {operator_metrics.get('complaints', 'Нет данных')}
        - Оценка обработки жалобы: {operator_metrics.get('complaint_handling_score', 'Нет данных')}
        
        6. Рекомендации на основе данных:
        {recommendations}
        """
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Отчет успешно отформатирован (Время выполнения: {elapsed_time:.4f} сек).")
        return report
