# progress_data.py
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Union, Tuple
import logging
import aiomysql
import numpy as np
from dataclasses import dataclass
from numpy.typing import NDArray
from visualization import MetricsVisualizer, GlobalConfig, PlotConfig
import pandas as pd

logger = logging.getLogger(__name__)

@dataclass
class MetricGroup:
    """Группа метрик с их описанием"""
    metrics: List[str]
    description: str

# Константы для группировки метрик
METRIC_GROUPS = {
    'quality': MetricGroup(
        metrics=['avg_call_rating', 'avg_lead_call_rating', 'avg_cancel_score'],
        description='Метрики качества обслуживания'
    ),
    'conversion': MetricGroup(
        metrics=['conversion_rate_leads', 'booked_services', 'total_calls'],
        description='Метрики конверсии'
    ),
    'call_handling': MetricGroup(
        metrics=['missed_calls', 'cancellation_rate', 'complaint_calls'],
        description='Метрики обработки звонков'
    ),
    'time': MetricGroup(
        metrics=['avg_conversation_time', 'avg_navigation_time', 'avg_service_time'],
        description='Временные метрики'
    )
}

# Для удобства создаем отдельные списки метрик
QUALITY_METRICS = METRIC_GROUPS['quality'].metrics
CONVERSION_METRICS = METRIC_GROUPS['conversion'].metrics
CALL_HANDLING_METRICS = METRIC_GROUPS['call_handling'].metrics
TIME_METRICS = METRIC_GROUPS['time'].metrics

# Типы для аннотаций
DateType = Union[date, datetime]
ReportDict = Dict[str, Any]

class ProgressData:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(self.__class__.__name__)
        if not isinstance(self.logger, logging.Logger):
            raise TypeError(f"Logger не является объектом logging.Logger: {type(self.logger)}")
        self.logger.info("Инициализация ProgressData")
        self.logger.debug(f"Инициализирован ProgressData с logger={self.logger}")

    def _validate_dates(self, start_date: DateType, end_date: DateType) -> Tuple[date, date]:
        """Валидация и преобразование дат"""
        self.logger.debug(f"Валидация дат: start_date={start_date}, end_date={end_date}")
        try:
            start = start_date.date() if isinstance(start_date, datetime) else start_date
            end = end_date.date() if isinstance(end_date, datetime) else end_date
            if start > end:
                self.logger.error(f"Некорректный диапазон дат: start_date={start} позже end_date={end}")
                raise ValueError("Дата начала не может быть позже даты окончания")
            self.logger.debug(f"Даты валидны: start={start}, end={end}")
            return start, end
        except AttributeError as e:
            self.logger.error(f"Ошибка формата даты: {e}", exc_info=True)
            raise ValueError(f"Некорректный формат даты: {e}")

    def _validate_operator_id(self, operator_id: int) -> None:
        """Валидация ID оператора"""
        self.logger.debug(f"Валидация user_id: {operator_id}")
        if not isinstance(operator_id, int) or operator_id <= 0:
            self.logger.error(f"Некорректный ID оператора: {operator_id}")
            raise ValueError(f"Некорректный ID оператора: {operator_id}")
        self.logger.debug(f"ID оператора {operator_id} валиден")

    def _validate_reports(self, reports: List[ReportDict]) -> None:
        """Валидация отчетов"""
        self.logger.debug(f"Начало валидации отчетов. Количество отчетов: {len(reports)}")
        if not isinstance(reports, list):
            self.logger.error("Отчеты должны быть списком")
            raise ValueError("Отчеты должны быть списком")

        required_fields = {'user_id', 'report_date'}
        for i, report in enumerate(reports):
            if not isinstance(report, dict):
                self.logger.error(f"Отчет #{i} не является словарем")
                raise ValueError("Каждый отчет должен быть словарем")

            missing_fields = required_fields - set(report.keys())
            if missing_fields:
                self.logger.error(f"В отчете #{i} отсутствуют обязательные поля: {missing_fields}")
                raise ValueError(f"В отчете отсутствуют обязательные поля: {missing_fields}")

        self.logger.debug("Валидация отчетов успешно завершена")

    def _safe_convert_value(self, value: Any, default: Any = None) -> Any:
        """Безопасное преобразование значения"""
        self.logger.debug(f"Преобразование значения: {value}, default={default}")
        try:
            if value is None or value == '':
                return default
            if isinstance(value, (int, float)):
                return float(value)
            return default
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Ошибка преобразования значения {value}: {e}")
            return default

    def _process_db_row(self, row: Dict[str, Any]) -> ReportDict:
        """Обработка строки из базы данных"""
        self.logger.debug(f"Обработка строки БД: user_id={row.get('user_id')}, date={row.get('report_date')}")
        try:
            return {
                'user_id': int(row['user_id']),
                'report_date': row['report_date'],
                'total_calls': self._safe_convert_value(row['total_calls']),
                'accepted_calls': self._safe_convert_value(row['accepted_calls']),
                'missed_calls': self._safe_convert_value(row['missed_calls']),
                'booked_services': self._safe_convert_value(row['booked_services']),
                'conversion_rate_leads': self._safe_convert_value(row['conversion_rate_leads']),
                'avg_call_rating': self._safe_convert_value(row['avg_call_rating']),
                'avg_lead_call_rating': self._safe_convert_value(row['avg_lead_call_rating']),
                'total_cancellations': self._safe_convert_value(row['total_cancellations']),
                'avg_cancel_score': self._safe_convert_value(row['avg_cancel_score']),
                'cancellation_rate': self._safe_convert_value(row['cancellation_rate']),
                'complaint_calls': self._safe_convert_value(row['complaint_calls']),
                'complaint_rating': self._safe_convert_value(row['complaint_rating']),
                'avg_conversation_time': self._safe_convert_value(row['avg_conversation_time']),
                'avg_navigation_time': self._safe_convert_value(row['avg_navigation_time']),
                'avg_service_time': self._safe_convert_value(row['avg_service_time'])
            }
        except Exception as e:
            self.logger.error(f"Ошибка при обработке строки из БД: {e}", exc_info=True)
            raise ValueError(f"Некорректные данные в строке: {e}")

    async def get_operator_reports(self, operator_id: int, start_date: DateType, end_date: DateType) -> List[ReportDict]:
        """
        Получение отчетов для конкретного оператора за указанный период из таблицы reports.
        Возвращает список словарей с данными по дням.
        """
        self._validate_operator_id(operator_id)
        start_date, end_date = self._validate_dates(start_date, end_date)

        query = """
        SELECT 
            user_id,
            report_date,
            total_calls,
            accepted_calls,
            missed_calls,
            booked_services,
            conversion_rate_leads,
            avg_call_rating,
            avg_lead_call_rating,
            total_cancellations,
            avg_cancel_score,
            cancellation_rate,
            complaint_calls,
            complaint_rating,
            avg_conversation_time,
            avg_navigation_time,
            avg_service_time
        FROM reports
        WHERE user_id = %s
          AND report_date BETWEEN %s AND %s
        ORDER BY report_date ASC
        """
        try:
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (operator_id, start_date, end_date))
                    rows = await cursor.fetchall()

            if not rows:
                self.logger.info(f"Не найдены отчеты для оператора {operator_id} за период {start_date} - {end_date}.")
                return []

            reports = []
            for row in rows:
                reports.append(self._process_db_row(row))

            return reports
        except Exception as e:
            self.logger.error(f"Ошибка при получении отчетов для оператора {operator_id}: {e}", exc_info=True)
            return []

    async def get_all_operators_reports(self, start_date: DateType, end_date: DateType) -> List[ReportDict]:
        """
        Получение отчетов для всех операторов за указанный период.
        Возвращает список словарей с данными по дням и операторам.
        """
        start_date, end_date = self._validate_dates(start_date, end_date)

        query = """
        SELECT
            user_id,
            report_date,
            total_calls,
            accepted_calls,
            missed_calls,
            booked_services,
            conversion_rate_leads,
            avg_call_rating,
            avg_lead_call_rating,
            total_cancellations,
            avg_cancel_score,
            cancellation_rate,
            complaint_calls,
            complaint_rating,
            avg_conversation_time,
            avg_navigation_time,
            avg_service_time
        FROM reports
        WHERE report_date BETWEEN %s AND %s
        ORDER BY report_date ASC, user_id ASC
        """
        try:
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (start_date, end_date))
                    rows = await cursor.fetchall()

            if not rows:
                self.logger.info(f"Не найдены отчеты для всех операторов за период {start_date} - {end_date}.")
                return []

            reports = []
            for row in rows:
                reports.append(self._process_db_row(row))

            return reports
        except Exception as e:
            self.logger.error(f"Ошибка при получении отчетов для всех операторов: {e}", exc_info=True)
            return []

    def _calculate_trend_value(self, values: List[float]) -> Dict[str, Any]:
        """Рассчитывает значение тренда для списка значений"""
        self.logger.debug(f"Расчет тренда для {len(values)} значений")
        if not values or len(values) < 2:
            self.logger.info("Недостаточно данных для расчета тренда")
            return {"trend": "insufficient_data"}

        try:
            x = np.arange(len(values))
            y = np.array(values)

            mask = ~np.isnan(y)
            if np.sum(mask) < 2:
                self.logger.info("Недостаточно валидных значений после удаления NaN")
                return {"trend": "insufficient_data"}

            x = x[mask]
            y = y[mask]

            slope, intercept = np.polyfit(x, y, 1)
            y_pred = slope * x + intercept
            r_squared = 1 - (np.sum((y - y_pred) ** 2) / np.sum((y - np.mean(y)) ** 2))

            if abs(slope) < 0.01:
                trend = "stable"
            else:
                trend = "up" if slope > 0 else "down"

            result = {
                "trend": trend,
                "slope": float(slope),
                "r_squared": float(r_squared),
                "start_value": float(y[0]),
                "end_value": float(y[-1]),
                "change_percent": float((y[-1] - y[0]) / y[0] * 100) if y[0] != 0 else None
            }
            self.logger.info(f"Рассчитан тренд: {trend}, наклон: {slope:.4f}, R²: {r_squared:.4f}")
            return result
        except Exception as e:
            self.logger.error(f"Ошибка при расчете тренда: {e}", exc_info=True)
            return {"trend": "error", "error": str(e)}

    def _calculate_group_trend(self, metric_trends: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Рассчитывает общий тренд для группы метрик"""
        self.logger.debug(f"Расчет группового тренда для {len(metric_trends)} метрик")
        if not metric_trends:
            self.logger.info("Нет данных для расчета группового тренда")
            return {"trend": "no_data"}

        valid_trends = [
            trend for trend in metric_trends.values()
            if trend["trend"] not in ["insufficient_data", "error", "no_data"]
        ]

        self.logger.debug(f"Найдено {len(valid_trends)} валидных трендов из {len(metric_trends)}")

        if not valid_trends:
            self.logger.info("Нет валидных трендов для расчета группового тренда")
            return {"trend": "insufficient_data"}

        avg_slope = np.mean([t["slope"] for t in valid_trends])
        avg_r_squared = np.mean([t["r_squared"] for t in valid_trends])

        if abs(avg_slope) < 0.01:
            trend = "stable"
        else:
            trend = "up" if avg_slope > 0 else "down"

        result = {
            "trend": trend,
            "avg_slope": float(avg_slope),
            "avg_r_squared": float(avg_r_squared),
            "metrics_count": len(valid_trends)
        }
        self.logger.info(f"Рассчитан групповой тренд: {trend}, средний наклон: {avg_slope:.4f}")
        return result

    def calculate_trends(self, reports: List[ReportDict], metric_name: str = None, group: str = None) -> Dict[str, Any]:
        """
        Рассчитывает тренды для заданной метрики или группы метрик.
        """
        self.logger.info(f"Расчет трендов: metric_name={metric_name}, group={group}")
        self._validate_reports(reports)

        if not reports:
            self.logger.info("Нет данных для расчета трендов")
            return {"trend": "no_data"}

        reports = sorted(reports, key=lambda x: x['report_date'])
        self.logger.debug(f"Отчеты отсортированы по дате, период: {reports[0]['report_date']} - {reports[-1]['report_date']}")

        if metric_name:
            self.logger.debug(f"Расчет тренда для метрики: {metric_name}")
            values = [report.get(metric_name, 0) for report in reports]
            return self._calculate_trend_value(values)

        if group:
            if group not in METRIC_GROUPS:
                self.logger.error(f"Неизвестная группа метрик: {group}")
                raise ValueError(f"Неизвестная группа метрик: {group}")

            metrics = METRIC_GROUPS[group].metrics
            self.logger.debug(f"Расчет трендов для группы {group}, метрики: {metrics}")

            trends = {}
            for metric in metrics:
                self.logger.debug(f"Расчет тренда для метрики {metric} в группе {group}")
                values = [report.get(metric, 0) for report in reports]
                trends[metric] = self._calculate_trend_value(values)

            group_trend = self._calculate_group_trend(trends)

            result = {
                "metrics": trends,
                "group_trend": group_trend,
                "description": METRIC_GROUPS[group].description
            }
            self.logger.info(f"Завершен расчет трендов для группы {group}")
            return result

        self.logger.info("Расчет трендов для всех групп")
        return {
            group_name: self.calculate_trends(reports, group=group_name)
            for group_name in METRIC_GROUPS
        }

    def calculate_average_metrics(self, reports: List[ReportDict]) -> Dict[str, float]:
        """
        Рассчитывает средние значения метрик по отчетам.
        """
        self.logger.info("Расчет средних значений метрик")
        self._validate_reports(reports)

        if not reports:
            self.logger.info("Нет данных для расчета средних значений")
            return {}

        numeric_metrics = [
            'total_calls', 'accepted_calls', 'missed_calls', 'booked_services',
            'conversion_rate_leads', 'avg_call_rating', 'avg_lead_call_rating',
            'total_cancellations', 'avg_cancel_score', 'cancellation_rate',
            'complaint_calls', 'complaint_rating', 'avg_conversation_time',
            'avg_navigation_time', 'avg_service_time'
        ]
        self.logger.debug(f"Расчет средних значений для метрик: {numeric_metrics}")

        sums = {m: 0.0 for m in numeric_metrics}
        counts = {m: 0 for m in numeric_metrics}

        for report in reports:
            for m in numeric_metrics:
                val = report.get(m)
                if isinstance(val, (int, float)):
                    sums[m] += val
                    counts[m] += 1

        averages = {m: sums[m] / counts[m] for m in numeric_metrics if counts[m] > 0}
        self.logger.debug(f"Рассчитаны средние значения для {len(averages)} метрик")
        return averages

    def group_by_operator(self, reports: List[ReportDict]) -> Dict[int, List[ReportDict]]:
        """
        Группирует отчеты по user_id.
        """
        self.logger.info("Группировка отчетов по операторам")
        self._validate_reports(reports)

        grouped = {}
        for report in reports:
            op_id = report.get('user_id')
            if op_id not in grouped:
                grouped[op_id] = []
            grouped[op_id].append(report)

        self.logger.debug(f"Отчеты сгруппированы по {len(grouped)} операторам")
        return grouped

    def filter_by_date_range(self, reports: List[ReportDict], date_range: str) -> List[ReportDict]:
        """
        Фильтрует отчеты по диапазону дат.

        Args:
            reports (List[ReportDict]): Список отчетов для фильтрации.
            date_range (str): Диапазон дат в формате "YYYY-MM-DD - YYYY-MM-DD".

        Returns:
            List[ReportDict]: Список отчетов, которые попадают в указанный диапазон.
        """
        self.logger.info(f"Фильтрация отчетов по диапазону дат: {date_range}")
        
        # Валидация входных отчетов
        self._validate_reports(reports)

        # Парсим диапазон дат
        try:
            start_date, end_date = self._parse_date_range(date_range)
        except ValueError as e:
            self.logger.error(f"Некорректный диапазон дат: {date_range}. Ошибка: {e}")
            raise

        filtered = []
        skipped = 0

        for report in reports:
            try:
                # Получаем дату из отчета
                report_date = report.get('report_date')
                if isinstance(report_date, str):
                    report_date = datetime.strptime(report_date.strip(), '%Y-%m-%d').date()
                elif isinstance(report_date, datetime):
                    report_date = report_date.date()
                elif not isinstance(report_date, date):
                    raise ValueError(f"Некорректный формат даты: {report_date}")

                # Фильтрация по диапазону
                if start_date <= report_date <= end_date:
                    filtered.append(report)
                else:
                    self.logger.debug(f"Отчет не входит в диапазон: {report}")
            except Exception as e:
                self.logger.warning(f"Пропущен отчет с некорректной датой: {report.get('report_date')}. Ошибка: {e}")
                skipped += 1

        self.logger.info(f"Фильтрация завершена. Отфильтровано: {len(filtered)} отчетов, пропущено: {skipped}.")
        return filtered

    def get_metrics_by_group(self, reports: List[ReportDict]) -> Dict[str, Dict[str, List[float]]]:
        """
        Группирует метрики по категориям и возвращает их значения.
        """
        self.logger.info("Группировка метрик по категориям")
        self._validate_reports(reports)

        metrics_groups = {
            'quality': {metric: [] for metric in QUALITY_METRICS},
            'conversion': {metric: [] for metric in CONVERSION_METRICS},
            'call_handling': {metric: [] for metric in CALL_HANDLING_METRICS},
            'time': {metric: [] for metric in TIME_METRICS}
        }

        for report in reports:
            for group, metrics in metrics_groups.items():
                for metric in metrics:
                    value = report.get(metric)
                    if value is not None and value != 0:
                        metrics_groups[group][metric].append(value)

        for group, metrics in metrics_groups.items():
            self.logger.debug(f"Группа {group}: собрано {sum(len(vals) for vals in metrics.values())} значений")

        return metrics_groups

    def calculate_statistics(self, values: List[float]) -> Dict[str, float]:
        """
        Рассчитывает статистические показатели для списка значений.
        """
        self.logger.debug(f"Расчет статистики для {len(values)} значений")
        if not values:
            self.logger.info("Нет данных для расчета статистики")
            return {
                'mean': None,
                'median': None,
                'std': None,
                'min': None,
                'max': None
            }

        try:
            result = {
                'mean': float(np.mean(values)),
                'median': float(np.median(values)),
                'std': float(np.std(values)) if len(values) > 1 else 0.0,
                'min': float(np.min(values)),
                'max': float(np.max(values))
            }
            self.logger.debug(f"Статистика рассчитана: mean={result['mean']:.2f}, median={result['median']:.2f}")
            return result
        except Exception as e:
            self.logger.error(f"Ошибка при расчете статистики: {e}", exc_info=True)
            return {
                'mean': None,
                'median': None,
                'std': None,
                'min': None,
                'max': None
            }

    def calculate_group_statistics(self, reports: List[ReportDict]) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Рассчитывает статистику для каждой группы метрик.
        """
        self.logger.info("Расчет статистики по группам метрик")
        self._validate_reports(reports)

        metrics_groups = self.get_metrics_by_group(reports)
        statistics = {}

        for group, metrics in metrics_groups.items():
            self.logger.debug(f"Расчет статистики для группы {group}")
            statistics[group] = {}
            for metric, values in metrics.items():
                statistics[group][metric] = self.calculate_statistics(values)

        self.logger.info(f"Статистика рассчитана для {len(statistics)} групп")
        return statistics

    def create_visualizer(self, output_dir: str) -> MetricsVisualizer:
        """
        Создает экземпляр визуализатора с настроенной конфигурацией.
        """
        global_config = GlobalConfig(
            show_grid=True,
            show_legend=True,
            value_labels=True,
            show_trend=True,
            show_confidence_interval=True,
            confidence_level=0.95,
            remove_outliers=True,
            outlier_threshold=2.0,
            normalize_data=False,
            title_fontsize=14,
            axis_label_fontsize=12,
            legend_fontsize=10
        )

        plot_configs = {
            'quality': PlotConfig(
                metrics=QUALITY_METRICS,
                title="Метрики качества",
                xlabel="Дата",
                ylabel="Значение"
            ),
            'conversion': PlotConfig(
                metrics=CONVERSION_METRICS,
                title="Метрики конверсии",
                xlabel="Дата",
                ylabel="Процент"
            ),
            'call_handling': PlotConfig(
                metrics=CALL_HANDLING_METRICS,
                title="Метрики обработки звонков",
                xlabel="Дата",
                ylabel="Количество"
            ),
            'time': PlotConfig(
                metrics=TIME_METRICS,
                title="Временные метрики",
                xlabel="Дата",
                ylabel="Время (сек)"
            )
        }

        return MetricsVisualizer(
            output_dir=output_dir,
            configs=plot_configs,
            global_config=global_config
        )

    def visualize_metrics(self, reports: List[ReportDict], output_dir: str) -> Dict[str, str]:
        """
        Создает визуализации для всех групп метрик.
        """
        self.logger.info("Начало визуализации метрик")
        try:
            visualizer = self.create_visualizer(output_dir)
            metrics_by_group = self.get_metrics_by_group(reports)
            plot_paths = {}
            for group_name, metrics in metrics_by_group.items():
                self.logger.debug(f"Создание графика для группы {group_name}")
                plot_data = pd.DataFrame(metrics)
                if not plot_data.empty:
                    plot_data['date'] = [report['report_date'] for report in reports]
                    result = visualizer.create_plot(
                        name=group_name,
                        data=plot_data,
                        date_column='date'
                    )
                    if result.file_path:
                        plot_paths[group_name] = result.file_path
                        self.logger.info(f"График для группы {group_name} сохранен: {result.file_path}")
                    else:
                        self.logger.warning(
                            f"Не удалось создать график для группы {group_name}: "
                            f"{'; '.join(result.warnings)}"
                        )
            return plot_paths
        except Exception as e:
            self.logger.error(f"Ошибка при создании визуализаций: {e}", exc_info=True)
            return {}

    async def visualize_operator_progress(
        self,
        operator_id: int,
        start_date: DateType,
        end_date: DateType,
        output_dir: str
    ) -> Dict[str, str]:
        """
        Создает визуализации прогресса оператора за период.
        """
        try:
            reports = await self.get_operator_reports(operator_id, start_date, end_date)
            if not reports:
                self.logger.warning(f"Нет данных для оператора {operator_id} за период {start_date} - {end_date}")
                return {}
            return self.visualize_metrics(reports, output_dir)
        except Exception as e:
            self.logger.error(
                f"Ошибка при создании визуализаций для оператора {operator_id}: {e}",
                exc_info=True
            )
            return {}
        
    def _validate_operator_id(self, operator_id: int) -> None:
        if operator_id <= 0:
            raise ValueError(f"Некорректный operator_id: {operator_id}")

    def _parse_date_range(self, period_str: str) -> tuple[date, date]:
        """
        Парсит строку вида 'YYYY-MM-DD - YYYY-MM-DD' в (start_date, end_date).
        """
        try:
            start_str, end_str = period_str.split("-")
            start_date = datetime.strptime(start_str.strip(), "%Y-%m-%d").date()
            end_date = datetime.strptime(end_str.strip(), "%Y-%m-%d").date()
            if start_date > end_date:
                raise ValueError("Начальная дата не может быть больше конечной.")
            return start_date, end_date
        except Exception as e:
            raise ValueError(f"Некорректный формат диапазона дат '{period_str}': {e}")

    async def get_operator_progress(self, operator_id: int, period: str) -> Optional[Dict[str, Any]]:
        """
        Получает дневные данные прогресса оператора за указанный период 
        (daily, weekly, monthly, yearly или диапазон дат "YYYY-MM-DD - YYYY-MM-DD"),
        возвращая полноценный тайм-сериес.

        Структура результата:
        {
        "quality": {
            "YYYY-MM-DD": { "avg_call_rating": ..., "avg_lead_call_rating": ..., ...},
            "YYYY-MM-DD": {...},
            ...
        },
        "conversion": {
            "YYYY-MM-DD": {...},
            ...
        },
        "call_handling": {
            "YYYY-MM-DD": {...},
            ...
        },
        "time": {
            "YYYY-MM-DD": {...},
            ...
        },
        "summary": {
            "user_id": int,
            "operator_name": str,
            "dates_used": [список дат],
            "total_reports": int,
            ...
        }
        }

        Если данных нет, возвращает None.
        """
        # Валидируем operator_id
        self._validate_operator_id(operator_id)

        # Определяем временные рамки
        if " - " in period:  # кастомный диапазон (например, "2024-01-01 - 2024-01-10")
            try:
                start_date, end_date = self._parse_date_range(period)
            except ValueError as e:
                self.logger.error(f"Ошибка парсинга диапазона дат: '{period}'. {e}")
                return None
        else:
            period_map = {
                "daily": 1,
                "weekly": 7,
                "monthly": 30,
                "yearly": 365
            }
            days = period_map.get(period)
            if not days:
                # Некорректный period
                self.logger.error(f"Некорректный период: {period}")
                raise ValueError(f"Некорректный период: {period}")
            now = date.today()
            start_date = now - timedelta(days=days)
            end_date = now

        query = """
        SELECT
        user_id,
        name,
        report_date,

        SUM(total_calls) AS total_calls,
        SUM(accepted_calls) AS accepted_calls,
        SUM(missed_calls) AS missed_calls,
        SUM(booked_services) AS booked_services,
        SUM(total_cancellations) AS total_cancellations,

        AVG(conversion_rate) AS avg_conversion_rate,
        AVG(conversion_rate_leads) AS avg_conversion_rate_leads,

        AVG(avg_call_rating) AS avg_call_rating,
        AVG(avg_lead_call_rating) AS avg_lead_call_rating,
        AVG(avg_cancel_score) AS avg_cancel_score,
        AVG(complaint_rating) AS avg_complaint_rating,

        AVG(avg_conversation_time) AS avg_conversation_time,
        AVG(avg_navigation_time) AS avg_navigation_time,
        AVG(avg_service_time) AS avg_service_time,
        AVG(avg_spam_time) AS avg_spam_time,

        SUM(total_conversation_time) AS total_conversation_time,
        SUM(total_spam_time) AS total_spam_time,

        AVG(missed_rate) AS avg_missed_rate

        FROM reports
        WHERE
        user_id = %s
        AND report_date BETWEEN %s AND %s
        GROUP BY user_id, name, report_date
        ORDER BY report_date ASC
        """

        try:
            async with self.db_manager.acquire() as connection:
                async with connection.cursor(aiomysql.DictCursor) as cursor:
                    self.logger.debug(
                        f"[get_operator_progress] Выполнение SQL:\n{query}\n"
                        f"Параметры: (operator_id={operator_id}, "
                        f"start_date={start_date}, end_date={end_date})"
                    )
                    await cursor.execute(query, (operator_id, start_date, end_date))
                    rows = await cursor.fetchall()

            if not rows:
                self.logger.warning(
                    f"[get_operator_progress] Нет данных для оператора {operator_id} за период '{period}'."
                )
                return None

            quality_dict = {}
            conv_dict = {}
            call_handling_dict = {}
            time_dict = {}
            all_dates = set()
            user_name = None
            user_id_fetched = operator_id  # по умолчанию, может быть переопределён из row

            for row in rows:
                dt_obj = row.get("report_date")
                
                # Преобразуем report_date в datetime.date
                if isinstance(dt_obj, str):
                    try:
                        if " - " in dt_obj:  # Обработка диапазона дат
                            start_str, end_str = map(str.strip, dt_obj.split(" - "))
                            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
                            # Выбираем начало диапазона (можно изменить на `end_date` или другое правило)
                            dt_obj = start_date
                        else:  # Преобразование одиночной даты
                            dt_obj = datetime.strptime(dt_obj.strip(), "%Y-%m-%d").date()
                    except ValueError as ve:
                        self.logger.warning(
                            f"Некорректный формат даты или диапазона в поле report_date: '{dt_obj}'. Ошибка: {ve}. Ставим dt_obj=None."
                        )
                        dt_obj = None

                elif isinstance(dt_obj, datetime):  # Если уже datetime, преобразуем к date
                    dt_obj = dt_obj.date()

                elif not isinstance(dt_obj, date):  # Если формат неизвестен, логируем и устанавливаем None
                    self.logger.warning(
                        f"Некорректный тип данных в поле report_date: '{dt_obj}' ({type(dt_obj)}). Ставим dt_obj=None."
                    )
                    dt_obj = None

                # Формируем строку даты или ставим "unknown" при отсутствии валидной даты
                date_str = dt_obj.strftime("%Y-%m-%d") if dt_obj else "unknown"

                # Сохраняем имя (если есть)
                user_id_fetched = row.get("user_id", user_id_fetched)
                user_name = row.get("name") or user_name

                quality_dict.setdefault(date_str, {})
                conv_dict.setdefault(date_str, {})
                call_handling_dict.setdefault(date_str, {})
                time_dict.setdefault(date_str, {})

                # Заполняем группы
                quality_dict[date_str].update({
                    "avg_call_rating": row.get("avg_call_rating"),
                    "avg_lead_call_rating": row.get("avg_lead_call_rating"),
                    "avg_cancel_score": row.get("avg_cancel_score"),
                    "avg_complaint_rating": row.get("complaint_rating"),
                })
                conv_dict[date_str].update({
                    "avg_conversion_rate": row.get("avg_conversion_rate"),
                    "avg_conversion_rate_leads": row.get("avg_conversion_rate_leads"),
                    "booked_services": row.get("booked_services"),
                    "total_calls": row.get("total_calls"),
                })
                call_handling_dict[date_str].update({
                    "missed_calls": row.get("missed_calls"),
                    "total_cancellations": row.get("total_cancellations"),
                    "avg_missed_rate": row.get("avg_missed_rate"),
                })
                time_dict[date_str].update({
                    "avg_conversation_time": row.get("avg_conversation_time"),
                    "avg_navigation_time": row.get("avg_navigation_time"),
                    "avg_service_time": row.get("avg_service_time"),
                    "avg_spam_time": row.get("avg_spam_time"),
                    "total_conversation_time": row.get("total_conversation_time"),
                    "total_spam_time": row.get("total_spam_time"),
                })

                all_dates.add(date_str)

            # summary - общая информация
            summary_dict = {
                "user_id": user_id_fetched,
                "operator_name": user_name,
                "dates_used": sorted(all_dates),
                "total_reports": len(rows),
            }

            progress = {
                "quality": quality_dict,
                "conversion": conv_dict,
                "call_handling": call_handling_dict,
                "time": time_dict,
                "summary": summary_dict
            }

            self.logger.info(
                f"[get_operator_progress] Прогресс оператора {operator_id} за период '{period}' сформирован. "
                f"Всего строк: {len(rows)}."
            )
            return progress

        except Exception as e:
            self.logger.error(
                f"[get_operator_progress] Ошибка при выборке прогресса оператора {operator_id} "
                f"за период '{period}': {e}",
                exc_info=True
            )
            return None