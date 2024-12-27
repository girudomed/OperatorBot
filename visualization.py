# visualization.py

import functools
import os
import uuid
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime, date
from collections import defaultdict
import seaborn as sns
import pandas as pd
from typing import Dict, List, Any, Tuple, Optional, Union, NamedTuple
import numpy as np
from scipy import stats
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

###############################################################################
# Интеграция структур и констант из progress_data.py для поддержки метрик
###############################################################################

@dataclass
class MetricGroup:
    """Группа метрик с их описанием."""
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

###############################################################################
# Настройка логгера
###############################################################################

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Настройка логгера с форматированием и обработчиками.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        file_handler = logging.FileHandler('visualization.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger

###############################################################################
# Глобальные конфигурации и классы для визуализации
###############################################################################

@dataclass
class PlotResult:
    """Результат создания графика."""
    file_path: str
    plot_name: str
    metrics_used: List[str]
    missing_metrics: List[str]
    warnings: List[str]
    data: pd.DataFrame
    error_type: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

@dataclass
class GlobalConfig:
    """Глобальные параметры для всех графиков."""
    style: str = "seaborn"
    palette: str = "husl"
    n_colors: int = 8
    figsize: Tuple[int, int] = (10, 6)
    dpi: int = 100

    title_fontsize: int = 14
    axis_label_fontsize: int = 12
    tick_label_fontsize: int = 10
    value_label_fontsize: int = 9

    legend_fontsize: int = 10
    legend_framealpha: float = 0.8
    legend_loc: str = "upper right"
    legend_columns: int = 1
    legend_title_fontsize: int = 11

    confidence_level: float = 0.95
    min_data_points: int = 3

    show_grid: bool = True
    grid_alpha: float = 0.3
    grid_linestyle: str = "--"
    show_legend: bool = True
    value_labels: bool = True
    value_label_fmt: str = "{:.2f}"

    show_trend: bool = True
    show_confidence_interval: bool = True
    trend_line_style: str = "--"
    trend_line_color: str = "gray"
    trend_line_width: float = 1.5
    confidence_interval_alpha: float = 0.2

    x_rotation: int = 0
    y_rotation: int = 0
    x_grid: bool = True
    y_grid: bool = True

    remove_outliers: bool = False
    outlier_threshold: float = 1.5
    normalize_data: bool = False
    normalization_method: str = "zscore"
    missing_metrics_action: str = "warn"
    handle_missing_values: str = "drop"
    min_non_null_ratio: float = 0.8

    def __post_init__(self):
        if not 0 < self.confidence_level < 1:
            raise ValueError("confidence_level должен быть между 0 и 1")
        if self.min_data_points < 2:
            raise ValueError("min_data_points должен быть не менее 2")
        if self.outlier_threshold <= 0:
            raise ValueError("outlier_threshold должен быть положительным")
        if not 0 <= self.legend_framealpha <= 1:
            raise ValueError("legend_framealpha должен быть между 0 и 1")
        if self.legend_fontsize <= 0:
            raise ValueError("legend_fontsize должен быть положительным")
        if self.title_fontsize <= 0:
            raise ValueError("title_fontsize должен быть положительным")
        if self.axis_label_fontsize <= 0:
            raise ValueError("axis_label_fontsize должен быть положительным")

    def apply_style_to_axis(self, ax: plt.Axes, title: str = "") -> None:
        """
        Применение стилей к осям графика.
        """
        if title:
            ax.set_title(title, fontsize=self.title_fontsize)
        ax.tick_params(axis='both', labelsize=self.tick_label_fontsize)
        if self.show_grid:
            ax.grid(visible=self.show_grid, alpha=self.grid_alpha, linestyle=self.grid_linestyle, which='major')
            ax.grid(which='x', visible=self.x_grid)
            ax.grid(which='y', visible=self.y_grid)
        ax.tick_params(axis='x', rotation=self.x_rotation)
        ax.tick_params(axis='y', rotation=self.y_rotation)
        if self.show_legend and len(ax.get_legend_handles_labels()[0]) > 0:
            ax.legend(
                fontsize=self.legend_fontsize,
                framealpha=self.legend_framealpha,
                loc=self.legend_loc,
                ncol=self.legend_columns,
                title_fontsize=self.legend_title_fontsize
            )

@dataclass
class PlotConfig:
    """Конфигурация для конкретного графика."""
    metrics: List[str]
    plot_type: str = "line"
    title: str = ""
    xlabel: str = ""
    ylabel: str = ""
    figsize: Optional[Tuple[int, int]] = None
    show_trend: Optional[bool] = None
    show_confidence_interval: Optional[bool] = None
    value_labels: Optional[bool] = None
    value_label_fmt: Optional[str] = None
    y_min: Optional[float] = None  # Минимальное значение оси Y
    y_max: Optional[float] = None  # Максимальное значение оси Y

    def merge_with_global(self, global_config: GlobalConfig) -> 'PlotConfig':
        """
        Объединение локальной конфигурации с глобальной.
        """
        merged = PlotConfig(
            metrics=self.metrics,
            plot_type=self.plot_type,
            title=self.title,
            xlabel=self.xlabel,
            ylabel=self.ylabel,
            figsize=self.figsize or global_config.figsize,
            show_trend=self.show_trend if self.show_trend is not None else global_config.show_trend,
            show_confidence_interval=(
                self.show_confidence_interval 
                if self.show_confidence_interval is not None 
                else global_config.show_confidence_interval
            ),
            value_labels=self.value_labels if self.value_labels is not None else global_config.value_labels,
            value_label_fmt=self.value_label_fmt or global_config.value_label_fmt,
            y_min=self.y_min if self.y_min is not None else None,
            y_max=self.y_max if self.y_max is not None else None,
        )
        return merged

@dataclass
class MetricsConfig:
    """Конфигурация для группы метрик."""
    plot_config: PlotConfig
    metrics: List[str] = field(default_factory=list)
    aggregation: str = "mean"
    preprocessing: Optional[callable] = None
    metric_name: str = ""

###############################################################################
# Базовые классы для построения графиков
###############################################################################

class BaseMetricsPlot(ABC):
    """Базовый класс для построения графиков метрик."""

    def __init__(self, config: PlotConfig):
        self.config = config
        plt.style.use(config.style)
        self.colors = sns.color_palette(config.palette, config.n_colors)
        self.logger = setup_logger(self.__class__.__name__)
        self.logger.info(f"Инициализация {self.__class__.__name__} с конфигурацией: {config}")

    def adjust_palette(self, num_colors: int) -> None:
        """
        Подбор палитры для количества метрик.
        """
        if num_colors > len(self.colors):
            self.logger.warning("Количество метрик превышает доступные цвета палитры. Расширяем палитру.")
            self.colors = sns.color_palette("tab20", num_colors)

    def _create_figure(self) -> Tuple[plt.Figure, plt.Axes]:
        """
        Создание фигуры для графика.
        """
        self.logger.debug("Создание фигуры для графика")
        try:
            fig, ax = plt.subplots(figsize=self.config.figsize)
            ax.set_title(self.config.title)
            ax.set_xlabel(self.config.xlabel)
            ax.set_ylabel(self.config.ylabel)
            # Настройка лимитов оси Y
            if self.config.y_min is not None or self.config.y_max is not None:
                ax.set_ylim(self.config.y_min, self.config.y_max)

            if self.config.show_grid:
                ax.grid(True, alpha=0.3)
            self.logger.debug("Фигура успешно создана")
            return fig, ax
        except Exception as e:
            self.logger.error(f"Ошибка при создании фигуры: {e}")
            raise

    def _add_trend_line(self, ax: plt.Axes, x_data: np.ndarray, y_data: np.ndarray, color: str, label: str) -> None:
        """
        Добавление линии тренда и доверительного интервала.
        """
        self.logger.debug(f"Добавление линии тренда для {label}")
        try:
            x_numeric = np.arange(len(x_data))
            if len(x_numeric) < 2:
                self.logger.warning(f"Недостаточно данных для построения тренда для {label}")
                return
            slope, intercept, r_value, p_value, std_err = stats.linregress(x_numeric, y_data)
            line = slope * x_numeric + intercept
            ax.plot(
                x_data,
                line,
                "--",
                color=color,
                alpha=0.5,
                label=f"Тренд {label} (R²={r_value**2:.2f})",
            )
            confidence = self.config.show_confidence_interval
            degrees_of_freedom = len(x_numeric) - 2
            t_value = stats.t.ppf((1 + confidence) / 2, degrees_of_freedom)
            mse = np.sum((y_data - line) ** 2) / degrees_of_freedom
            std_error = np.sqrt(mse / len(x_numeric))
            ci = t_value * std_error
            ax.fill_between(x_data, line - ci, line + ci, color=color, alpha=0.1)
            self.logger.debug(f"Линия тренда для {label} успешно добавлена")
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении линии тренда для {label}: {e}")

    def _filter_data(self, data: pd.DataFrame, metrics: List[str], date_column: str = "date", single_metric: Optional[str] = None) -> pd.DataFrame:
        """
        Фильтрация данных по метрикам и дате.
        Не оставляем записи, где значения метрик отсутствуют или равны нулю.
        """
        try:
            metrics_to_check = [single_metric] if single_metric else metrics
            self.logger.debug(f"Фильтрация данных для метрик: {metrics_to_check}")
            df = data.dropna(subset=metrics_to_check + [date_column])
            mask = df[metrics_to_check].gt(0).all(axis=1)
            filtered_data = df[mask]
            if filtered_data.empty:
                self.logger.warning("Все данные были удалены в процессе фильтрации (нет положительных значений).")
            return filtered_data
        except Exception as e:
            self.logger.error(f"Ошибка при фильтрации данных: {e}")
            return pd.DataFrame()

    def _filter_zero_values(self, data: pd.DataFrame, metric: str, date_column: str = "date") -> Tuple[np.ndarray, np.ndarray]:
        """
        Фильтрация нулевых и некорректных значений для заданной метрики.
        """
        filtered_data = self._filter_data(data, metrics=[], date_column=date_column, single_metric=metric)
        return filtered_data[date_column].values, filtered_data[metric].values

    def _add_value_labels(self, ax: plt.Axes, x_data: np.ndarray, y_data: np.ndarray, fmt: str = "{:.2f}", offset: Tuple[float, float] = (0, 10)) -> None:
        """
        Добавление подписей значений на графике.
        """
        self.logger.debug("Добавление подписей значений на график")
        try:
            for x, y in zip(x_data, y_data):
                ax.annotate(
                    fmt.format(y),
                    (x, y),
                    textcoords="offset points",
                    xytext=offset,
                    ha="center",
                    bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="gray", alpha=0.7),
                )
            self.logger.debug("Подписи значений успешно добавлены")
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении подписей значений: {e}")

    def create_twinx(self, ax: plt.Axes) -> plt.Axes:
        """
        Создание вторых осей Y для совместного отображения разных метрик.
        """
        return ax.twinx()

    def plot_generic(self, ax: plt.Axes, data: pd.DataFrame, metrics: Dict[str, str]) -> None:
        """
        Универсальный метод для построения графиков для набора метрик.
        Отображает только те метрики, по которым есть положительные данные.
        Логирует отсутствующие метрики.
        """
        self.adjust_palette(len(metrics))
        missing_metrics = []

        for i, (metric, label) in enumerate(metrics.items()):
            # Проверка наличия метрики в данных
            if metric not in data.columns:
                missing_metrics.append(metric)
                continue

            # Фильтрация данных по метрике
            x_data, y_data = self._filter_zero_values(data, metric)
            if len(x_data) > 0:
                # Построение графика
                ax.plot(
                    x_data,
                    y_data,
                    "o-",
                    label=label,
                    color=self.colors[i % len(self.colors)],
                    markersize=8
                )
                # Добавление линии тренда
                if self.config.show_trend:
                    self._add_trend_line(ax, x_data, y_data, self.colors[i % len(self.colors)], label)
                # Добавление подписей значений
                if self.config.value_labels:
                    self._add_value_labels(ax, x_data, y_data)
            else:
                self.logger.debug(f"Нет положительных данных для метрики '{label}' ({metric})")

        # Логирование отсутствующих метрик
        if missing_metrics:
            self.logger.warning(f"Следующие метрики отсутствуют в DataFrame: {missing_metrics}")

class BasePlot:
    """Базовый класс для методов построения графиков."""
    def plot_metrics(self, ax: plt.Axes, data: pd.DataFrame, metrics: List[str], labels: List[str], colors: List[str]) -> None:
        """
        Построение нескольких метрик на одном графике.
        """
        for i, metric in enumerate(metrics):
            x_data = data['date']
            y_data = data[metric]
            ax.plot(x_data, y_data, "o-", label=labels[i], color=colors[i % len(colors)], markersize=8)
            if hasattr(self, 'config') and self.config.show_trend:
                self._add_trend_line(ax, x_data, y_data, colors[i % len(colors)], labels[i])
            if hasattr(self, 'config') and self.config.value_labels:
                self._add_value_labels(ax, x_data, y_data)

###############################################################################
# Классы для построения конкретных групп метрик
###############################################################################

class QualityMetricsPlot(BaseMetricsPlot, BasePlot):
    """График метрик качества звонков."""

    def format_stats_text(self, data: pd.DataFrame, metrics: Dict[str, str]) -> str:
        """
        Форматирует текстовую статистику для метрик.
        Args:
            data (pd.DataFrame): Данные для анализа.
            metrics (Dict[str, str]): Словарь метрик и их описаний.

        Returns:
            str: Текст со статистикой.
        """
        stats_text = "Статистика:\n"
        for metric, label in metrics.items():
            if metric in data.columns:
                values = data[data[metric] > 0][metric]
                if len(values) > 0:
                    mean_v = values.mean()
                    median_v = values.median()
                    std_v = values.std()
                    stats_text += (
                        f"\n{label}:\n"
                        f"Среднее: {mean_v:.2f}\n"
                        f"Медиана: {median_v:.2f}\n"
                        f"Стд. откл.: {std_v:.2f}\n"
                    )
        return stats_text

    def plot(self, data: pd.DataFrame, date_column: str = "date") -> plt.Figure:
        """
        Построение графика метрик качества звонков.

        Args:
            data (pd.DataFrame): Данные для анализа.
            date_column (str): Название колонки с датами.

        Returns:
            plt.Figure: Построенный график.
        """
        self.logger.info("Построение графика метрик качества звонков")
        fig, ax = self._create_figure()

        # Определяем метрики
        metrics = {
            "avg_call_rating": "Средняя оценка всех разговоров",
            "avg_lead_call_rating": "Средняя оценка лидов",
            "avg_cancel_score": "Средняя оценка отмен",
        }
        # Построение графика с помощью plot_generic
        self.plot_generic(ax, data, metrics)
        # Настройка легенды
        ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
        # Формирование и отображение статистики
        stats_text = self.format_stats_text(data, metrics)
        if stats_text.strip():
            plt.figtext(
                1.15, 0.5,
                stats_text,
                fontsize=8,
                bbox=dict(facecolor="white", alpha=0.8)
            )

        # Финальная настройка
        plt.tight_layout()
        self.logger.info("График метрик качества звонков построен успешно")
        return fig

class ConversionMetricsPlot(BaseMetricsPlot, BasePlot):
    """График метрик конверсии."""
    def plot(self, data: pd.DataFrame, date_column: str = "date") -> plt.Figure:
        self.logger.info("Построение графика метрик конверсии")
        fig, ax1 = self._create_figure()
        if "conversion_rate_leads" in data.columns:
            x_data, y_data = self._filter_zero_values(data, "conversion_rate_leads", date_column)
            if len(x_data) > 0:
                ax1.plot(x_data, y_data, "o-", label="Конверсия в запись (%)", color=self.colors[0 % len(self.colors)], markersize=8)
                if self.config.show_trend:
                    self._add_trend_line(ax1, x_data, y_data, self.colors[0 % len(self.colors)], "конверсии")
                if self.config.value_labels:
                    self._add_value_labels(ax1, x_data, y_data, "{:.1f}%")
        ax1.set_ylabel("Конверсия (%)")
        ax2 = self.create_twinx(ax1)
        metrics_right = {
            "booked_services": "Успешные записи",
            "total_leads": "Всего лидов",
        }
        self.plot_generic(ax2, data, metrics_right)
        ax2.set_ylabel("Количество")
        ax1.legend(loc="upper left")
        ax2.legend(loc="upper right")
        plt.tight_layout()
        self.logger.info("График метрик конверсии построен успешно")
        return fig

class CallHandlingMetricsPlot(BaseMetricsPlot, BasePlot):
    """График метрик обработки звонков."""
    def plot(self, data: pd.DataFrame, date_column: str = "date") -> plt.Figure:
        self.logger.info("Построение графика метрик обработки звонков")
        fig, ax = self._create_figure()
        metrics = {
            "total_calls": "Всего звонков",
            "accepted_calls": "Принятые звонки",
            "missed_calls": "Пропущенные звонки",
        }
        self.plot_generic(ax, data, metrics)
        ax.legend(loc="upper left")
        plt.tight_layout()
        self.logger.info("График метрик обработки звонков построен успешно")
        return fig

class TimeMetricsPlot(BaseMetricsPlot, BasePlot):
    """График временных метрик."""
    def plot(self, data: pd.DataFrame, date_column: str = "date") -> plt.Figure:
        self.logger.info("Построение графика временных метрик")
        fig, ax = self._create_figure()
        metrics = {
            "avg_conversation_time": "Среднее время разговора",
            "avg_navigation_time": "Среднее время навигации",
            "avg_service_time": "Среднее время обслуживания",
        }
        self.plot_generic(ax, data, metrics)
        ax.set_ylabel("Время (секунды)")
        ax.legend(loc="upper left")
        plt.tight_layout()
        self.logger.info("График временных метрик построен успешно")
        return fig

class AllOperatorsProgressPlot(BaseMetricsPlot, BasePlot):
    """График динамики всех операторов."""
    def plot(self, data: pd.DataFrame, config: MetricsConfig, date_column: str = "date") -> plt.Figure:
        self.logger.info("Построение графика динамики всех операторов")
        fig, ax = plt.subplots(figsize=(10, 5))

        operator_dict = defaultdict(list)
        for _, row in data.iterrows():
            op = row["operator_name"]
            d = row["date"]
            val = row["metric_value"]
            operator_dict[op].append((d, val))

        for op in operator_dict:
            operator_dict[op].sort(key=lambda x: x[0])

        cmap = plt.cm.get_cmap("tab10", len(operator_dict))
        for i, (op, values) in enumerate(operator_dict.items()):
            dates = [v[0] for v in values]
            vals = [v[1] for v in values]
            ax.plot(dates, vals, marker="o", linestyle="-", label=op, color=cmap(i))

        ax.set_title(f"Сравнительная динамика по метрике {config.metric_name}")
        ax.set_xlabel("Дата")
        ax.set_ylabel(config.metric_name)
        ax.grid(True)
        ax.legend()

        plt.tight_layout()
        self.logger.info("График динамики всех операторов построен успешно")
        return fig

###############################################################################
# Класс для визуализации всех метрик
###############################################################################

class MetricsVisualizer:
    """Класс для визуализации всех метрик."""
    def __init__(self, output_dir: str, configs: Optional[Dict[str, MetricsConfig]] = None,
                 global_config: Optional[GlobalConfig] = None, max_parallel_plots: int = 2):
        self.logger = setup_logger(self.__class__.__name__)
        self.logger.info("Инициализация MetricsVisualizer")

        self.output_dir = output_dir
        self.max_parallel_plots = max_parallel_plots
        self.global_config = global_config or GlobalConfig()
        self.configs = configs or {}
        for name, config in self.configs.items():
            if isinstance(config, PlotConfig):
                self.configs[name] = config.merge_with_global(self.global_config)

        # Убедимся, что global_config корректно преобразуется в строку
        try:
            global_config_repr = asdict(self.global_config) if hasattr(self.global_config, "__dataclass_fields__") else str(self.global_config)
        except Exception as e:
            global_config_repr = f"Ошибка представления: {e}"

        self.logger.debug(
            "Инициализация завершена с параметрами: "
            f"output_dir={output_dir}, max_parallel_plots={max_parallel_plots}, global_config={global_config_repr}"
        )

    async def create_plots_for_groups(self, data_dict: Dict[str, pd.DataFrame]) -> List[PlotResult]:
        """
        data_dict: словарь {group_name: dataframe} — для каждой группы свои данные
        Возвращает список PlotResult.
        """
        loop = asyncio.get_running_loop()
        tasks = []
        for name, df in data_dict.items():
            tasks.append(
                loop.run_in_executor(
                    self.executor,  # ThreadPoolExecutor(max_workers=self.max_parallel_plots)
                    functools.partial(self.create_plot, name, df)
                )
            )
        return await asyncio.gather(*tasks)

    def create_plot(self, config, data):
        """
        Создает фигуру и оси для графиков.

        Args:
            config (dict): Конфигурация графика (например, figsize).
            data (dict): Данные для построения графика, с ключами 'x' и 'y'.

        Returns:
            tuple: Объект фигуры и осей matplotlib.
        """
        fig, ax = plt.subplots(figsize=config.get("figsize", (10, 6)))
        ax.plot(data['x'], data['y'], label="Sample Data")
        ax.set_title(config.get("title", "График"))
        ax.set_xlabel(config.get("xlabel", "X"))
        ax.set_ylabel(config.get("ylabel", "Y"))
        ax.legend()
        plt.close(fig)  # Очистка фигур, чтобы избежать утечек памяти
        return fig, ax


    def preprocess_data(self, data: pd.DataFrame, metrics: List[str]) -> pd.DataFrame:
        """
        Предобработка данных: удаление выбросов, нормализация, обработка пропусков.
        """
        if data.empty:
            self.logger.warning("Получены пустые данные для предобработки")
            return data

        processed_data = data.copy()
        try:
            # Обработка пропусков
            if self.global_config.handle_missing_values != "ignore":
                for metric in metrics:
                    if metric in processed_data.columns:
                        non_null_ratio = processed_data[metric].notna().mean()

                        # Логируем метрики с большим количеством пропусков
                        if non_null_ratio < self.global_config.min_non_null_ratio:
                            self.logger.warning(
                                f"Метрика '{metric}' содержит слишком много пропущенных значений "
                                f"({(1 - non_null_ratio) * 100:.1f}%)"
                            )

                        # Обработка пропусков в зависимости от конфигурации
                        if self.global_config.handle_missing_values == "drop":
                            processed_data = processed_data.dropna(subset=[metric])
                        elif self.global_config.handle_missing_values == "interpolate":
                            processed_data[metric] = processed_data[metric].interpolate(method='linear')
                        elif self.global_config.handle_missing_values in ["ffill", "bfill"]:
                            processed_data[metric] = processed_data[metric].fillna(method=self.global_config.handle_missing_values)

            # Удаление выбросов
            if self.global_config.remove_outliers:
                for metric in metrics:
                    if metric in processed_data.columns:
                        q1 = processed_data[metric].quantile(0.25)
                        q3 = processed_data[metric].quantile(0.75)
                        iqr = q3 - q1
                        lower_bound = q1 - self.global_config.outlier_threshold * iqr
                        upper_bound = q3 + self.global_config.outlier_threshold * iqr
                        outliers = processed_data[
                            (processed_data[metric] < lower_bound) | (processed_data[metric] > upper_bound)
                        ]

                        # Логируем информацию о выбросах
                        if not outliers.empty:
                            self.logger.info(
                                f"Удалено {len(outliers)} выбросов для метрики '{metric}' "
                                f"(границы: {lower_bound:.2f}, {upper_bound:.2f})"
                            )

                        processed_data = processed_data[
                            (processed_data[metric] >= lower_bound) & (processed_data[metric] <= upper_bound)
                        ]

            # Нормализация
            if self.global_config.normalize_data:
                for metric in metrics:
                    if metric in processed_data.columns:
                        if self.global_config.normalization_method == "zscore":
                            mean = processed_data[metric].mean()
                            std = processed_data[metric].std()
                            if std != 0:
                                processed_data[metric] = (processed_data[metric] - mean) / std
                            else:
                                self.logger.warning(f"Пропуск нормализации для метрики '{metric}': нулевое стандартное отклонение")
                        elif self.global_config.normalization_method == "minmax":
                            min_val = processed_data[metric].min()
                            max_val = processed_data[metric].max()
                            if min_val != max_val:
                                processed_data[metric] = (processed_data[metric] - min_val) / (max_val - min_val)
                            else:
                                self.logger.warning(f"Пропуск нормализации для метрики '{metric}': константные значения")
                        elif self.global_config.normalization_method == "robust":
                            median = processed_data[metric].median()
                            q1 = processed_data[metric].quantile(0.25)
                            q3 = processed_data[metric].quantile(0.75)
                            iqr = q3 - q1
                            if iqr != 0:
                                processed_data[metric] = (processed_data[metric] - median) / iqr
                            else:
                                self.logger.warning(f"Пропуск робастной нормализации для метрики '{metric}': нулевой IQR")

            return processed_data
        except Exception as e:
            self.logger.error(f"Ошибка при предобработке данных: {str(e)}", exc_info=True)
            return data

    def process_data(self, data: pd.DataFrame, metrics: List[str], date_column: str) -> pd.DataFrame:
        """
        Обработка данных с предварительной подготовкой.
        """
        try:
            self.logger.info(f"Начало обработки данных. Размер: {data.shape}")
            if data.empty:
                self.logger.warning("Получены пустые данные")
                return pd.DataFrame()

            processed_data = data.copy()

            # Проверяем наличие колонки с датой
            if date_column not in processed_data.columns:
                self.logger.error(f"Указанная колонка с датой '{date_column}' отсутствует в данных")
                return pd.DataFrame()

            # Сортировка по дате
            processed_data = processed_data.sort_values(date_column)

            # Вызываем метод предобработки данных
            processed_data = self.preprocess_data(processed_data, metrics)

            # Проверяем итоговый размер данных после обработки
            if processed_data.empty:
                self.logger.warning("После обработки данных не осталось записей")
                return pd.DataFrame()

            self.logger.info(f"Обработка данных завершена. Итоговый размер: {processed_data.shape}")
            return processed_data
        except Exception as e:
            self.logger.error(f"Ошибка при обработке данных: {str(e)}", exc_info=True)
            return pd.DataFrame()

###############################################################################
# Функция для расчета трендов (конечная версия)
###############################################################################

def calculate_trends(data: pd.DataFrame, metrics: List[str], date_column: str = "date") -> Dict[str, float]:
    """
    Рассчитывает тренды метрик.
    Возвращает словарь metric -> slope.
    """
    if data.empty:
        raise ValueError("Нет данных для расчета трендов.")

    trends = {}
    for metric in metrics:
        if metric in data.columns:
            if len(data) < 2:
                logging.getLogger(__name__).warning(f"Недостаточно данных для метрики '{metric}' для расчета тренда.")
                continue
            x = np.arange(len(data))
            y = data[metric].fillna(0).values
            slope, _, _, _, _ = stats.linregress(x, y)
            trends[metric] = slope
    return trends

###############################################################################
# Функции для создания графиков для операторов и средних значений (конечная версия)
###############################################################################

async def create_multi_metric_graph(data: list[dict], metrics: list[str], operator_name: str, title: str = None) -> str:
    """
    Строит график динамики нескольких метрик для одного оператора.
    Учтены все актуальные требования:
    - Не показывать нули, если данных нет.
    - Подписывать точки.
    - Отображать только те метрики, которые логически могут сосуществовать.

    Параметры:
        data (List[Dict]): данные, например [{"report_date": ..., "metric1": ..., ...}, ...]
        metrics (List[str]): метрики для отображения.
        operator_name (str): имя оператора.
        title (str, optional): заголовок графика.

    Возвращает:
        str: Путь к PNG-файлу с графиком.
    """
    if not data:
        raise ValueError("Нет данных для построения графика.")

    def parse_date(d):
        if isinstance(d, str):
            if " - " in d:
                d = d.split(" - ")[0].strip()
            return datetime.strptime(d, '%Y-%m-%d')
        elif isinstance(d, datetime):
            return d
        else:
            raise ValueError(f"Неподдерживаемый формат даты: {d}")

    # Формируем подготовленный набор данных
    filtered_data = []
    for row in data:
        try:
            report_date = parse_date(row['report_date'])
            valid_row = {'report_date': report_date}
            # Сохраняем только нужные метрики, игнорируем None и 0.
            for metric in metrics:
                val = row.get(metric)
                # Если данные есть и > 0, сохраняем их
                if isinstance(val, (int, float)) and val > 0:
                    valid_row[metric] = val
                else:
                    # Если данных нет или 0, не записываем метрику
                    # чтобы в дальнейшем не было нулей.
                    pass
            # Если есть хотя бы одна метрика с данными - добавляем
            if any(m in valid_row for m in metrics):
                filtered_data.append(valid_row)
        except Exception as e:
            print(f"Пропущена запись из-за ошибки: {e}")

    # Сортируем по дате
    filtered_data.sort(key=lambda x: x['report_date'])

    if not filtered_data:
        raise ValueError("Нет данных после фильтрации для построения графика.")

    # Преобразуем в DataFrame для удобства
    df = pd.DataFrame(filtered_data)

    # Проверяем, что есть хотя бы одна колонка с данными
    available_metrics = [m for m in metrics if m in df.columns and df[m].notnull().any()]
    if not available_metrics:
        raise ValueError("Нет подходящих метрик с положительными значениями.")

    # Подготовка данных для отрисовки
    dates = df['report_date'].values

    # Создаем график
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.tab10(range(len(available_metrics)))

    for i, m in enumerate(available_metrics):
        y_data = df[m].values
        ax.plot(dates, y_data, marker='o', linestyle='-', label=m, color=colors[i])
        # Подписываем точки
        for x_val, y_val in zip(dates, y_data):
            ax.annotate(
                f"{y_val:.2f}",
                (x_val, y_val),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="gray", alpha=0.7),
            )

    if not title:
        title = f"Динамика метрик для {operator_name} (с {dates[0].strftime('%d.%m.%Y')} по {dates[-1].strftime('%d.%m.%Y')})"
    ax.set_title(title)
    ax.set_xlabel("Дата")
    ax.set_ylabel("Значение метрик")
    ax.grid(True)
    ax.legend()

    filename = f"multi_metric_{uuid.uuid4().hex}.png"
    filepath = os.path.join("/tmp", filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close(fig)

    return filepath

async def create_all_operators_progress_graph(data: List[Dict[str, Any]], metric_name: str) -> str:
    """
    Строит график динамики для нескольких операторов по одной метрике.
    Применяются те же принципы: не отображаем нули и отсутствующие данные, подписываем точки.

    Параметры:
        data (List[Dict]): [{'operator_name': str, 'report_date': str|datetime, 'metric_value': float}, ...]
        metric_name (str): Имя метрики.

    Возвращает:
        str: Путь к PNG-файлу с графиком.
    """
    if not data:
        raise ValueError("Нет данных для построения графика.")

    def parse_date(d):
        if isinstance(d, str):
            if " - " in d:
                d = d.split(" - ")[0].strip()
            return datetime.strptime(d, '%Y-%m-%d')
        elif isinstance(d, datetime):
            return d
        else:
            raise ValueError(f"Неподдерживаемый формат даты: {d}")

    operator_dict = defaultdict(list)
    for row in data:
        op = row['operator_name']
        d = parse_date(row['report_date'])
        val = row['metric_value']
        if isinstance(val, (int, float)) and val > 0:
            operator_dict[op].append((d, val))

    # Убираем операторов без данных
    operator_dict = {op: vals for op, vals in operator_dict.items() if vals}

    if not operator_dict:
        raise ValueError("Нет положительных данных для построения графика.")

    for op in operator_dict:
        operator_dict[op].sort(key=lambda x: x[0])

    fig, ax = plt.subplots(figsize=(10, 5))
    cmap = plt.cm.get_cmap('tab10', len(operator_dict))

    for i, (op, values) in enumerate(operator_dict.items()):
        dates = [v[0] for v in values]
        vals = [v[1] for v in values]
        ax.plot(dates, vals, marker='o', linestyle='-', label=op, color=cmap(i))
        for x_val, y_val in zip(dates, vals):
            ax.annotate(
                f"{y_val:.2f}",
                (x_val, y_val),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="gray", alpha=0.7),
            )

    ax.set_title(f"Сравнительная динамика по метрике {metric_name}")
    ax.set_xlabel("Дата")
    ax.set_ylabel(metric_name)
    ax.grid(True)
    ax.legend()

    filename = f"all_operators_progress_{uuid.uuid4().hex}.png"
    filepath = os.path.join("/tmp", filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close(fig)

    return filepath

async def create_average_line_graph(data: List[Dict[str, Any]], metric_name: str) -> str:
    """
    Строит график со средними значениями метрики по всем операторам.
    Не показывает нули, подписывает точки.

    Параметры:
        data (List[Dict]): [{'date': str|datetime, 'metric_value': float}, ...]
        metric_name (str): Имя метрики.

    Возвращает:
        str: Путь к PNG-файлу с графиком.
    """
    if not data:
        raise ValueError("Нет данных для построения графика.")

    def parse_date(d):
        if isinstance(d, str):
            if " - " in d:
                d = d.split(" - ")[0].strip()
            return datetime.strptime(d, '%Y-%m-%d')
        elif isinstance(d, datetime):
            return d
        else:
            raise ValueError(f"Неподдерживаемый формат даты: {d}")

    date_dict = defaultdict(list)
    for row in data:
        d = parse_date(row['date'])
        val = row['metric_value']
        if isinstance(val, (int, float)) and val > 0:
            date_dict[d].append(val)

    date_dict = {d: vals for d, vals in date_dict.items() if vals}

    if not date_dict:
        raise ValueError("Нет положительных данных для вычисления среднего.")

    avg_values = []
    for d, vals in date_dict.items():
        avg_val = sum(vals) / len(vals)
        avg_values.append((d, avg_val))

    avg_values.sort(key=lambda x: x[0])
    dates = [x[0] for x in avg_values]
    values = [x[1] for x in avg_values]

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(dates, values, marker='o', linestyle='-', color='green')
    for x_val, y_val in zip(dates, values):
        ax.annotate(
            f"{y_val:.2f}",
            (x_val, y_val),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="gray", alpha=0.7),
        )
    ax.set_title(f"Средние значения {metric_name} по всем операторам")
    ax.set_xlabel("Дата")
    ax.set_ylabel(metric_name)
    ax.grid(True)

    filename = f"avg_metric_{uuid.uuid4().hex}.png"
    filepath = os.path.join("/tmp", filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close(fig)

    return filepath