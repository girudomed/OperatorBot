# visualization.py
import os
import uuid
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib
from collections import defaultdict

# Если при запуске будет ошибка, что matplotlib не может работать без дисплея,
# установите backend без интерфейса, например:
# import matplotlib
# matplotlib.use('Agg')

async def create_multi_metric_graph(data: list[dict], metrics: list[str], operator_name: str, title: str = None) -> str:
        """
        Строит график динамики нескольких метрик для одного оператора.

        Параметры:
            data (List[Dict]): Список словарей с данными, например:
                [{"report_date": ..., "metric1": ..., "metric2": ...}, ...]
            metrics (List[str]): Список имен метрик, которые необходимо отобразить.
            operator_name (str): Имя оператора, для отображения в заголовке.
            title (str, optional): Пользовательский заголовок графика.

        Возвращает:
            str: Путь к PNG-файлу с сохраненным графиком.
        """
        if not data:
            raise ValueError("Нет данных для построения графика.")
        # Преобразуем даты и фильтруем данные
        def parse_date(d):
            if isinstance(d, str):
                if " - " in d:
                    d = d.split(" - ")[0].strip()  # Берём первую дату
                return datetime.strptime(d, '%Y-%m-%d')
            elif isinstance(d, datetime):
                return d
            else:
                raise ValueError(f"Неподдерживаемый формат даты: {d}")

        filtered_data = []
        for row in data:
            try:
                report_date = parse_date(row['report_date'])
                valid_row = {'report_date': report_date}
                for m in metrics:
                    valid_row[m] = row.get(m, 0)  # Если значение отсутствует, подставляем 0
                filtered_data.append(valid_row)
            except Exception as e:
                print(f"Пропущена запись из-за ошибки: {e}")

        # Сортируем данные по дате
        filtered_data.sort(key=lambda x: x['report_date'])

        # Если после фильтрации данных нет
        if not filtered_data:
            raise ValueError("После фильтрации данных для построения графика не осталось.")

        # Составляем списки значений для каждой метрики
        dates = [row['report_date'] for row in filtered_data]
        metric_values = {m: [row[m] for row in filtered_data] for m in metrics}

        # Инициализация графика
        fig, ax = plt.subplots(figsize=(12, 6))
        colors = plt.cm.tab10(range(len(metrics)))

        for i, m in enumerate(metrics):
            ax.plot(dates, metric_values[m], marker='o', linestyle='-', label=m, color=colors[i])

        # Формирование заголовка
        if not title:
            title = f"Динамика метрик для {operator_name}" + \
                    f" (с {dates[0].strftime('%d.%m.%Y')} по {dates[-1].strftime('%d.%m.%Y')})"
        ax.set_title(title)

        # Подписываем оси
        ax.set_xlabel("Дата")
        ax.set_ylabel("Значение метрик")
        ax.legend()
        ax.grid(True)

        # Сохраняем график
        filename = f"multi_metric_{uuid.uuid4().hex}.png"
        filepath = os.path.join("/tmp", filename)
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close(fig)

        return filepath

def calculate_trends(data, metrics):
    """
    Рассчитывает тренды метрик.

    Параметры:
        data (List[Dict]): данные с отсортированными датами.
        metrics (List[str]): метрики для анализа.

    Возвращает:
        Dict[str, str]: тренды в формате "метрика: тренд".
    """
    trends = {}
    for m in metrics:
        try:
            values = [row[m] for row in data if row.get(m) is not None]
            if len(values) >= 2:
                diff = values[-1] - values[0]
                trend = "выросла" if diff > 0 else "упала" if diff < 0 else "осталась на месте"
                trends[m] = f"{m}: {trend} (начальное {values[0]}, конечное {values[-1]})"
            else:
                trends[m] = f"{m}: недостаточно данных для анализа."
        except KeyError:
            trends[m] = f"{m}: метрика отсутствует в данных."
    return trends

async def create_all_operators_progress_graph(data, metric_name: str) -> str:
        """
        Строит график динамики сразу для нескольких операторов на одном поле.

        Параметры:
            data (List[Dict]): список словарей c ключами ['operator_name', 'date', 'metric_value'].
                - 'operator_name': str
                - 'report_date': datetime или str в формате YYYY-MM-DD
                - 'metric_value': float или int
            metric_name (str): Название метрики для оси Y.

        Возвращает:
            str: Путь к PNG-файлу с графиком.
        """
        if not data:
            raise ValueError("Нет данных для построения графика.")

        operator_dict = defaultdict(list)
        for row in data:
            op = row['operator_name']
            d = row['report_date']
            if isinstance(d, str):
                if " - " in d:  # Если это диапазон, берем первую дату
                    d = d.split(" - ")[0].strip()
                d = datetime.strptime(d, '%Y-%m-%d')
            val = row['metric_value']
            operator_dict[op].append((d, val))

        # Сортируем по дате для каждого оператора
        for op in operator_dict:
            operator_dict[op].sort(key=lambda x: x[0])

        fig, ax = plt.subplots(figsize=(10, 5))

        # Используем табличную цветовую палитру
        cmap = plt.cm.get_cmap('tab10', len(operator_dict))
        for i, (op, values) in enumerate(operator_dict.items()):
            dates = [v[0] for v in values]
            vals = [v[1] for v in values]
            ax.plot(dates, vals, marker='o', linestyle='-', label=op, color=cmap(i))

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


async def create_average_line_graph(data, metric_name: str) -> str:
        """
        Строит график со средними значениями метрики по всем операторам с течением времени.

        Параметры:
            data (List[Dict]): [{'date': datetime или str, 'metric_value': float}, ...]
            metric_name (str): Название метрики.

        Возвращает:
            str: Путь к PNG-файлу с графиком.
        """
        if not data:
            raise ValueError("Нет данных для построения графика.")

        date_dict = defaultdict(list)
        for row in data:
            d = row['date']
            if isinstance(d, str):
                if " - " in d:  # Если это диапазон, берем первую дату
                    d = d.split(" - ")[0].strip()
                d = datetime.strptime(d, '%Y-%m-%d')
            val = row['metric_value']
            date_dict[d].append(val)

        # Вычисляем среднее значение по датам
        avg_values = []
        for d, vals in date_dict.items():
            if vals:
                avg_val = sum(vals) / len(vals)
                avg_values.append((d, avg_val))

        if not avg_values:
            raise ValueError("Нет данных для вычисления среднего.")

        avg_values.sort(key=lambda x: x[0])
        dates = [x[0] for x in avg_values]
        values = [x[1] for x in avg_values]

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(dates, values, marker='o', linestyle='-', color='green')
        ax.set_title(f"Средние значения {metric_name} по всем операторам")
        ax.set_xlabel("Дата")
        ax.set_ylabel(metric_name)
        ax.grid(True)

        filename = f"avg_metric_{uuid.uuid4().hex}.png"
        filepath = os.path.join("/tmp", filename)
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close(fig)

        return filepath