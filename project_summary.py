import os
import openai
import logging
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Получаем API ключ OpenAI из .env
openai.api_key = os.getenv("OPENAI_API_KEY")

if not openai.api_key:
    raise ValueError("API ключ для OpenAI не найден. Добавьте его в файл .env как OPENAI_API_KEY")

# Настройка логирования
logging.basicConfig(filename='project_summary.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def scan_project_files(directory):
    """Обход всех файлов в проекте и вывод их путей"""
    project_files = []
    for root, dirs, files in os.walk(directory):
        # Исключаем системные директории или виртуальные среды
        dirs[:] = [d for d in dirs if d not in ['.git', 'venv', '__pycache__']]
        for file in files:
            if file.endswith('.py'):  # анализируем только Python-файлы
                file_path = os.path.join(root, file)
                project_files.append(file_path)
    return project_files

def generate_project_description(files):
    """Создание описания проекта с помощью ChatGPT"""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",  # Используйте GPT-4
            messages=[
                {"role": "system", "content": "Вы – обучающий модуль для анализа программного проекта. Ваша задача – проанализировать все файлы Python в проекте и создать подробное описание о проекте, его назначении, сильных и слабых сторонах, реализации и взаимодействии с базой данных. Вы должны объяснить все моменты так, чтобы было понятно новичку. Убедитесь, что отчет охватывает следующие аспекты:\n\n1. Назначение проекта: для чего он создан и какие основные цели он преследует.\n2. Общая структура проекта: что в проекте реализовано и как это организовано.\n3. Реализация: как выполнены основные функции, какие модули и файлы за что отвечают.\n4. Взаимодействие с базой данных: как проект работает с базой данных, какие операции выполняются.\n5. Сильные и слабые стороны: что в проекте сделано хорошо, а что требует улучшения.\n6. Примеры кода: объясните ключевые части кода для понимания новичка.\n\nИспользуйте детализированные объяснения, простые примеры и старайтесь максимально ясно донести информацию, как если бы вы объясняли её школьнику или начинающему разработчику очень подробно, при этом создавайте полностью структурно свой отчет."},
                {"role": "user", "content": "\n".join([open(file, 'r').read() for file in files])}
            ],
            max_tokens=8000,
            temperature=0.7
        )
        return response.choices[0].message['content'].strip()
    except Exception as e:
        return f"Ошибка анализа с GPT: {str(e)}"

def analyze_project(directory):
    """Генерация отчета по всем файлам и созданию карты проекта"""
    files = scan_project_files(directory)
    if not files:
        print("Файлы для анализа не найдены.")
        logging.info("Файлы для анализа не найдены.")
        return
    
    logging.info(f"Найдено файлов для анализа: {len(files)}")
    project_description = generate_project_description(files)  # GPT описание проекта
    logging.info(f"Описание проекта:\n{project_description}")
    print("Описание проекта сохранено в файл 'project_summary.log'")

if __name__ == "__main__":
    # Определяем текущую директорию проекта
    project_directory = os.getcwd()
    
    # Запускаем анализ проекта
    print(f"Запуск анализа проекта в директории: {project_directory}")
    logging.info(f"Запуск анализа проекта в директории: {project_directory}")
    analyze_project(project_directory)
