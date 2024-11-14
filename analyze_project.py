import os
import logging
from openai import OpenAI
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Получаем API ключ OpenAI из .env
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

if not client.api_key:
    raise ValueError("API ключ для OpenAI не найден. Добавьте его в файл .env как OPENAI_API_KEY")

# Настройка логирования
logging.basicConfig(filename='analyzer.log', level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Конфигурации для OpenAI
MAX_TOKENS_PER_REQUEST = 8000  # Ограничение токенов на запрос
MODEL = "gpt-4o-mini"
TEMPERATURE = 0.7

def scan_project_files(directory):
    """Сканирование всех файлов в проекте и возврат их путей."""
    project_files = []
    for root, dirs, files in os.walk(directory):
        # Исключаем системные директории или виртуальные среды
        dirs[:] = [d for d in dirs if d not in ['.git', 'venv', '__pycache__']]
        for file in files:
            if file.endswith('.py'):  # анализируем только Python файлы
                file_path = os.path.join(root, file)
                project_files.append(file_path)
    return project_files

def split_code_into_chunks(code, max_chunk_size=1500):
    """
    Разбиваем код на части, чтобы избежать превышения лимитов по токенам.
    :param code: Полный код файла.
    :param max_chunk_size: Максимальный размер одного куска текста.
    :return: Список кусочков кода для анализа.
    """
    lines = code.split('\n')
    chunks = []
    current_chunk = []

    for line in lines:
        current_chunk.append(line)
        if len(current_chunk) >= max_chunk_size:
            chunks.append("\n".join(current_chunk))
            current_chunk = []

    if current_chunk:
        chunks.append("\n".join(current_chunk))

    return chunks

def analyze_code_with_gpt(code_chunk):
    """Запрос к OpenAI GPT для анализа кода в рамках одной части."""
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "Act as a no-nonsense, hard-nosed senior Python developer. Your task is to thoroughly audit and criticize a Python project for any inefficiencies, bad practices, or potential bugs. Your responses should be sharp, direct, and focused on finding the weakest spots in the code, design, and structure of the project. Break down complex issues, explain why they are problematic, and provide actionable, practical fixes. Don’t hold back – be blunt and to the point.  Here’s what you need to do: Code Review: Go through the code meticulously and highlight areas where the logic is unclear, the performance is suboptimal, or the design could be improved. Identify Bottlenecks: Look for any potential performance bottlenecks, inefficient loops, redundant code, or operations that could cause slowdowns. Highlight Security Risks: Pinpoint security vulnerabilities, especially in areas that handle user input, data validation, or external API calls. Offer Fixes: For each problem you find, suggest specific, actionable ways to improve the code, performance, and security. Best Practices: Suggest Python best practices, coding standards, and any libraries or design patterns that should be implemented to make the project cleaner and more maintainable. Be relentless in your assessment and prioritize making the codebase as efficient and robust as possible. Example Interaction: User: Here's part of the code that handles file uploads. What do you think? ChatGPT (as hard-nosed dev): This file upload handler a mess. First, you're not validating the file type properly, which is a security risk. Anyone could upload malicious files. Second, you're reading the entire file into memory – a huge problem large files. Use streaming instead to handle uploads more efficiently. Lastly, you're repeating code that could be refactored into a helper function. Also i ask you my friend to make a list of files and examples which code i have to rewrite in which right form to avoid any erros and bugs from your analysis"},
                {"role": "user", "content": code_chunk}
            ],
            max_tokens=MAX_TOKENS_PER_REQUEST,
            temperature=TEMPERATURE
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Ошибка анализа с GPT: {str(e)}")
        return f"Ошибка анализа с GPT: {str(e)}"

def analyze_file(file_path):
    """Анализирует один файл, разбивая его на части и анализируя каждую часть."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            code = file.read()
    except Exception as e:
        logger.error(f"Не удалось прочитать файл {file_path}: {e}")
        return

    if not code.strip():
        logger.info(f"Файл {file_path} пуст.")
        return

    code_chunks = split_code_into_chunks(code)
    logger.info(f"Разбивка файла {file_path} на {len(code_chunks)} частей для анализа.")

    full_analysis = ""
    for idx, chunk in enumerate(code_chunks):
        logger.info(f"Анализ части {idx + 1} из {len(code_chunks)} для файла {file_path}.")
        gpt_analysis = analyze_code_with_gpt(chunk)
        full_analysis += f"\n--- Часть {idx + 1} ---\n{gpt_analysis}\n"

    logger.info(f"Результат анализа для файла {file_path}:\n{full_analysis}")

def analyze_project(directory):
    """Запуск анализа проекта: анализ всех файлов с разбиением на части."""
    files = scan_project_files(directory)
    if not files:
        print("Файлы для анализа не найдены.")
        logger.info("Файлы для анализа не найдены.")
        return

    logger.info(f"Найдено файлов для анализа: {len(files)}")
    for file in files:
        logger.info(f"Начало анализа файла: {file}")
        analyze_file(file)

if __name__ == "__main__":
    project_directory = os.getcwd()  # Текущая директория проекта
    print(f"Запуск анализа проекта в директории: {project_directory}")
    logger.info(f"Запуск анализа проекта в директории: {project_directory}")
    analyze_project(project_directory)
