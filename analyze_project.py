import os
import logging
from openai import OpenAI
from dotenv import load_dotenv
# Use a pipeline as a high-level helper
from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
# Загружаем переменные из .env файла
load_dotenv()

# Получаем API ключ OpenAI из .env
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

if not client.api_key:
    raise ValueError("API ключ для OpenAI не найден. Добавьте его в файл .env как OPENAI_API_KEY")

# Настройка логирования
logging.basicConfig(filename='analyzer.log', level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Инициализация модели Hugging Face
model_name = "Salesforce/codegen-350M-multi"  # Или другая подходящая модель
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name)
code_pipeline = pipeline("text-generation", model=model, tokenizer=tokenizer, device=-1)  # Укажите device=0 для GPU

def analyze_code_with_hf(code_chunk):
    """
    Запрос к модели Hugging Face для анализа кода с постобработкой.
    """
    try:
        result = code_pipeline(
            f"""### Python code:
{code_chunk}

### Instructions for AI:
Analyze the provided Python code. Your tasks are:
1. Detect potential issues or inefficiencies in the code logic, structure, or style.
2. Highlight areas where the performance could be improved (e.g., algorithm complexity, redundant computations).
3. Identify any potential security vulnerabilities in the code.
4. Suggest actionable improvements to make the code more Pythonic, robust, and maintainable.
5. Recommend appropriate libraries, design patterns, or best practices where applicable.
6. Explain why each identified issue is problematic and provide clear, specific fixes.

### Analysis and recommendations:
""",
            max_length=2048,
            num_return_sequences=1,
            temperature=0.7,
            top_p=0.9
        )
        generated_text = result[0]['generated_text']
        analysis = generated_text.split("### Analysis and recommendations:")[1].strip()
        
        # Постобработка текста для выделения ключевых проблем
        processed_analysis = "\n".join([f"- {line}" for line in analysis.split('\n') if line.strip()])
        return processed_analysis
    except Exception as e:
        logger.error(f"Ошибка анализа с Hugging Face: {str(e)}")
        return f"Ошибка анализа с Hugging Face: {str(e)}"
    
# Остальной код остается прежним, только используйте новую функцию:
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
        hf_analysis = analyze_code_with_hf(chunk)  # Используем Hugging Face модель
        full_analysis += f"\n--- Часть {idx + 1} ---\n{hf_analysis}\n"

    # Сохраняем результаты анализа в отдельный файл
    analysis_file = f"{file_path}_analysis.txt"
    with open(analysis_file, 'w', encoding='utf-8') as f:
        f.write(full_analysis)
    logger.info(f"Результат анализа для файла {file_path} сохранён в {analysis_file}")

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

def split_code_into_chunks(code, max_token_size=512):
    """
    Разбиваем код на части, чтобы не превышать лимит токенов.
    """
    tokens = tokenizer.tokenize(code)
    chunks = []
    current_chunk = []

    for token in tokens:
        current_chunk.append(token)
        if len(current_chunk) >= max_token_size:
            chunks.append(tokenizer.convert_tokens_to_string(current_chunk))
            current_chunk = []

    if current_chunk:
        chunks.append(tokenizer.convert_tokens_to_string(current_chunk))

    return chunks

def analyze_project(directory):
    """Запуск анализа проекта: анализ указанных файлов."""
    files = scan_project_files(directory)
    if not files:
        print("Файлы для анализа не найдены.")
        logger.info("Файлы для анализа не найдены.")
        return

    report = "Project Analysis Report\n\n"
    logger.info(f"Найдено файлов для анализа: {len(files)}")
    for file in files:
        logger.info(f"Начало анализа файла: {file}")
        analyze_file(file)
        with open(f"{file}_analysis.txt", 'r', encoding='utf-8') as f:
            report += f"\n=== Анализ файла: {file} ===\n"
            report += f.read()

    with open("project_analysis_report.txt", 'w', encoding='utf-8') as report_file:
        report_file.write(report)
    logger.info("Анализ проекта завершён. Отчёт сохранён в project_analysis_report.txt.")

if __name__ == "__main__":
    project_directory = os.getcwd()  # Текущая директория проекта
    print(f"Запуск анализа проекта в директории: {project_directory}")
    logger.info(f"Запуск анализа проекта в директории: {project_directory}")
    analyze_project(project_directory)

def scan_project_files(directory):
    """Сканирование только указанных файлов в проекте."""
    target_files = [
        "bot.py",
        "metrics_calculator.py",
        "openai_telebot.py",
        "operator_data.py"
    ]
    project_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file in target_files:
                file_path = os.path.join(root, file)
                project_files.append(file_path)
    return project_files