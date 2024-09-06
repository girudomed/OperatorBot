import logging
import os

def setup_logging(log_file="logs.log"):
    """
    Настраивает логирование для проекта.
    Логи записываются как в файл, так и выводятся в консоль.

    :param log_file: Имя файла для сохранения логов.
    :return: Конфигурированный объект логгера.
    """
    # Проверяем, существует ли папка для логов, и создаем её при необходимости
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        level=logging.INFO,  # Уровень логирования
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Формат логов
        handlers=[
            logging.FileHandler(log_file),  # Логирование в файл
            logging.StreamHandler()  # Логирование в консоль
        ]
    )

    # Получаем глобальный логгер
    logger = logging.getLogger(__name__)
    
    # Логируем информацию о запуске
    logger.info(f"Логирование настроено. Логи сохраняются в файл: {log_file}")

    return logger

# Пример использования
if __name__ == "__main__":
    # Инициализация логгера
    logger = setup_logging()

    # Пример логирования
    logger.info("Пример информационного сообщения.")
    logger.warning("Пример предупреждения.")
    logger.error("Пример ошибки.")
