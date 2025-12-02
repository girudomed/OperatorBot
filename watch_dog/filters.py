# watch_dog/filters.py
import logging
import re
import os
from typing import List, Pattern

from .config import SENSITIVE_KEYS

class SensitiveDataFilter(logging.Filter):
    """
    Фильтр для маскировки чувствительных данных в логах.
    Автоматически скрывает значения переменных окружения, указанных в SENSITIVE_KEYS,
    а также телефоны и email-адреса.
    """
    
    def __init__(self):
        super().__init__()
        self.patterns: List[Pattern] = []
        self._load_sensitive_patterns()
        
        # Регулярки для общих паттернов
        self.phone_pattern = re.compile(r'(?<!\d)(\+?7|8)[\s(-]*\d{3}[\s)-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}(?!\d)')
        # Email regex (простой)
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')

    def _load_sensitive_patterns(self):
        """Загружает значения секретов из env для маскировки"""
        for key in SENSITIVE_KEYS:
            value = os.getenv(key)
            if value and len(value) > 4:  # Маскируем только если длина > 4 символов
                self.patterns.append(re.compile(re.escape(value)))

    def filter(self, record: logging.LogRecord) -> bool:
        """Маскирует данные в сообщении лога"""
        if not isinstance(record.msg, str):
            return True

        message = record.msg
        
        # 1. Маскировка конкретных секретов из ENV
        for pattern in self.patterns:
            message = pattern.sub('***SECRET***', message)
            
        # 2. Маскировка телефонов (оставляем последние 4 цифры)
        def mask_phone(match):
            phone = match.group(0)
            digits = re.sub(r'\D', '', phone)
            if len(digits) >= 10:
                return f"{phone[:2]}...{phone[-4:]}"
            return phone
            
        message = self.phone_pattern.sub(mask_phone, message)
        
        # 3. Маскировка email
        message = self.email_pattern.sub('***EMAIL***', message)

        record.msg = message
        
        # Также обрабатываем аргументы форматирования, если они есть
        if record.args:
            new_args = []
            for arg in record.args:
                if isinstance(arg, str):
                    for pattern in self.patterns:
                        arg = pattern.sub('***SECRET***', arg)
                    arg = self.phone_pattern.sub(mask_phone, arg)
                    arg = self.email_pattern.sub('***EMAIL***', arg)
                new_args.append(arg)
            record.args = tuple(new_args)

        return True
