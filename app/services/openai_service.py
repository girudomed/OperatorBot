# Файл: app/services/openai_service.py

"""
Сервис для взаимодействия с OpenAI API.
"""

import asyncio
from asyncio import Semaphore
from typing import List

from openai import AsyncOpenAI, OpenAIError

from app.config import OPENAI_API_KEY, OPENAI_COMPLETION_OPTIONS, OPENAI_MODEL
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class OpenAIService:
    def __init__(self, model: str | None = None):
        if not OPENAI_API_KEY:
            logger.error("OpenAI API ключ не найден.")
            raise EnvironmentError("OpenAI API ключ не найден.")
        
        self.client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.model = model or OPENAI_MODEL
        self.semaphore = Semaphore(5)
        
        # Используем параметры из конфига, если они нужны, но здесь пока просто модель
        self.completion_options = OPENAI_COMPLETION_OPTIONS

    async def generate_recommendations(
        self, 
        prompt: str, 
        max_tokens: int = 1500,
        max_retries: int = 3
    ) -> str:
        """
        Генерация ответа от OpenAI на основе промпта.
        """
        for attempt in range(max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=self.completion_options.get("temperature", 0.7),
                )
                if not response.choices or not response.choices[0].message:
                    raise ValueError("Пустой ответ от OpenAI")
                
                content = response.choices[0].message.content
                return content.strip() if content else ""

            except OpenAIError as e:
                logger.warning(
                    "OpenAIError (попытка %s/%s): %s",
                    attempt + 1,
                    max_retries,
                    e,
                )
                await asyncio.sleep(2 ** attempt)
            except (asyncio.TimeoutError,) as exc:
                # Временные сетевые/таймаут ошибки — повторяем попытку с экспоненциальной задержкой.
                logger.warning(
                    "OpenAI timeout (попытка %s/%s): %s",
                    attempt + 1,
                    max_retries,
                    exc,
                )
                await asyncio.sleep(2 ** attempt)
            except Exception as exc:
                # Непредвиденные ошибки логируем и пробрасываем выше — не скрываем баги.
                logger.exception(
                    "Unexpected error while calling OpenAI (attempt %s/%s): %s",
                    attempt + 1,
                    max_retries,
                    exc,
                )
                raise
        
        return "Ошибка: Не удалось получить ответ от OpenAI."

    async def process_batched_requests(
        self, 
        prompts: List[str], 
        max_tokens: int = 1500
    ) -> str:
        """
        Параллельная обработка списка промптов.
        """
        tasks = [
            self.generate_recommendations(prompt, max_tokens=max_tokens)
            for prompt in prompts
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_results = []
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Ошибка в батче: {res}")
            elif isinstance(res, str) and res.startswith("Ошибка:"):
                logger.error(f"Ошибка генерации: {res}")
            elif isinstance(res, str):
                valid_results.append(res)
                
        if not valid_results:
            return "Ошибка: Все запросы завершились неудачей."
            
        return "\n".join(valid_results)

    def split_text(self, text: str, max_length: int) -> List[str]:
        """
        Разбивает текст на части по длине.
        """
        return [text[i:i+max_length] for i in range(0, len(text), max_length)]
