# Файл: app/services/openai_service.py

"""
Сервис для взаимодействия с OpenAI API.
"""

import asyncio
from asyncio import Semaphore
from typing import List

import httpx
from openai import AsyncOpenAI, OpenAIError

from app.config import OPENAI_API_KEY, OPENAI_COMPLETION_OPTIONS, OPENAI_MODEL
from app.error_policy import get_retry_config, is_retryable
from app.errors import OpenAIIntegrationError
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
                if content:
                    preview = content[:800].replace("\n", " ")
                    logger.info(
                        "OpenAI response received (chars=%s, preview='%s')",
                        len(content),
                        preview,
                    )
                else:
                    logger.info("OpenAI response received with empty content")
                return content.strip() if content else ""

            except ValueError:
                raise
            except (OpenAIError, asyncio.TimeoutError, httpx.HTTPError) as exc:
                wrapped = OpenAIIntegrationError(
                    "OpenAI request failed",
                    user_message="Сервис рекомендаций временно недоступен.",
                    retryable=True,
                    details={
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "error_type": type(exc).__name__,
                    },
                )
                logger.warning(
                    "OpenAI integration error (attempt %s/%s): %s",
                    attempt + 1,
                    max_retries,
                    exc,
                )
                retry_cfg = get_retry_config(wrapped)
                if (not is_retryable(wrapped)) or attempt >= min(
                    max_retries, retry_cfg.max_retries
                ) - 1:
                    raise wrapped from exc
                delay = retry_cfg.base_delay
                if retry_cfg.exponential_backoff:
                    delay = min(
                        retry_cfg.max_delay,
                        retry_cfg.base_delay * (2 ** attempt),
                    )
                await asyncio.sleep(delay)
        raise OpenAIIntegrationError(
            "OpenAI request retry budget exhausted",
            user_message="Сервис рекомендаций временно недоступен.",
            retryable=False,
        )

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
        failure_count = 0
        for res in results:
            if isinstance(res, Exception):
                failure_count += 1
                logger.warning("Ошибка в батче: %s", res)
            elif isinstance(res, str):
                valid_results.append(res)
                
        if not valid_results:
            raise OpenAIIntegrationError(
                "All batched OpenAI requests failed",
                user_message="Сервис рекомендаций временно недоступен.",
                retryable=False,
                details={"failed_requests": failure_count, "total_requests": len(prompts)},
            )
            
        return "\n".join(valid_results)

    def split_text(self, text: str, max_length: int) -> List[str]:
        """
        Разбивает текст на части по длине.
        """
        return [text[i:i+max_length] for i in range(0, len(text), max_length)]
