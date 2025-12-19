# Обработка ошибок — краткие правила и примеры

Цель: централизовать поведение при ошибках, чтобы
- ожидаемые ошибки обрабатывались fallback'ом;
- непредвиденные ошибки логировались через logger.exception и пробрасывались дальше;
- не использовать "except Exception: pass".

Короткие правила
- Перехватывать конкретные исключения для ожидаемых случаев:
  - парсинг: json.JSONDecodeError, ValueError, TypeError
  - доступ к ключам: KeyError или использовать dict.get(...)
  - чтение файлов: FileNotFoundError, OSError
  - внешние API: requests.exceptions.RequestException / httpx.HTTPError / asyncio.TimeoutError / OpenAIError
- Для ожидаемых ошибок — возвращать понятный fallback (None, строка с сообщением, пустой список) и логировать на уровне warning/info.
- Для непредвиденных ошибок — logger.exception(...) с контекстом и повторный raise.
- Не использовать "except Exception: pass" или молча глотать ошибки.
- Для асинхронных хендлеров использовать декораторы (log_async_exceptions) чтобы логировать и уведомлять пользователя.

Примеры
- JSON парсинг:
  try:
      data = json.loads(raw)
  except (json.JSONDecodeError, TypeError, ValueError) as exc:
      logger.warning("Invalid JSON payload: %s", exc)
      return None

- Вызов внешнего API:
  try:
      resp = await client.call(...)
      resp.raise_for_status()
  except (httpx.HTTPError, asyncio.TimeoutError) as exc:
      logger.warning("External API failed: %s", exc)
      return fallback
  except Exception:
      logger.exception("Unexpected error calling external API")
      raise

- Доступ к ключам:
  val = payload.get("key")
  if val is None:
      logger.info("Missing 'key' in payload")
      return default

Изменённые файлы (кратко)
- app/services/openai_service.py — явная обработка Timeout/OpenAIError; непредвиденные ошибки логируем и пробрасываем.
- app/telegram/handlers/call_lookup.py — заменены broad except на конкретное поведение: ожидаемые ValueError -> fallback/уведомление пользователя; непредвиденные Exception -> logger.exception + raise; корректное освобождение busy-статуса.
- tests/unit/test_exception_handling.py — добавлены минимальные тесты:
  - тест поведения resolve_hash_async при ошибке доступа к Redis (возвращает None);
  - тест retry/fallback у OpenAIService при OpenAIError.

Как тестировать
- Локально: запустить `pytest tests/unit/test_exception_handling.py -q`
- Общие тесты: `pytest -q`

Контакт для on-call
- При непредвиденной ошибке ищите trace_id в логах (trace_id добавляется глобально).
