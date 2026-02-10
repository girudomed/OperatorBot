from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_no_string_based_polling_classification():
    source = _read("app/main.py")
    assert "timeout\" in str(" not in source
    assert "_TRANSIENT_POLLING_MARKERS" not in source


def test_openai_service_does_not_return_error_string_contract():
    source = _read("app/services/openai_service.py")
    assert 'return "Ошибка:' not in source


def test_db_manager_wraps_db_errors_to_app_error():
    source = _read("app/db/manager.py")
    assert "DatabaseIntegrationError" in source
    assert "raise DatabaseIntegrationError" in source


def test_error_handlers_no_business_string_classification():
    source = _read("app/utils/error_handlers.py")
    assert "_classify_business_error" not in source
