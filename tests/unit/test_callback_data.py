import pytest
from app.telegram.utils.callback_data import AdminCB

def test_create_simple():
    data = AdminCB.create(AdminCB.DASHBOARD)
    assert data == "adm:dsh"

def test_create_with_args():
    data = AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING, 1)
    assert data == "adm:usr:lst:p:1"

def test_length_limit():
    # Ранее код бросал ValueError при превышении лимита.
    # Теперь возвращается хэш-фиктивный callback в формате adm:hd:<hash> или adm:err в редком случае.
    long_arg = "a" * 60
    data = AdminCB.create(AdminCB.USERS, long_arg)
    assert isinstance(data, str)
    # Допустимые варианты: hashed fallback или err
    assert data.startswith("adm:hd:") or data == "adm:err"
    assert len(data.encode("utf-8")) <= 64

def test_parse_valid():
    data = "adm:usr:lst:p:1"
    action, args = AdminCB.parse(data)
    assert action == AdminCB.USERS
    assert args == ["lst", "p", "1"]

def test_parse_invalid_prefix():
    data = "other:usr:lst"
    action, args = AdminCB.parse(data)
    assert action is None
    assert args == []

def test_match():
    data = "adm:dsh"
    assert AdminCB.match(data, AdminCB.DASHBOARD) is True
    assert AdminCB.match(data, AdminCB.USERS) is False

def test_starts_with():
    data = "adm:usr:lst:p"
    assert AdminCB.starts_with(data, AdminCB.USERS) is True
    assert AdminCB.starts_with(data, AdminCB.DASHBOARD) is False
