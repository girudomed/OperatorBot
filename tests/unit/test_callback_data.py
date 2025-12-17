import pytest
from app.telegram.utils.callback_data import AdminCB

def test_create_simple():
    data = AdminCB.create(AdminCB.DASHBOARD)
    assert data == "adm:dsh"

def test_create_with_args():
    data = AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING, 1)
    assert data == "adm:usr:lst:p:1"

def test_length_limit():
    long_arg = "a" * 60
    with pytest.raises(ValueError, match="exceeds 64 bytes"):
        AdminCB.create(AdminCB.USERS, long_arg)

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
