import json

import pytest

import app.telegram.handlers.manual as manual


def test_load_video_file_id_missing(tmp_path, monkeypatch):
    path = tmp_path / "manual_video.json"
    monkeypatch.setattr(manual, "MANUAL_VIDEO_PATH", path)
    assert manual._load_video_file_id() is None


def test_load_video_file_id_invalid_json_returns_none(tmp_path, monkeypatch):
    path = tmp_path / "manual_video.json"
    path.write_text("not-json", encoding="utf-8")
    monkeypatch.setattr(manual, "MANUAL_VIDEO_PATH", path)
    assert manual._load_video_file_id() is None


def test_save_and_load_video_file_id(tmp_path, monkeypatch):
    path = tmp_path / "manual_video.json"
    monkeypatch.setattr(manual, "MANUAL_VIDEO_PATH", path)
    manual._save_video_file_id("abc123")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["file_id"] == "abc123"
    assert manual._load_video_file_id() == "abc123"
