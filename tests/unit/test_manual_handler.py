from pathlib import Path

import app.telegram.handlers.manual as manual


def test_load_video_file_id_invalid_json_returns_none(tmp_path, monkeypatch):
    bad_file = tmp_path / "manual_video.json"
    bad_file.write_text("{bad-json}", encoding="utf-8")
    monkeypatch.setattr(manual, "MANUAL_VIDEO_PATH", Path(bad_file))

    assert manual._load_video_file_id() is None
