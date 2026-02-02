
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

log_dir = os.getenv("LOG_DIR", "/var/log/operabot")
print(f"Effective LOG_DIR: {log_dir}")

log_path = Path(log_dir)
print(f"Log dir exists: {log_path.exists()}")
if log_path.exists():
    print(f"Log dir permissions: {oct(log_path.stat().st_mode)}")
    try:
        test_file = log_path / "test_write.tmp"
        test_file.write_text("test")
        print("Successfully wrote to log dir")
        test_file.unlink()
    except Exception as e:
        print(f"Failed to write to log dir: {e}")
else:
    print("Log dir does not exist")

from watch_dog.config import LOG_DIR as WD_LOG_DIR
print(f"WatchDog LOG_DIR: {WD_LOG_DIR}")
