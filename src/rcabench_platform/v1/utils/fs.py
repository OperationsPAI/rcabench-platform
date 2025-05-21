from contextlib import contextmanager
from pathlib import Path
import shutil


@contextmanager
def running_mark(folder: Path):
    running = folder / ".running"

    if running.exists():
        shutil.rmtree(folder)

    folder.mkdir(parents=True, exist_ok=True)
    running.touch()

    try:
        yield
    except Exception:
        raise
    else:
        running.unlink()
