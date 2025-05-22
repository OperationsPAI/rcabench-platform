from contextlib import contextmanager
from pathlib import Path
import shutil


@contextmanager
def running_mark(folder: Path, *, clear: bool = False):
    running = folder / ".running"

    if clear or running.exists():
        shutil.rmtree(folder, ignore_errors=True)

    folder.mkdir(parents=True, exist_ok=True)
    running.touch()

    try:
        yield
    except Exception:
        raise
    else:
        running.unlink()
