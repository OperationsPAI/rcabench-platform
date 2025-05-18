from ..logging import logger

from pathlib import Path
from typing import Any
import datetime
import json
import dataclasses


def json_default(obj):
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, Path):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def load_json(*, path: str | Path) -> Any:
    logger.opt(colors=True).debug(f"loading json from <green>{path}</green>")
    with open(path) as f:
        return json.load(f)


def save_json(obj: Any, *, path: str | Path) -> None:
    if hasattr(obj, "__dataclass_fields__"):
        obj = dataclasses.asdict(obj)

    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4, default=json_default)

    logger.opt(colors=True).debug(f"saved json to <green>{file_path}</green>")
