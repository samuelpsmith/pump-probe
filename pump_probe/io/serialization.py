"""JSON-friendly serialization helpers for dataclasses."""

from dataclasses import asdict, is_dataclass
import json
from pathlib import Path
from typing import Any

import numpy as np


def to_jsonable(obj: Any) -> Any:
    """Convert common scientific Python objects into JSON-safe structures."""
    if is_dataclass(obj):
        return to_jsonable(asdict(obj))
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj


def dump_json(path: Path, obj: Any) -> None:
    """Write JSON with a stable, human-readable layout."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(to_jsonable(obj), fh, indent=2, sort_keys=True)
