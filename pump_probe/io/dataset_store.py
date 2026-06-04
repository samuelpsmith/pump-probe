"""Minimal dataset-store abstraction for upcoming experiment runners."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from pump_probe.io.serialization import dump_json


@dataclass(slots=True)
class ExperimentDatasetStore:
    """Helper for writing experiment outputs into a single run directory."""

    root: Path

    def save_metadata(self, metadata: object, filename: str = "metadata.json") -> Path:
        path = self.root / filename
        dump_json(path, metadata)
        return path

    def save_json(self, filename: str, payload: Any) -> Path:
        path = self.root / filename
        dump_json(path, payload)
        return path

    def save_npz(self, filename: str, **arrays: np.ndarray) -> Path:
        path = self.root / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        np.savez(path, **arrays)
        return path
