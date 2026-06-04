"""Detector baseline calibration helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from pump_probe.calibration.models import DetectorBaselineCalibration


def build_detector_baseline_calibration(
    baseline_signal: np.ndarray,
    *,
    calibration_id: str = "detector_baseline",
    operator: str | None = None,
    notes: str | None = None,
    axis_label: str = "sample_index",
) -> DetectorBaselineCalibration:
    return DetectorBaselineCalibration(
        calibration_id=calibration_id,
        kind="detector_baseline",
        created_at=datetime.now(timezone.utc).isoformat(),
        operator=operator,
        notes=notes,
        baseline_signal=np.asarray(baseline_signal, dtype=float).copy(),
        axis_label=axis_label,
    )


def load_detector_baseline_calibration(path: str | Path) -> DetectorBaselineCalibration:
    payload = np.load(Path(path), allow_pickle=False)
    if "baseline_signal" in payload:
        baseline = payload["baseline_signal"]
    elif "demod_integrated" in payload:
        baseline = payload["demod_integrated"]
    elif "delta_od" in payload:
        baseline = payload["delta_od"]
    else:
        raise ValueError(
            "Could not find a baseline array in "
            f"{path}. Expected 'baseline_signal', 'demod_integrated', or 'delta_od'."
        )
    calibration_id = Path(path).stem
    return build_detector_baseline_calibration(
        baseline,
        calibration_id=calibration_id,
        notes=f"loaded from {Path(path)}",
    )
