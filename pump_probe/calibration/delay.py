"""Delay calibration helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from pump_probe.calibration.models import DelayCalibration


def build_delay_calibration(
    *,
    zero_position_mm: float,
    ps_per_mm: float,
    geometry_label: str = "custom",
    passes: int | None = None,
    calibration_id: str = "delay_default",
    operator: str | None = None,
    notes: str | None = None,
) -> DelayCalibration:
    """Create a delay calibration record from explicit geometry values."""

    return DelayCalibration(
        calibration_id=calibration_id,
        kind="delay",
        created_at=datetime.now(timezone.utc).isoformat(),
        operator=operator,
        notes=notes,
        zero_position_mm=float(zero_position_mm),
        ps_per_mm=float(ps_per_mm),
        geometry_label=geometry_label,
        passes=passes,
    )


def delay_ps_from_stage_position(
    position_mm: np.ndarray | float,
    calibration: DelayCalibration,
) -> np.ndarray:
    """Convert stage position in mm to optical delay in ps."""

    pos = np.asarray(position_mm, dtype=float)
    return (pos - float(calibration.zero_position_mm)) * float(calibration.ps_per_mm)


def stage_position_mm_from_delay_ps(
    delay_ps: np.ndarray | float,
    calibration: DelayCalibration,
) -> np.ndarray:
    """Convert desired delay in ps back to stage position in mm."""

    delay = np.asarray(delay_ps, dtype=float)
    return float(calibration.zero_position_mm) + delay / float(calibration.ps_per_mm)
