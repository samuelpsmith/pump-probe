"""Helpers for stage-related calibration metadata."""

from __future__ import annotations

from pump_probe.calibration.delay import build_delay_calibration
from pump_probe.processing.units import ps_per_mm_from_path_multiplier


def four_pass_retroreflector_delay_calibration(
    *,
    zero_position_mm: float,
    path_multiplier: float = 4.0,
    calibration_id: str = "delay_4pass",
    operator: str | None = None,
    notes: str | None = None,
):
    """Build a delay calibration for a four-pass retroreflector setup.

    The default assumes an optical path-length multiplier of 4x mechanical
    displacement. If the bench geometry differs, callers should override
    ``path_multiplier`` or build a fully explicit calibration instead.
    """

    return build_delay_calibration(
        zero_position_mm=zero_position_mm,
        ps_per_mm=ps_per_mm_from_path_multiplier(path_multiplier),
        geometry_label="four-pass retroreflector",
        passes=4,
        calibration_id=calibration_id,
        operator=operator,
        notes=notes,
    )
