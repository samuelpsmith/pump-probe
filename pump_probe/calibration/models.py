"""Calibration dataclasses."""

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass(slots=True)
class CalibrationRecord:
    """Base envelope shared by all calibration objects."""

    calibration_id: str
    kind: str
    created_at: str
    source_run_ids: list[str] = field(default_factory=list)
    operator: str | None = None
    notes: str | None = None
    software_version: str | None = None
    git_commit: str | None = None
    compatibility_tags: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class WavelengthCalibration(CalibrationRecord):
    """Maps detector pixels to wavelength/wavenumber axes."""

    pixel_index: np.ndarray = field(default_factory=lambda: np.array([], dtype=float))
    wavelength_nm: np.ndarray = field(default_factory=lambda: np.array([], dtype=float))
    polynomial_coefficients: np.ndarray | None = None


@dataclass(slots=True)
class DelayCalibration(CalibrationRecord):
    """Maps stage position to optical delay."""

    zero_position_mm: float = 0.0
    ps_per_mm: float = 0.0
    geometry_label: str | None = None
    passes: int | None = None


@dataclass(slots=True)
class DetectorBaselineCalibration(CalibrationRecord):
    """Stores dark or differential offsets for detector-derived signals."""

    baseline_signal: np.ndarray = field(default_factory=lambda: np.array([], dtype=float))
    axis_label: str = "sample_index"


@dataclass(slots=True)
class LinearAbsorptionReference(CalibrationRecord):
    """Stores sample-out reference data for steady-state absorption workflows."""

    reference_signal: np.ndarray = field(default_factory=lambda: np.array([], dtype=float))
    wavelength_nm: np.ndarray | None = None


@dataclass(slots=True)
class PolarizationCalibration(CalibrationRecord):
    """Stores polarization-state calibration information."""

    state_labels: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
