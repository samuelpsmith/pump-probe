"""Common DAQ line and spectrum dataclasses."""

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass(slots=True)
class RawLine:
    """One accepted detector read with optional state labeling."""

    sample_index: np.ndarray
    voltage_main: np.ndarray
    voltage_ref: np.ndarray | None = None
    timestamp_s: float | None = None
    trigger_counter: int | None = None
    state_label: str | None = None
    stage_position_mm: float | None = None
    timing_profile_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class IntegratedSpectrum:
    """Average over many raw lines with optional axis information."""

    signal_main: np.ndarray
    signal_ref: np.ndarray | None = None
    signal_diff: np.ndarray | None = None
    wavelength_nm: np.ndarray | None = None
    wavenumber_cm_inv: np.ndarray | None = None
    n_lines: int = 0
    n_pulse_groups: int = 0
    grouping_size: int = 1
    acquisition_duration_s: float | None = None
    statistics: dict[str, np.ndarray | float | int] = field(default_factory=dict)
