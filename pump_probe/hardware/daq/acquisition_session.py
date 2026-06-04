"""Shared DAQ acquisition session primitives."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pump_probe.hardware.daq.line_models import RawLine


@dataclass(slots=True)
class AcquisitionSessionInfo:
    """Minimal session diagnostics for tools and experiments."""

    runtime_profile: str
    timing_profile: str
    expected_trigger_hz: float
    read_reference: bool
    external_trigger_enabled: bool
    samples_per_line_read: int
    samples_per_line_output: int
    crop_output_to_valid_pixels: bool


class LineAcquisitionSession(Protocol):
    """Protocol for repeated ordered reads from a DAQ backend."""

    info: AcquisitionSessionInfo

    def read_line(self, timeout: float | None = None) -> RawLine:
        """Read the next accepted detector line."""

    def close(self) -> None:
        """Release any hardware or background resources."""

    def __enter__(self) -> "LineAcquisitionSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False
