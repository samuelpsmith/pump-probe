"""Abstract DAQ controller interfaces.

Concrete implementations can wrap the current legacy ``DAQ_pda_hard.py`` tool
or, later, a cleaner extracted hardware core.  The rest of the project should
depend on these small interfaces rather than reaching into a monolithic script.
"""

from __future__ import annotations

from typing import Protocol, TYPE_CHECKING

from pump_probe.hardware.daq.line_models import RawLine

if TYPE_CHECKING:
    from pump_probe.hardware.daq.acquisition_session import LineAcquisitionSession


class LineAcquisitionController(Protocol):
    """Protocol for DAQ backends that can return detector lines."""

    def acquire_line(
        self,
        timeout: float = 10.0,
        read_reference: bool = False,
    ) -> RawLine:
        """Acquire a single detector line."""

    def open_session(
        self,
        runtime_profile: str,
        *,
        expected_trigger_hz: float,
        read_reference: bool = False,
        ai_buffer_lines: int = 256,
        latest_only_read: bool | None = None,
        overwrite_unread: bool | None = None,
    ) -> "LineAcquisitionSession":
        """Open a reusable acquisition session for repeated reads."""
