"""Wrapper around the legacy ``DAQ_pda_hard.py`` acquisition stack.

This adapter lets the new package reuse the proven DAQ logic without touching
the existing bench tool.  It is intentionally thin: configuration and line
reads are delegated to the legacy controller, while the rest of the project
interacts with a small, typed surface.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import importlib.util
from pathlib import Path
import time
from typing import Any

import numpy as np

from pump_probe.hardware.daq.acquisition_session import AcquisitionSessionInfo
from pump_probe.hardware.daq.line_models import RawLine


RUNTIME_PROFILE_SAFE = "safe"
RUNTIME_PROFILE_PERSISTENT_ROBUST = "persistent_robust_test"
RUNTIME_PROFILE_PERSISTENT_LATEST = "persistent_latest_test"


@lru_cache(maxsize=1)
def _load_legacy_hard_module():
    module_path = Path(__file__).resolve().parents[3] / "DAQ_pda_hard.py"
    spec = importlib.util.spec_from_file_location("pump_probe_legacy_daq_hard", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load legacy module from {module_path}.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _runtime_profile_defaults(
    runtime_profile: str,
    latest_only_read: bool | None,
    overwrite_unread: bool | None,
) -> tuple[bool, bool]:
    if latest_only_read is not None and overwrite_unread is not None:
        return bool(latest_only_read), bool(overwrite_unread)

    profile = str(runtime_profile).strip().lower()
    if profile == RUNTIME_PROFILE_PERSISTENT_LATEST:
        return (
            True if latest_only_read is None else bool(latest_only_read),
            True if overwrite_unread is None else bool(overwrite_unread),
        )
    return (
        False if latest_only_read is None else bool(latest_only_read),
        False if overwrite_unread is None else bool(overwrite_unread),
    )


def _as_raw_line(
    data: np.ndarray | dict[str, np.ndarray],
    *,
    timing_profile_name: str,
    metadata: dict[str, Any] | None = None,
) -> RawLine:
    metadata = dict(metadata or {})
    timestamp_s = time.time()
    if isinstance(data, dict):
        main = np.asarray(data["main"], dtype=float).copy()
        ref = np.asarray(data["reference"], dtype=float).copy()
    else:
        main = np.asarray(data, dtype=float).copy()
        ref = None
    sample_index = np.arange(main.size, dtype=int)
    return RawLine(
        sample_index=sample_index,
        voltage_main=main,
        voltage_ref=ref,
        timestamp_s=timestamp_s,
        timing_profile_name=timing_profile_name,
        metadata=metadata,
    )


@dataclass(slots=True)
class LegacyPdaSession:
    """Session wrapper for safe or persistent line reads."""

    controller: "LegacyPdaController"
    runtime_profile: str
    expected_trigger_hz: float
    read_reference: bool
    timeout_s: float
    latest_only_read: bool
    overwrite_unread: bool
    ai_buffer_lines: int
    info: AcquisitionSessionInfo = field(init=False)
    _legacy_session: Any = None

    def __post_init__(self) -> None:
        pda = self.controller.legacy
        self.info = AcquisitionSessionInfo(
            runtime_profile=self.runtime_profile,
            timing_profile=pda.timing_profile_name,
            expected_trigger_hz=float(self.expected_trigger_hz),
            read_reference=bool(self.read_reference),
            external_trigger_enabled=bool(pda.use_external_trigger),
            samples_per_line_read=int(pda.ai_samples_per_line),
            samples_per_line_output=int(pda.output_samples_per_line),
            crop_output_to_valid_pixels=bool(pda.crop_output_to_valid_pixels),
        )

        if self.runtime_profile != RUNTIME_PROFILE_SAFE:
            module = self.controller.module
            session = module._RetriggerLineSession(
                pda,
                ai_buffer_lines=int(self.ai_buffer_lines),
                read_reference=bool(self.read_reference),
                latest_only_read=bool(self.latest_only_read),
                overwrite_unread=bool(self.overwrite_unread),
            )
            self._legacy_session = session.__enter__()

    def read_line(self, timeout: float | None = None) -> RawLine:
        timeout_s = self.timeout_s if timeout is None else float(timeout)
        if self.runtime_profile == RUNTIME_PROFILE_SAFE:
            data = self.controller.legacy.acquire_line(
                timeout=timeout_s,
                read_reference=self.read_reference,
            )
            metadata = {"runtime_profile": self.runtime_profile}
            return _as_raw_line(
                data,
                timing_profile_name=self.controller.legacy.timing_profile_name,
                metadata=metadata,
            )

        data = self._legacy_session.read_line(timeout=timeout_s)
        metadata = {
            "runtime_profile": self.runtime_profile,
            "lines_consumed": getattr(self._legacy_session, "last_lines_consumed", 1),
            "latest_only_read": self.latest_only_read,
            "overwrite_unread": self.overwrite_unread,
        }
        return _as_raw_line(
            data,
            timing_profile_name=self.controller.legacy.timing_profile_name,
            metadata=metadata,
        )

    def close(self) -> None:
        if self._legacy_session is not None:
            self._legacy_session.close()
            self._legacy_session = None

    def __enter__(self) -> "LegacyPdaSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


@dataclass(slots=True)
class LegacyChopperSyncSession:
    """Wrapper for the legacy hardware chopper sync output session."""

    controller: "LegacyPdaController"
    _legacy_session: Any = None
    available: bool = False
    error_text: str = ""

    def __post_init__(self) -> None:
        session = self.controller.module._ChopperSyncOutSession(self.controller.legacy)
        self._legacy_session = session.__enter__()
        self.available = bool(getattr(self._legacy_session, "available", False))
        self.error_text = str(getattr(self._legacy_session, "error_text", "") or "")

    def close(self) -> None:
        if self._legacy_session is not None:
            self._legacy_session.close()
            self._legacy_session = None

    def __enter__(self) -> "LegacyChopperSyncSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


@dataclass(slots=True)
class LegacyChopperInputMonitorSession:
    """Wrapper for the legacy hardware chopper-input edge monitor session."""

    controller: "LegacyPdaController"
    counter: str = "ctr3"
    rate_gate_s: float = 0.05
    _legacy_session: Any = None
    available: bool = False
    error_text: str = ""

    def __post_init__(self) -> None:
        session = self.controller.module._ChopperInputEdgeMonitorSession(
            self.controller.legacy,
            counter=self.counter,
            rate_gate_s=self.rate_gate_s,
        )
        self._legacy_session = session.__enter__()
        self.available = bool(getattr(self._legacy_session, "available", False))
        self.error_text = str(getattr(self._legacy_session, "error_text", "") or "")

    def read_rate(self) -> tuple[float, int | None]:
        if self._legacy_session is None:
            return float("nan"), None
        return self._legacy_session.read_rate()

    def close(self) -> None:
        if self._legacy_session is not None:
            self._legacy_session.close()
            self._legacy_session = None

    def __enter__(self) -> "LegacyChopperInputMonitorSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


class LegacyPdaController:
    """Thin reusable wrapper around ``PDAControllerDAQSimple``."""

    def __init__(
        self,
        *,
        device: str = "Dev1",
        num_pixels: int = 1024,
        ai_main: str = "ai2",
        ai_ref: str = "ai0",
        trig_pfi: str = "PFI9",
        st_pfi: str = "PFI8",
        clk_pfi: str = "PFI4",
        chopper_sync_pfi: str = "PFI3",
        chopper_input_pfi: str = "PFI14",
    ) -> None:
        module = _load_legacy_hard_module()
        self.module = module
        self.legacy = module.PDAControllerDAQSimple(
            device=device,
            num_pixels=num_pixels,
            ai_main=ai_main,
            ai_ref=ai_ref,
            trig_pfi=trig_pfi,
            st_pfi=st_pfi,
            clk_pfi=clk_pfi,
            chopper_sync_pfi=chopper_sync_pfi,
            chopper_input_pfi=chopper_input_pfi,
        )

    def apply_timing_profile(self, profile_name: str) -> None:
        self.legacy.apply_timing_profile(profile_name)

    def set_trigger_phase_shift_s(self, value_s: float) -> None:
        self.legacy.set_trigger_phase_shift(trigger_to_st_delay_s=float(value_s))

    def enable_external_trigger(self, enable: bool = True) -> None:
        self.legacy.enable_external_trigger(enable=bool(enable))

    def set_trigger_filter(self, enable: bool, min_pulse_width_s: float) -> None:
        self.legacy.set_trigger_filter(
            enable=bool(enable),
            min_pulse_width_s=float(min_pulse_width_s),
        )

    def set_trigger_sync(self, enable: bool) -> None:
        self.legacy.set_trigger_sync(enable=bool(enable))

    def set_retrigger_initial_delay(self, enable: bool = True) -> None:
        self.legacy.set_retrigger_initial_delay(enable=bool(enable))

    def set_output_cropping(self, enable: bool = True) -> None:
        self.legacy.set_output_cropping(enable=bool(enable))

    def set_video_timing(
        self,
        *,
        dummy_clocks: int = 14,
        pixel_clocks: int | None = None,
        start_on_st_fall: bool = True,
    ) -> None:
        self.legacy.set_video_timing(
            dummy_clocks=int(dummy_clocks),
            pixel_clocks=pixel_clocks,
            start_on_st_fall=bool(start_on_st_fall),
        )

    def set_chopper_sync_output(
        self,
        *,
        enable: bool = False,
        out_pfi: str = "PFI3",
        source_terminal: str | None = None,
        initial_delay_ticks: int = 0,
        high_ticks: int = 2,
        low_ticks: int = 2,
    ) -> None:
        self.legacy.set_chopper_sync_output(
            enable=bool(enable),
            out_pfi=out_pfi,
            source_terminal=source_terminal,
            initial_delay_ticks=int(initial_delay_ticks),
            high_ticks=int(high_ticks),
            low_ticks=int(low_ticks),
        )

    def set_chopper_input_terminal(self, *, in_pfi: str = "PFI14") -> None:
        self.legacy.set_chopper_input_terminal(in_pfi=in_pfi)

    def estimate_line_timing(self, trigger_frequency_hz: float | None = None) -> dict[str, Any]:
        return self.legacy.estimate_line_timing(trigger_frequency_hz=trigger_frequency_hz)

    def get_timing_diagnostics(self, trigger_frequency_hz: float | None = None) -> dict[str, Any]:
        return self.legacy.get_timing_diagnostics(trigger_frequency_hz=trigger_frequency_hz)

    def format_timing_diagnostics(self, trigger_frequency_hz: float | None = None) -> str:
        return self.legacy.format_timing_diagnostics(trigger_frequency_hz=trigger_frequency_hz)

    def open_chopper_sync_output_session(self) -> LegacyChopperSyncSession:
        return LegacyChopperSyncSession(controller=self)

    def open_chopper_input_monitor_session(
        self,
        *,
        counter: str = "ctr3",
        rate_gate_s: float = 0.05,
    ) -> LegacyChopperInputMonitorSession:
        return LegacyChopperInputMonitorSession(
            controller=self,
            counter=counter,
            rate_gate_s=rate_gate_s,
        )

    def acquire_line(
        self,
        timeout: float = 10.0,
        read_reference: bool = False,
    ) -> RawLine:
        data = self.legacy.acquire_line(timeout=float(timeout), read_reference=bool(read_reference))
        return _as_raw_line(
            data,
            timing_profile_name=self.legacy.timing_profile_name,
            metadata={"runtime_profile": RUNTIME_PROFILE_SAFE},
        )

    def open_session(
        self,
        runtime_profile: str,
        *,
        expected_trigger_hz: float,
        read_reference: bool = False,
        ai_buffer_lines: int = 256,
        latest_only_read: bool | None = None,
        overwrite_unread: bool | None = None,
        timeout_s: float = 10.0,
    ) -> LegacyPdaSession:
        latest, overwrite = _runtime_profile_defaults(
            runtime_profile,
            latest_only_read,
            overwrite_unread,
        )
        return LegacyPdaSession(
            controller=self,
            runtime_profile=str(runtime_profile),
            expected_trigger_hz=float(expected_trigger_hz),
            read_reference=bool(read_reference),
            timeout_s=float(timeout_s),
            latest_only_read=latest,
            overwrite_unread=overwrite,
            ai_buffer_lines=int(ai_buffer_lines),
        )
