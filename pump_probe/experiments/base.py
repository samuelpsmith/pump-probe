"""Base experiment abstractions."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any

import numpy as np

from pump_probe.experiments.metadata import ExperimentMetadata
from pump_probe.hardware.daq import IntegratedSpectrum
from pump_probe.hardware.daq.controller import LineAcquisitionController


@dataclass(slots=True)
class CapturedLineStack:
    """Raw accepted lines captured for one logical acquisition block."""

    sample_index: np.ndarray
    main_lines: np.ndarray
    ref_lines: np.ndarray | None = None
    timestamps_s: np.ndarray | None = None
    timing_profile_name: str | None = None
    line_metadata: list[dict[str, Any]] = field(default_factory=list)

    @property
    def n_lines(self) -> int:
        return int(self.main_lines.shape[0])

    def integrated_spectrum(
        self,
        *,
        wavelength_nm: np.ndarray | None = None,
        wavenumber_cm_inv: np.ndarray | None = None,
        acquisition_duration_s: float | None = None,
    ) -> IntegratedSpectrum:
        """Return mean spectra plus simple uncertainty summaries."""

        signal_main = np.mean(self.main_lines, axis=0)
        stats: dict[str, np.ndarray | float | int] = {
            "main_std": np.std(self.main_lines, axis=0, ddof=1)
            if self.n_lines > 1
            else np.zeros_like(signal_main),
            "main_sem": np.std(self.main_lines, axis=0, ddof=1) / np.sqrt(self.n_lines)
            if self.n_lines > 1
            else np.zeros_like(signal_main),
        }
        signal_ref = None
        signal_diff = None
        if self.ref_lines is not None:
            signal_ref = np.mean(self.ref_lines, axis=0)
            stats["ref_std"] = (
                np.std(self.ref_lines, axis=0, ddof=1)
                if self.n_lines > 1
                else np.zeros_like(signal_ref)
            )
            stats["ref_sem"] = (
                np.std(self.ref_lines, axis=0, ddof=1) / np.sqrt(self.n_lines)
                if self.n_lines > 1
                else np.zeros_like(signal_ref)
            )
            signal_diff = signal_main - signal_ref

        return IntegratedSpectrum(
            signal_main=signal_main,
            signal_ref=signal_ref,
            signal_diff=signal_diff,
            wavelength_nm=wavelength_nm,
            wavenumber_cm_inv=wavenumber_cm_inv,
            n_lines=self.n_lines,
            acquisition_duration_s=acquisition_duration_s,
            statistics=stats,
        )


@dataclass(slots=True)
class ExperimentBase:
    """Shared envelope for experiment runners."""

    metadata: ExperimentMetadata

    def capture_line_stack(
        self,
        controller: LineAcquisitionController,
        *,
        runtime_profile: str,
        expected_trigger_hz: float,
        n_lines: int,
        read_reference: bool = False,
        ai_buffer_lines: int = 256,
        timeout_s: float = 10.0,
    ) -> CapturedLineStack:
        """Capture a block of accepted lines while preserving per-line data."""

        main_lines: list[np.ndarray] = []
        ref_lines: list[np.ndarray] = []
        timestamps: list[float] = []
        line_metadata: list[dict[str, Any]] = []
        t0 = time.perf_counter()

        with controller.open_session(
            runtime_profile,
            expected_trigger_hz=expected_trigger_hz,
            read_reference=read_reference,
            ai_buffer_lines=ai_buffer_lines,
            timeout_s=timeout_s,
        ) as session:
            timing_profile_name = session.info.timing_profile
            for _ in range(int(n_lines)):
                line = session.read_line(timeout=timeout_s)
                main_lines.append(line.voltage_main.copy())
                if read_reference and line.voltage_ref is not None:
                    ref_lines.append(line.voltage_ref.copy())
                timestamps.append(float(line.timestamp_s or time.time()))
                line_metadata.append(dict(line.metadata))

        elapsed = time.perf_counter() - t0
        sample_index = np.arange(main_lines[0].size, dtype=int)
        ref_stack = np.stack(ref_lines, axis=0) if ref_lines else None
        stack = CapturedLineStack(
            sample_index=sample_index,
            main_lines=np.stack(main_lines, axis=0),
            ref_lines=ref_stack,
            timestamps_s=np.asarray(timestamps, dtype=float),
            timing_profile_name=timing_profile_name,
            line_metadata=line_metadata,
        )
        if stack.line_metadata:
            stack.line_metadata[0]["capture_duration_s"] = elapsed
        return stack
