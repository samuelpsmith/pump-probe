"""Steady-state circular dichroism experiment scaffolding."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

from pump_probe.calibration.models import WavelengthCalibration
from pump_probe.calibration.wavelength import calibrated_wavenumber_axis
from pump_probe.experiments.base import CapturedLineStack
from pump_probe.experiments.metadata import ExperimentMetadata
from pump_probe.experiments.steady_state_base import SteadyStateExperimentBase
from pump_probe.hardware.daq.controller import LineAcquisitionController
from pump_probe.io.dataset_store import ExperimentDatasetStore
from pump_probe.processing.outliers import integrated_area_per_line, mad_inlier_mask
from pump_probe.processing.steady_state_cd import (
    circular_difference,
    normalized_circular_difference,
    odd_even_circular_components,
)
from pump_probe.processing.uncertainty import summarize_grouped_spectra


@dataclass(slots=True)
class SteadyStateCDRunConfig:
    """Acquisition parameters for a steady-state CD run."""

    runtime_profile: str = "persistent_robust_test"
    expected_trigger_hz: float = 1000.0
    lines_per_point: int = 256
    use_reference_channel: bool = False
    modulation_sign: float = 1.0
    ai_buffer_lines: int = 256
    timeout_s: float = 10.0
    pulse_grouping_size: int = 32
    save_subaverages: bool = True


@dataclass(slots=True)
class SteadyStateCDResult:
    """Processed output for one steady-state CD acquisition."""

    left_signal: np.ndarray
    right_signal: np.ndarray
    circular_diff: np.ndarray
    normalized_diff: np.ndarray
    wavelength_nm: np.ndarray | None = None
    wavenumber_cm_inv: np.ndarray | None = None


@dataclass(slots=True)
class SteadyStateCDRunner(SteadyStateExperimentBase):
    """Runner scaffold for odd/even PEM-style steady-state CD."""

    config: SteadyStateCDRunConfig
    dataset_store: ExperimentDatasetStore
    wavelength_calibration: WavelengthCalibration | None = None

    def capture_modulation_stack(
        self,
        controller: LineAcquisitionController,
    ) -> CapturedLineStack:
        return self.capture_line_stack(
            controller,
            runtime_profile=self.config.runtime_profile,
            expected_trigger_hz=self.config.expected_trigger_hz,
            n_lines=self.config.lines_per_point,
            read_reference=self.config.use_reference_channel,
            ai_buffer_lines=self.config.ai_buffer_lines,
            timeout_s=self.config.timeout_s,
        )

    def build_result(self, stack: CapturedLineStack) -> SteadyStateCDResult:
        wavelength_nm = None
        wavenumber_cm_inv = None
        if self.wavelength_calibration is not None:
            wavelength_nm = self.wavelength_calibration.wavelength_nm
            wavenumber_cm_inv = calibrated_wavenumber_axis(self.wavelength_calibration)

        working_main = np.asarray(stack.main_lines, dtype=float)
        if self.config.use_reference_channel:
            if stack.ref_lines is None:
                raise ValueError("Reference-enabled CD requires reference-channel lines.")
            working_main = working_main / np.clip(stack.ref_lines, 1e-12, None)

        left_signal, right_signal = odd_even_circular_components(
            working_main,
            sign=self.config.modulation_sign,
        )
        return SteadyStateCDResult(
            left_signal=left_signal,
            right_signal=right_signal,
            circular_diff=circular_difference(left_signal, right_signal),
            normalized_diff=normalized_circular_difference(left_signal, right_signal),
            wavelength_nm=wavelength_nm,
            wavenumber_cm_inv=wavenumber_cm_inv,
        )

    def save_captured_stack(self, filename_stem: str, stack: CapturedLineStack) -> None:
        arrays: dict[str, np.ndarray] = {
            "sample_index": stack.sample_index,
            "main_lines": stack.main_lines,
            "ref_lines": (
                stack.ref_lines
                if stack.ref_lines is not None
                else np.array([], dtype=float)
            ),
            "timestamps_s": (
                stack.timestamps_s
                if stack.timestamps_s is not None
                else np.array([], dtype=float)
            ),
            "main_line_area": integrated_area_per_line(stack.main_lines),
            "main_line_inlier_mask": mad_inlier_mask(
                integrated_area_per_line(stack.main_lines)
            ).astype(int),
        }
        if stack.ref_lines is not None:
            ref_area = integrated_area_per_line(stack.ref_lines)
            arrays["ref_line_area"] = ref_area
            arrays["ref_line_inlier_mask"] = mad_inlier_mask(ref_area).astype(int)

        if self.config.save_subaverages and self.config.pulse_grouping_size > 1:
            main_grouped = summarize_grouped_spectra(
                stack.main_lines,
                self.config.pulse_grouping_size,
            )
            arrays["main_group_means"] = main_grouped.group_means
            arrays["main_group_counts"] = main_grouped.group_counts
            if stack.ref_lines is not None:
                ref_grouped = summarize_grouped_spectra(
                    stack.ref_lines,
                    self.config.pulse_grouping_size,
                )
                arrays["ref_group_means"] = ref_grouped.group_means
                arrays["ref_group_counts"] = ref_grouped.group_counts

        self.dataset_store.save_npz(f"{filename_stem}.npz", **arrays)
        self.dataset_store.save_json(
            f"{filename_stem}_metadata.json",
            {
                "timing_profile_name": stack.timing_profile_name,
                "n_lines": stack.n_lines,
                "line_metadata": stack.line_metadata,
                "pulse_grouping_size": self.config.pulse_grouping_size,
                "save_subaverages": self.config.save_subaverages,
            },
        )

    def save_result(self, result: SteadyStateCDResult) -> None:
        self.dataset_store.save_metadata(self.metadata)
        self.dataset_store.save_npz(
            "steady_state_cd_result.npz",
            left_signal=result.left_signal,
            right_signal=result.right_signal,
            circular_diff=result.circular_diff,
            normalized_diff=result.normalized_diff,
            wavelength_nm=(
                result.wavelength_nm
                if result.wavelength_nm is not None
                else np.array([], dtype=float)
            ),
            wavenumber_cm_inv=(
                result.wavenumber_cm_inv
                if result.wavenumber_cm_inv is not None
                else np.array([], dtype=float)
            ),
        )


def default_steady_state_cd_metadata(sample_name: str) -> ExperimentMetadata:
    return ExperimentMetadata(
        experiment_type="steady_state_cd",
        sample_name=sample_name,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
