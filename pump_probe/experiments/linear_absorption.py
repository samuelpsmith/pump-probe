"""Linear absorption experiment scaffolding."""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np

from pump_probe.calibration.models import WavelengthCalibration
from pump_probe.calibration.wavelength import calibrated_wavenumber_axis
from pump_probe.experiments.base import CapturedLineStack
from pump_probe.experiments.metadata import ExperimentMetadata
from pump_probe.experiments.steady_state_base import SteadyStateExperimentBase
from pump_probe.hardware.daq import IntegratedSpectrum
from pump_probe.hardware.daq.controller import LineAcquisitionController
from pump_probe.io.dataset_store import ExperimentDatasetStore
from pump_probe.processing.outliers import integrated_area_per_line, mad_inlier_mask
from pump_probe.processing.linear_absorption import (
    absorbance_spectrum_from_transmission,
)
from pump_probe.processing.uncertainty import summarize_grouped_spectra


LINEAR_ABSORPTION_MAIN_ONLY = "main_only"
LINEAR_ABSORPTION_MAIN_REF_NORMALIZED = "main_ref_normalized"


@dataclass(slots=True)
class LinearAbsorptionRunConfig:
    """Acquisition parameters for a linear absorption run."""

    runtime_profile: str = "safe"
    expected_trigger_hz: float = 1000.0
    sample_lines: int = 128
    reference_lines: int = 128
    use_reference_channel: bool = False
    observable_mode: str = LINEAR_ABSORPTION_MAIN_ONLY
    ai_buffer_lines: int = 256
    timeout_s: float = 10.0
    pulse_grouping_size: int = 32
    save_subaverages: bool = True


@dataclass(slots=True)
class LinearAbsorptionResult:
    """Processed output for one linear absorption measurement pair."""

    sample_spectrum: IntegratedSpectrum
    reference_spectrum: IntegratedSpectrum
    transmission: np.ndarray
    absorbance: np.ndarray
    wavelength_nm: np.ndarray | None = None
    wavenumber_cm_inv: np.ndarray | None = None


@dataclass(slots=True)
class LinearAbsorptionRunner(SteadyStateExperimentBase):
    """Runner scaffold for linear absorption experiments."""

    config: LinearAbsorptionRunConfig
    dataset_store: ExperimentDatasetStore
    wavelength_calibration: WavelengthCalibration | None = None

    def capture_reference(self, controller: LineAcquisitionController) -> CapturedLineStack:
        return self.capture_line_stack(
            controller,
            runtime_profile=self.config.runtime_profile,
            expected_trigger_hz=self.config.expected_trigger_hz,
            n_lines=self.config.reference_lines,
            read_reference=self.config.use_reference_channel,
            ai_buffer_lines=self.config.ai_buffer_lines,
            timeout_s=self.config.timeout_s,
        )

    def capture_sample(self, controller: LineAcquisitionController) -> CapturedLineStack:
        return self.capture_line_stack(
            controller,
            runtime_profile=self.config.runtime_profile,
            expected_trigger_hz=self.config.expected_trigger_hz,
            n_lines=self.config.sample_lines,
            read_reference=self.config.use_reference_channel,
            ai_buffer_lines=self.config.ai_buffer_lines,
            timeout_s=self.config.timeout_s,
        )

    def build_result(
        self,
        *,
        sample_stack: CapturedLineStack,
        reference_stack: CapturedLineStack,
    ) -> LinearAbsorptionResult:
        wavelength_nm = None
        wavenumber_cm_inv = None
        if self.wavelength_calibration is not None:
            wavelength_nm = self.wavelength_calibration.wavelength_nm
            wavenumber_cm_inv = calibrated_wavenumber_axis(self.wavelength_calibration)

        sample_spec = sample_stack.integrated_spectrum(
            wavelength_nm=wavelength_nm,
            wavenumber_cm_inv=wavenumber_cm_inv,
        )
        reference_spec = reference_stack.integrated_spectrum(
            wavelength_nm=wavelength_nm,
            wavenumber_cm_inv=wavenumber_cm_inv,
        )

        if self.config.observable_mode == LINEAR_ABSORPTION_MAIN_REF_NORMALIZED:
            if sample_spec.signal_ref is None or reference_spec.signal_ref is None:
                raise ValueError(
                    "main_ref_normalized mode requires reference-channel acquisition."
                )
            sample_norm = sample_spec.signal_main / np.clip(sample_spec.signal_ref, 1e-12, None)
            reference_norm = reference_spec.signal_main / np.clip(
                reference_spec.signal_ref, 1e-12, None
            )
            transmission = sample_norm / np.clip(reference_norm, 1e-12, None)
        else:
            transmission = sample_spec.signal_main / np.clip(
                reference_spec.signal_main,
                1e-12,
                None,
            )

        absorbance = absorbance_spectrum_from_transmission(transmission)
        return LinearAbsorptionResult(
            sample_spectrum=sample_spec,
            reference_spectrum=reference_spec,
            transmission=transmission,
            absorbance=absorbance,
            wavelength_nm=wavelength_nm,
            wavenumber_cm_inv=wavenumber_cm_inv,
        )

    def save_result(self, result: LinearAbsorptionResult) -> None:
        self.dataset_store.save_metadata(self.metadata)
        self.dataset_store.save_npz(
            "linear_absorption_result.npz",
            sample_signal=result.sample_spectrum.signal_main,
            reference_signal=result.reference_spectrum.signal_main,
            transmission=result.transmission,
            absorbance=result.absorbance,
            wavelength_nm=(
                np.asarray(result.wavelength_nm, dtype=float)
                if result.wavelength_nm is not None
                else np.array([], dtype=float)
            ),
            wavenumber_cm_inv=(
                np.asarray(result.wavenumber_cm_inv, dtype=float)
                if result.wavenumber_cm_inv is not None
                else np.array([], dtype=float)
            ),
        )

    def save_captured_stack(self, filename_stem: str, stack: CapturedLineStack) -> None:
        """Persist raw accepted lines plus per-line metadata."""

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

        self.dataset_store.save_npz(
            f"{filename_stem}.npz",
            **arrays,
        )
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


def default_linear_absorption_metadata(sample_name: str) -> ExperimentMetadata:
    """Convenience metadata builder for first-pass runner setup."""

    from datetime import datetime, timezone

    return ExperimentMetadata(
        experiment_type="linear_absorption",
        sample_name=sample_name,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
