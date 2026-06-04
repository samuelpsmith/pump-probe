"""Transient absorption experiment scaffolding."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import warnings

import numpy as np

from pump_probe.calibration.delay import delay_ps_from_stage_position
from pump_probe.calibration.models import DelayCalibration, DetectorBaselineCalibration, WavelengthCalibration
from pump_probe.calibration.wavelength import calibrated_wavenumber_axis
from pump_probe.experiments.base import CapturedLineStack
from pump_probe.experiments.metadata import ExperimentMetadata
from pump_probe.experiments.scan_plan import ScanAxis, ScanPlan
from pump_probe.experiments.time_resolved_base import TimeResolvedExperimentBase
from pump_probe.hardware.daq.controller import LineAcquisitionController
from pump_probe.io.dataset_store import ExperimentDatasetStore
from pump_probe.processing.outliers import integrated_area_per_line, mad_inlier_mask
from pump_probe.processing.chop_demod import (
    ChannelDarkCorrection,
    ChopDemodConfig,
    DEMOD_MODE_TAIL_GUIDED_PAIRS,
    demodulate_chop_stack,
    target_lines_for_pairs,
)
from pump_probe.processing.uncertainty import summarize_grouped_spectra


def _nanmean_no_warning(values: np.ndarray, axis: int = 0) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanmean(values, axis=axis)


def _nanstd_no_warning(values: np.ndarray, axis: int = 0, ddof: int = 0) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanstd(values, axis=axis, ddof=ddof)


@dataclass(slots=True)
class TransientAbsorptionRunConfig:
    """Acquisition parameters for one transient absorption run."""

    runtime_profile: str = "persistent_robust_test"
    expected_trigger_hz: float = 1000.0
    lines_per_delay: int = 256
    pairs_per_delay: int | None = None
    scans: int = 1
    use_reference_channel: bool = False
    demod_mode: str = DEMOD_MODE_TAIL_GUIDED_PAIRS
    pump_chop_sign: float = -1.0
    tail_sign_start: int = 900
    tail_sign_stop: int = 1000
    tail_sign_expected: float = 1.0
    tail_baseline_subtract: bool = True
    min_light_voltage: float = 0.025
    divide_floor: float = 1e-12
    ai_buffer_lines: int = 256
    timeout_s: float = 10.0
    pulse_grouping_size: int = 32
    save_subaverages: bool = True
    save_dark_offset: bool = False


@dataclass(slots=True)
class TransientDelayPointResult:
    """Processed result for one delay point."""

    requested_delay_position_mm: float
    actual_delay_position_mm: float
    delay_ps: float
    pumped_signal: np.ndarray
    unpumped_signal: np.ndarray
    delta_t_over_t: np.ndarray
    delta_od: np.ndarray
    accepted_lines: int
    accepted_pairs: int
    applied_dark_offset_label: str | None = None
    applied_intensity_dark_label: str | None = None
    demod_mode: str = DEMOD_MODE_TAIL_GUIDED_PAIRS
    tail_flip_count: int = 0
    tail_mean_median: float | None = None
    invalid_pixel_count: int = 0
    invalid_pixel_fraction: float = 0.0
    invalid_pixel_mask: np.ndarray = field(default_factory=lambda: np.array([], dtype=bool))
    demod_metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class TransientAbsorptionScanResult:
    """Collection of delay-point results for one scan or merged scan set."""

    delay_position_mm: np.ndarray
    delay_ps: np.ndarray
    delta_od: np.ndarray
    delta_t_over_t: np.ndarray
    wavelength_nm: np.ndarray | None = None
    wavenumber_cm_inv: np.ndarray | None = None


@dataclass(slots=True)
class TransientAbsorptionMergedResult:
    """Merged summary across multiple TAS scans."""

    delay_position_mm: np.ndarray
    delay_ps: np.ndarray
    delta_od_mean: np.ndarray
    delta_od_std: np.ndarray
    delta_od_sem: np.ndarray
    delta_t_over_t_mean: np.ndarray
    delta_t_over_t_std: np.ndarray
    delta_t_over_t_sem: np.ndarray
    delta_od_scans: np.ndarray
    delta_t_over_t_scans: np.ndarray
    wavelength_nm: np.ndarray | None = None
    wavenumber_cm_inv: np.ndarray | None = None
    n_scans: int = 0


@dataclass(slots=True)
class TransientAbsorptionRunner(TimeResolvedExperimentBase):
    """Runner scaffold for odd/even transient absorption experiments."""

    config: TransientAbsorptionRunConfig
    dataset_store: ExperimentDatasetStore
    delay_calibration: DelayCalibration
    wavelength_calibration: WavelengthCalibration | None = None
    dark_offset_calibration: DetectorBaselineCalibration | None = None
    intensity_dark_correction: ChannelDarkCorrection | None = None

    def effective_lines_per_delay(self) -> int:
        if self.config.pairs_per_delay is not None:
            return target_lines_for_pairs(
                self.config.demod_mode,
                int(self.config.pairs_per_delay),
            )
        return int(self.config.lines_per_delay)

    def target_pairs_per_delay(self) -> int:
        if self.config.pairs_per_delay is not None:
            return int(self.config.pairs_per_delay)
        return max(1, int(self.config.lines_per_delay) // 2)

    def demod_config(self, extra_metadata: dict[str, object] | None = None) -> ChopDemodConfig:
        metadata: dict[str, object] = {
            "expected_trigger_hz": float(self.config.expected_trigger_hz),
            "pairs_per_delay": (
                None
                if self.config.pairs_per_delay is None
                else int(self.config.pairs_per_delay)
            ),
            "lines_per_delay": int(self.config.lines_per_delay),
        }
        if extra_metadata:
            metadata.update(extra_metadata)
        return ChopDemodConfig(
            mode=self.config.demod_mode,
            pump_chop_sign=float(self.config.pump_chop_sign),
            use_reference_channel=bool(self.config.use_reference_channel),
            tail_start=int(self.config.tail_sign_start),
            tail_stop=int(self.config.tail_sign_stop),
            tail_expected_sign=float(self.config.tail_sign_expected),
            tail_baseline_subtract=bool(self.config.tail_baseline_subtract),
            min_light_voltage=float(self.config.min_light_voltage),
            divide_floor=float(self.config.divide_floor),
            dark_correction=self.intensity_dark_correction,
            metadata=metadata,
        )

    def capture_delay_point(
        self,
        controller: LineAcquisitionController,
        *,
        n_lines: int | None = None,
    ) -> CapturedLineStack:
        return self.capture_line_stack(
            controller,
            runtime_profile=self.config.runtime_profile,
            expected_trigger_hz=self.config.expected_trigger_hz,
            n_lines=(self.effective_lines_per_delay() if n_lines is None else int(n_lines)),
            read_reference=self.config.use_reference_channel,
            ai_buffer_lines=self.config.ai_buffer_lines,
            timeout_s=self.config.timeout_s,
        )

    def build_delay_point_result(
        self,
        *,
        requested_delay_position_mm: float,
        stack: CapturedLineStack,
        actual_delay_position_mm: float | None = None,
    ) -> TransientDelayPointResult:
        demod = demodulate_chop_stack(
            stack.main_lines,
            stack.ref_lines,
            config=self.demod_config(
                {
                    "timing_profile_name": stack.timing_profile_name,
                    "line_metadata_count": len(stack.line_metadata),
                }
            ),
        )
        pumped_signal = demod.pumped_signal
        unpumped_signal = demod.unpumped_signal
        delta_t_over_t = demod.delta_t_over_t
        delta_od = demod.delta_od
        applied_label = None
        if self.dark_offset_calibration is not None:
            baseline = np.asarray(
                self.dark_offset_calibration.baseline_signal,
                dtype=float,
            )
            if baseline.shape != delta_od.shape:
                raise ValueError(
                    "Dark offset calibration shape does not match TAS spectral shape: "
                    f"{baseline.shape} vs {delta_od.shape}"
                )
            delta_od = delta_od - baseline
            applied_label = self.dark_offset_calibration.calibration_id

        actual_position_mm = (
            float(requested_delay_position_mm)
            if actual_delay_position_mm is None
            else float(actual_delay_position_mm)
        )
        delay_ps = float(
            delay_ps_from_stage_position(actual_position_mm, self.delay_calibration)
        )
        tail_mean_median = (
            float(np.nanmedian(demod.tail_means))
            if demod.tail_means.size
            else None
        )
        invalid_pixel_count = int(np.sum(demod.invalid_pixel_mask))
        invalid_pixel_fraction = (
            float(invalid_pixel_count) / float(demod.invalid_pixel_mask.size)
            if demod.invalid_pixel_mask.size
            else 0.0
        )
        intensity_label = (
            None
            if self.intensity_dark_correction is None
            else (
                self.intensity_dark_correction.label
                or self.intensity_dark_correction.source_path
            )
        )
        return TransientDelayPointResult(
            requested_delay_position_mm=float(requested_delay_position_mm),
            actual_delay_position_mm=actual_position_mm,
            delay_ps=delay_ps,
            pumped_signal=pumped_signal,
            unpumped_signal=unpumped_signal,
            delta_t_over_t=delta_t_over_t,
            delta_od=delta_od,
            accepted_lines=int(demod.accepted_lines),
            accepted_pairs=int(demod.accepted_pairs),
            applied_dark_offset_label=applied_label,
            applied_intensity_dark_label=intensity_label,
            demod_mode=demod.mode,
            tail_flip_count=int(demod.tail_flip_count),
            tail_mean_median=tail_mean_median,
            invalid_pixel_count=invalid_pixel_count,
            invalid_pixel_fraction=invalid_pixel_fraction,
            invalid_pixel_mask=demod.invalid_pixel_mask.astype(bool),
            demod_metadata=demod.metadata,
        )

    def build_scan_result(
        self,
        delay_points: list[TransientDelayPointResult],
    ) -> TransientAbsorptionScanResult:
        wavelength_nm = None
        wavenumber_cm_inv = None
        if self.wavelength_calibration is not None:
            wavelength_nm = self.wavelength_calibration.wavelength_nm
            wavenumber_cm_inv = calibrated_wavenumber_axis(self.wavelength_calibration)

        return TransientAbsorptionScanResult(
            delay_position_mm=np.asarray(
                [point.actual_delay_position_mm for point in delay_points], dtype=float
            ),
            delay_ps=np.asarray([point.delay_ps for point in delay_points], dtype=float),
            delta_od=np.stack([point.delta_od for point in delay_points], axis=0),
            delta_t_over_t=np.stack(
                [point.delta_t_over_t for point in delay_points], axis=0
            ),
            wavelength_nm=wavelength_nm,
            wavenumber_cm_inv=wavenumber_cm_inv,
        )

    def merge_scan_results(
        self,
        scan_results: list[TransientAbsorptionScanResult],
    ) -> TransientAbsorptionMergedResult:
        if not scan_results:
            raise ValueError("At least one scan result is required for merging.")

        first = scan_results[0]
        delay_position_mm = first.delay_position_mm
        delay_ps = first.delay_ps
        delta_od_stack = np.stack([scan.delta_od for scan in scan_results], axis=0)
        delta_t_stack = np.stack([scan.delta_t_over_t for scan in scan_results], axis=0)
        if delta_od_stack.shape[0] > 1:
            delta_od_std = _nanstd_no_warning(delta_od_stack, axis=0, ddof=1)
            delta_t_std = _nanstd_no_warning(delta_t_stack, axis=0, ddof=1)
            delta_od_count = np.sum(np.isfinite(delta_od_stack), axis=0)
            delta_t_count = np.sum(np.isfinite(delta_t_stack), axis=0)
            delta_od_sem = np.where(
                delta_od_count > 1,
                delta_od_std / np.sqrt(delta_od_count),
                0.0,
            )
            delta_t_sem = np.where(
                delta_t_count > 1,
                delta_t_std / np.sqrt(delta_t_count),
                0.0,
            )
        else:
            delta_od_std = np.zeros_like(delta_od_stack[0])
            delta_t_std = np.zeros_like(delta_t_stack[0])
            delta_od_sem = np.zeros_like(delta_od_stack[0])
            delta_t_sem = np.zeros_like(delta_t_stack[0])

        return TransientAbsorptionMergedResult(
            delay_position_mm=delay_position_mm,
            delay_ps=delay_ps,
            delta_od_mean=_nanmean_no_warning(delta_od_stack, axis=0),
            delta_od_std=delta_od_std,
            delta_od_sem=delta_od_sem,
            delta_t_over_t_mean=_nanmean_no_warning(delta_t_stack, axis=0),
            delta_t_over_t_std=delta_t_std,
            delta_t_over_t_sem=delta_t_sem,
            delta_od_scans=delta_od_stack,
            delta_t_over_t_scans=delta_t_stack,
            wavelength_nm=first.wavelength_nm,
            wavenumber_cm_inv=first.wavenumber_cm_inv,
            n_scans=int(delta_od_stack.shape[0]),
        )

    def save_scan_result(self, filename_stem: str, result: TransientAbsorptionScanResult) -> None:
        self.dataset_store.save_metadata(self.metadata)
        self.dataset_store.save_npz(
            f"{filename_stem}.npz",
            delay_position_mm=result.delay_position_mm,
            delay_ps=result.delay_ps,
            delta_od=result.delta_od,
            delta_t_over_t=result.delta_t_over_t,
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

    def save_delay_point_result(
        self,
        filename_stem: str,
        result: TransientDelayPointResult,
    ) -> None:
        """Persist processed spectra for one delay point."""

        self.dataset_store.save_npz(
            f"{filename_stem}.npz",
            pumped_signal=result.pumped_signal,
            unpumped_signal=result.unpumped_signal,
            delta_t_over_t=result.delta_t_over_t,
            delta_od=result.delta_od,
            invalid_pixel_mask=result.invalid_pixel_mask.astype(int),
        )
        self.dataset_store.save_json(
            f"{filename_stem}_summary.json",
            {
                "delay_position_mm": result.actual_delay_position_mm,
                "delay_ps": result.delay_ps,
                "accepted_lines": result.accepted_lines,
                "accepted_pairs": result.accepted_pairs,
                "requested_delay_position_mm": result.requested_delay_position_mm,
                "actual_delay_position_mm": result.actual_delay_position_mm,
                "applied_dark_offset_label": result.applied_dark_offset_label,
                "applied_intensity_dark_label": result.applied_intensity_dark_label,
                "demod_mode": result.demod_mode,
                "tail_flip_count": result.tail_flip_count,
                "tail_mean_median": result.tail_mean_median,
                "invalid_pixel_count": result.invalid_pixel_count,
                "invalid_pixel_fraction": result.invalid_pixel_fraction,
                "demod_metadata": result.demod_metadata,
            },
        )

    def save_merged_result(self, filename_stem: str, result: TransientAbsorptionMergedResult) -> None:
        self.dataset_store.save_npz(
            f"{filename_stem}.npz",
            delay_position_mm=result.delay_position_mm,
            delay_ps=result.delay_ps,
            delta_od_mean=result.delta_od_mean,
            delta_od_std=result.delta_od_std,
            delta_od_sem=result.delta_od_sem,
            delta_t_over_t_mean=result.delta_t_over_t_mean,
            delta_t_over_t_std=result.delta_t_over_t_std,
            delta_t_over_t_sem=result.delta_t_over_t_sem,
            delta_od_scans=result.delta_od_scans,
            delta_t_over_t_scans=result.delta_t_over_t_scans,
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
        self.dataset_store.save_json(
            f"{filename_stem}_summary.json",
            {
                "n_scans": result.n_scans,
                "n_delay_points": int(result.delay_ps.shape[0]),
            },
        )


def default_transient_absorption_metadata(sample_name: str) -> ExperimentMetadata:
    """Convenience metadata builder for first-pass runner setup."""

    return ExperimentMetadata(
        experiment_type="transient_absorption",
        sample_name=sample_name,
        started_at=datetime.now(timezone.utc).isoformat(),
    )


def default_transient_scan_plan(points_ps: list[float]) -> ScanPlan:
    """Create a simple one-axis scan plan from delay points in ps."""

    return ScanPlan(
        axis=ScanAxis(name="delay", units="ps", values=list(points_ps)),
        order_mode="ordered",
        metadata={"delay_ps_points": repr(points_ps)},
    )
