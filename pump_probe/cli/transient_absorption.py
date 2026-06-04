"""CLI runner for transient absorption experiments."""

from __future__ import annotations

import argparse
import contextlib
from dataclasses import asdict
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any
import warnings

import matplotlib.pyplot as plt
import numpy as np

from pump_probe.acquisition import ContinuousLineReader
from pump_probe.calibration import (
    WavelengthAnchor,
    build_detector_baseline_calibration,
    fit_manual_wavelength_calibration,
    load_detector_baseline_calibration,
    load_wavelength_calibration,
)
from pump_probe.calibration.delay import stage_position_mm_from_delay_ps
from pump_probe.config.defaults import DATA_ROOT
from pump_probe.experiments.base import CapturedLineStack
from pump_probe.experiments.transient_absorption import (
    TransientAbsorptionRunConfig,
    TransientAbsorptionRunner,
    default_transient_absorption_metadata,
    default_transient_scan_plan,
)
from pump_probe.hardware.daq import (
    LegacyPdaController,
    RUNTIME_PROFILE_PERSISTENT_LATEST,
    RUNTIME_PROFILE_PERSISTENT_ROBUST,
    RUNTIME_PROFILE_SAFE,
    RawLine,
)
from pump_probe.hardware.daq.timing_profiles import (
    DEFAULT_TIMING_PROFILE_NAME,
    TIMING_PROFILES,
)
from pump_probe.hardware.stages import ManualDelayStage, XPSDelayStage
from pump_probe.hardware.stages.calibration import (
    four_pass_retroreflector_delay_calibration,
)
from pump_probe.io.dataset_store import ExperimentDatasetStore
from pump_probe.io.naming import run_directory_name
from pump_probe.processing.chop_demod import (
    DEMOD_MODE_CHOICES,
    DEMOD_MODE_TAIL_GUIDED_PAIRS,
    load_channel_dark_npz,
)


STAGE_MODE_MANUAL = "manual"
STAGE_MODE_XPS = "xps"
SCAN_ORDER_ORDERED = "ordered"
SCAN_ORDER_RANDOMIZED = "randomized"


def _nanmean_no_warning(values: np.ndarray, axis: int = 0) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanmean(values, axis=axis)


def _parse_anchor(text: str) -> WavelengthAnchor:
    try:
        pixel_text, wavelength_text = str(text).split(":", maxsplit=1)
        return WavelengthAnchor(
            pixel_index=float(pixel_text),
            wavelength_nm=float(wavelength_text),
            label=f"{pixel_text}:{wavelength_text}",
        )
    except Exception as exc:
        raise argparse.ArgumentTypeError(
            f"Could not parse wavelength anchor '{text}'. Use PIXEL:WAVELENGTH_NM."
        ) from exc


def _parse_delay_file(path: Path) -> list[float]:
    values: list[float] = []
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        pieces = [piece for piece in re.split(r"[\s,]+", line) if piece]
        values.extend(float(piece) for piece in pieces)
    return values


def _load_config_defaults(config_path: str | None) -> dict[str, Any]:
    if not config_path:
        return {}
    path = Path(config_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    defaults = dict(payload)
    path_keys = {
        "output_root",
        "delay_ps_file",
        "pump_dark_file",
        "intensity_dark_file",
        "save_dark_offset",
        "wavelength_calibration_file",
        "config",
    }
    for key in path_keys:
        if key in defaults and defaults[key] not in (None, ""):
            defaults[key] = Path(defaults[key])
    if "delay_points_ps" in defaults:
        defaults["delay_points_ps"] = [float(v) for v in defaults["delay_points_ps"]]
    anchors = []
    for item in defaults.get("wavelength_anchors", []):
        if isinstance(item, str):
            anchors.append(_parse_anchor(item))
        elif isinstance(item, dict):
            anchors.append(
                WavelengthAnchor(
                    pixel_index=float(item["pixel_index"]),
                    wavelength_nm=float(item["wavelength_nm"]),
                    label=item.get("label"),
                )
            )
        else:
            raise ValueError(f"Unsupported wavelength anchor config payload: {item!r}")
    if anchors:
        defaults["wavelength_anchors"] = anchors
    return defaults


def _build_parser(defaults: dict[str, Any] | None = None) -> argparse.ArgumentParser:
    defaults = dict(defaults or {})
    parser = argparse.ArgumentParser(
        description=(
            "Acquire transient absorption pump-chop demod data over an explicit "
            "delay list. Supports manual or XPS-controlled delay stages, merged "
            "multi-scan outputs, and optional dark-offset subtraction."
        )
    )
    parser.add_argument("--config", type=Path, default=None, help="Optional JSON config file.")
    parser.add_argument("--sample-name", required=("sample_name" not in defaults))
    parser.add_argument("--label", default=None)
    parser.add_argument("--operator", default=None)
    parser.add_argument("--notes", default=None)
    parser.add_argument("--output-root", type=Path, default=DATA_ROOT)
    parser.add_argument(
        "--timing-profile",
        default=DEFAULT_TIMING_PROFILE_NAME,
        choices=tuple(sorted(TIMING_PROFILES)),
    )
    parser.add_argument(
        "--runtime-profile",
        default=RUNTIME_PROFILE_PERSISTENT_ROBUST,
        choices=(
            RUNTIME_PROFILE_SAFE,
            RUNTIME_PROFILE_PERSISTENT_ROBUST,
            RUNTIME_PROFILE_PERSISTENT_LATEST,
        ),
    )
    parser.add_argument("--expected-trigger-hz", type=float, default=1000.0)
    parser.add_argument(
        "--lines-per-delay",
        type=int,
        default=256,
        help=(
            "Legacy line-count target for each delay. If --pairs-per-delay is "
            "provided, the runner derives the needed line count from the demod mode."
        ),
    )
    parser.add_argument(
        "--pairs-per-delay",
        type=int,
        default=None,
        help=(
            "Preferred pump-chop integration target. Tail-guided mode reads "
            "pairs+1 lines; fixed odd/even mode reads 2*pairs lines."
        ),
    )
    parser.add_argument("--scans", type=int, default=1)
    parser.add_argument(
        "--scan-order",
        default=SCAN_ORDER_ORDERED,
        choices=(SCAN_ORDER_ORDERED, SCAN_ORDER_RANDOMIZED),
    )
    parser.add_argument("--scan-seed", type=int, default=0)
    parser.add_argument("--use-reference-channel", action="store_true")
    parser.add_argument(
        "--demod-mode",
        default=DEMOD_MODE_TAIL_GUIDED_PAIRS,
        choices=DEMOD_MODE_CHOICES,
        help="Pump-chop demod mode. Tail-guided adjacent pairs is the default.",
    )
    parser.add_argument("--pump-chop-sign", type=float, default=-1.0)
    parser.add_argument("--tail-sign-start", type=int, default=900)
    parser.add_argument("--tail-sign-stop", type=int, default=1000)
    parser.add_argument("--tail-sign-expected", type=float, default=1.0)
    parser.add_argument(
        "--tail-baseline-subtract",
        dest="tail_baseline_subtract",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-tail-baseline-subtract",
        dest="tail_baseline_subtract",
        action="store_false",
    )
    parser.add_argument(
        "--min-light-voltage",
        type=float,
        default=0.025,
        help=(
            "Minimum dark-corrected light voltage for valid main-only OD pixels."
        ),
    )
    parser.add_argument("--divide-floor", type=float, default=1e-12)
    parser.add_argument("--external-trigger", dest="external_trigger", action="store_true", default=True)
    parser.add_argument("--no-external-trigger", dest="external_trigger", action="store_false")
    parser.add_argument("--read-timeout-s", type=float, default=10.0)
    parser.add_argument("--ai-buffer-lines", type=int, default=256)
    parser.add_argument(
        "--continuous-acquisition",
        dest="continuous_acquisition",
        action="store_true",
        default=True,
        help=(
            "Keep one DAQ session open across the TAS run and continuously drain "
            "lines during stage motion."
        ),
    )
    parser.add_argument(
        "--per-delay-session",
        dest="continuous_acquisition",
        action="store_false",
        help="Use the old behavior: open/close a DAQ session at every delay point.",
    )
    parser.add_argument(
        "--reader-buffer-lines",
        type=int,
        default=8192,
        help="FIFO depth for continuous TAS DAQ reader.",
    )
    parser.add_argument("--pulse-grouping-size", type=int, default=32)
    parser.add_argument("--save-subaverages", dest="save_subaverages", action="store_true", default=True)
    parser.add_argument("--no-save-subaverages", dest="save_subaverages", action="store_false")
    parser.add_argument("--trigger-phase-us", type=float, default=0.0)
    parser.add_argument("--crop-output", dest="crop_output", action="store_true", default=True)
    parser.add_argument("--full-window", dest="crop_output", action="store_false")
    parser.add_argument("--dummy-clocks", type=int, default=14)
    parser.add_argument("--start-on-st-rise", action="store_true")
    parser.add_argument("--trigger-filter", action="store_true")
    parser.add_argument("--trigger-filter-min-pulse-us", type=float, default=0.2)
    parser.add_argument("--trigger-sync", dest="trigger_sync", action="store_true", default=True)
    parser.add_argument("--no-trigger-sync", dest="trigger_sync", action="store_false")
    parser.add_argument(
        "--chopper-sync-output",
        dest="chopper_sync_output",
        action="store_true",
        default=True,
        help="Enable the DAQ-driven 500 Hz chopper sync output on PFI3 during TAS.",
    )
    parser.add_argument(
        "--no-chopper-sync-output",
        dest="chopper_sync_output",
        action="store_false",
    )
    parser.add_argument("--chopper-sync-pfi", default="PFI3")
    parser.add_argument(
        "--configure-chopper",
        dest="configure_chopper",
        action="store_true",
        default=True,
        help="Run the Newport 3502 configuration script before TAS acquisition.",
    )
    parser.add_argument(
        "--no-configure-chopper",
        dest="configure_chopper",
        action="store_false",
    )
    parser.add_argument("--chopper-freq-hz", type=float, default=500.0)
    parser.add_argument("--chopper-wheel", default="42/30")
    parser.add_argument("--chopper-sync-mode", default="ext+")
    parser.add_argument("--chopper-mode", default="normal")
    parser.add_argument(
        "--chopper-warmup-s",
        type=float,
        default=30.0,
        help="Wait time after chopper configuration and DAQ sync-output startup before delay-point acquisition begins.",
    )
    parser.add_argument(
        "--delay-ps",
        dest="delay_points_ps",
        action="append",
        type=float,
        default=[],
        help="Add one delay point in ps. Repeat as needed.",
    )
    parser.add_argument(
        "--delay-ps-file",
        type=Path,
        default=None,
        help="Text file containing delay points in ps (whitespace or comma separated).",
    )
    parser.add_argument(
        "--zero-position-mm",
        type=float,
        required=("zero_position_mm" not in defaults),
        help="Mechanical stage position corresponding to 0 ps.",
    )
    parser.add_argument(
        "--path-multiplier",
        type=float,
        default=4.0,
        help="Optical path-length multiplier relative to stage displacement.",
    )
    parser.add_argument(
        "--stage-mode",
        default=STAGE_MODE_MANUAL,
        choices=(STAGE_MODE_MANUAL, STAGE_MODE_XPS),
    )
    parser.add_argument("--stage-settle-s", type=float, default=0.2)
    parser.add_argument("--stage-position-tolerance-mm", type=float, default=0.002)
    parser.add_argument("--xps-ip", default="192.168.254.254")
    parser.add_argument("--xps-port", type=int, default=5001)
    parser.add_argument("--xps-timeout-ms", type=int, default=100)
    parser.add_argument("--xps-group", default="DS")
    parser.add_argument("--xps-positioner-name", default=None)
    parser.add_argument("--xps-auto-initialize", dest="xps_auto_initialize", action="store_true", default=True)
    parser.add_argument("--no-xps-auto-initialize", dest="xps_auto_initialize", action="store_false")
    parser.add_argument("--xps-auto-home", dest="xps_auto_home", action="store_true", default=True)
    parser.add_argument("--no-xps-auto-home", dest="xps_auto_home", action="store_false")
    parser.add_argument("--xps-verbose", dest="xps_verbose", action="store_true", default=True)
    parser.add_argument("--no-xps-verbose", dest="xps_verbose", action="store_false")
    parser.add_argument(
        "--pump-dark-file",
        type=Path,
        default=None,
        help=(
            "Optional NPZ file containing a saved dark offset baseline to subtract "
            "from delta OD. Accepts either TAS-saved baseline_signal files or the "
            "live_cmos pump_chop_dark_offset_latest.npz format."
        ),
    )
    parser.add_argument(
        "--intensity-dark-file",
        type=Path,
        default=None,
        help=(
            "Optional live-view channel_dark_offset_latest.npz style file. "
            "Main-only TAS subtracts main_integrated before OD; referenced TAS "
            "subtracts main/reference darks before main/ref."
        ),
    )
    parser.add_argument(
        "--save-dark-offset",
        type=Path,
        default=None,
        help="If set, save the merged delta OD mean as a detector baseline calibration at the end of the run.",
    )
    parser.add_argument(
        "--wavelength-anchor",
        dest="wavelength_anchors",
        action="append",
        type=_parse_anchor,
        default=[],
        help="Manual wavelength anchor as PIXEL:WAVELENGTH_NM. Repeat as needed.",
    )
    parser.add_argument("--wavelength-polynomial-order", type=int, default=None)
    parser.add_argument(
        "--wavelength-calibration-file",
        type=Path,
        default=None,
        help="Load an existing wavelength_calibration.json instead of fitting from anchors.",
    )
    parser.add_argument(
        "--noninteractive",
        action="store_true",
        help="Disable manual prompts. Required for unattended XPS-driven runs.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate configuration and print the resolved TAS run plan without moving stages or acquiring data.",
    )
    parser.set_defaults(**defaults)
    return parser


def _build_controller(args: argparse.Namespace) -> LegacyPdaController:
    controller = LegacyPdaController()
    controller.apply_timing_profile(args.timing_profile)
    controller.enable_external_trigger(args.external_trigger)
    controller.set_trigger_phase_shift_s(args.trigger_phase_us * 1e-6)
    controller.set_trigger_filter(
        enable=args.trigger_filter,
        min_pulse_width_s=args.trigger_filter_min_pulse_us * 1e-6,
    )
    controller.set_trigger_sync(args.trigger_sync)
    controller.set_retrigger_initial_delay(True)
    controller.set_chopper_sync_output(
        enable=bool(args.chopper_sync_output),
        out_pfi=str(args.chopper_sync_pfi),
        source_terminal=controller.legacy.trig_in,
        initial_delay_ticks=0,
        high_ticks=2,
        low_ticks=2,
    )
    controller.set_output_cropping(args.crop_output)
    controller.set_video_timing(
        dummy_clocks=args.dummy_clocks,
        pixel_clocks=controller.legacy.video_pixel_clocks,
        start_on_st_fall=(not args.start_on_st_rise),
    )
    return controller


def _run_chopper_setup(args: argparse.Namespace) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "stepper_control" / "chopper.py"
    if not script_path.exists():
        raise FileNotFoundError(
            f"Could not find chopper setup script at {script_path}"
        )
    cmd = [
        sys.executable,
        str(script_path),
        "--freq-hz",
        str(float(args.chopper_freq_hz)),
        "--wheel",
        str(args.chopper_wheel),
        "--sync",
        str(args.chopper_sync_mode),
        "--mode",
        str(args.chopper_mode),
    ]
    print(
        "Configuring chopper: "
        f"wheel={args.chopper_wheel}, sync={args.chopper_sync_mode}, "
        f"mode={args.chopper_mode}, freq={float(args.chopper_freq_hz):.3f} Hz"
    )
    result = subprocess.run(cmd, cwd=str(repo_root))
    if result.returncode != 0:
        raise RuntimeError(
            "Chopper configuration script failed with exit code "
            f"{result.returncode}. Command: {' '.join(cmd)}"
        )


def _build_stage(args: argparse.Namespace):
    if args.stage_mode == STAGE_MODE_MANUAL:
        if args.noninteractive:
            raise ValueError(
                "Manual stage mode requires interactive prompts. "
                "Use --stage-mode xps for unattended runs."
            )
        return ManualDelayStage(initial_position_mm=args.zero_position_mm)

    return XPSDelayStage(
        ip=args.xps_ip,
        port=args.xps_port,
        timeout_ms=args.xps_timeout_ms,
        group=args.xps_group,
        auto_initialize=args.xps_auto_initialize,
        auto_home=args.xps_auto_home,
        positioner_name=args.xps_positioner_name,
        move_settle_s=args.stage_settle_s,
        position_tolerance_mm=args.stage_position_tolerance_mm,
        verbose=args.xps_verbose,
    )


def _raw_lines_to_stack(lines: list[RawLine], *, timing_profile_name: str | None = None) -> CapturedLineStack:
    if not lines:
        raise ValueError("Cannot build CapturedLineStack from an empty line list.")
    main_lines = [np.asarray(line.voltage_main, dtype=float).copy() for line in lines]
    ref_present = all(line.voltage_ref is not None for line in lines)
    ref_lines = (
        [np.asarray(line.voltage_ref, dtype=float).copy() for line in lines]
        if ref_present
        else []
    )
    timestamps = [
        float(line.timestamp_s if line.timestamp_s is not None else time.time())
        for line in lines
    ]
    metadata = [dict(line.metadata) for line in lines]
    sample_index = np.asarray(lines[0].sample_index, dtype=int)
    return CapturedLineStack(
        sample_index=sample_index,
        main_lines=np.stack(main_lines, axis=0),
        ref_lines=(np.stack(ref_lines, axis=0) if ref_lines else None),
        timestamps_s=np.asarray(timestamps, dtype=float),
        timing_profile_name=timing_profile_name or lines[0].timing_profile_name,
        line_metadata=metadata,
    )


def _capture_from_continuous_reader(
    *,
    reader: ContinuousLineReader,
    runner: TransientAbsorptionRunner,
    timing_profile_name: str | None,
    timeout_s: float,
    pre_capture_discarded: int,
) -> CapturedLineStack:
    n_lines = int(runner.effective_lines_per_delay())
    expected_hz = max(1e-9, float(runner.config.expected_trigger_hz))
    collect_timeout_s = max(float(timeout_s), 2.0 + 2.5 * n_lines / expected_hz)
    lines = reader.collect_lines(n_lines, timeout_s=collect_timeout_s)
    stack = _raw_lines_to_stack(lines, timing_profile_name=timing_profile_name)
    if stack.line_metadata:
        stack.line_metadata[0]["continuous_reader"] = True
        stack.line_metadata[0]["pre_capture_discarded_lines"] = int(
            pre_capture_discarded
        )
        stack.line_metadata[0]["reader_stats_after_capture"] = reader.stats()
        stack.line_metadata[0]["target_pairs_per_delay"] = int(
            runner.target_pairs_per_delay()
        )
        stack.line_metadata[0]["effective_lines_per_delay"] = int(n_lines)
    return stack


def _delay_points_ps(args: argparse.Namespace) -> list[float]:
    values = list(args.delay_points_ps)
    if args.delay_ps_file is not None:
        values.extend(_parse_delay_file(args.delay_ps_file))
    if not values:
        raise ValueError("Provide delay points via --delay-ps and/or --delay-ps-file.")
    return values


def _maybe_build_wavelength_calibration(args: argparse.Namespace):
    if args.wavelength_calibration_file is not None:
        return load_wavelength_calibration(args.wavelength_calibration_file)
    if not args.wavelength_anchors:
        return None
    return fit_manual_wavelength_calibration(
        anchors=list(args.wavelength_anchors),
        polynomial_order=args.wavelength_polynomial_order,
        operator=args.operator,
        notes="manual anchors from transient_absorption CLI",
    )


def _save_merged_overview(
    run_dir: Path,
    merged_result,
    *,
    sample_name: str,
) -> None:
    delay_ps = np.asarray(merged_result.delay_ps, dtype=float)
    delta_od_mean = np.asarray(merged_result.delta_od_mean, dtype=float)
    delta_od_sem = np.asarray(merged_result.delta_od_sem, dtype=float)

    if merged_result.wavelength_nm is not None and len(merged_result.wavelength_nm):
        spectral_axis = np.asarray(merged_result.wavelength_nm, dtype=float)
        axis_label = "Wavelength (nm)"
    elif merged_result.wavenumber_cm_inv is not None and len(merged_result.wavenumber_cm_inv):
        spectral_axis = np.asarray(merged_result.wavenumber_cm_inv, dtype=float)
        axis_label = "Wavenumber (cm$^{-1}$)"
    else:
        spectral_axis = np.arange(delta_od_mean.shape[1], dtype=float)
        axis_label = "Detector pixel"

    order = np.argsort(spectral_axis)
    spectral_axis = spectral_axis[order]
    delta_od_mean = delta_od_mean[:, order]
    delta_od_sem = delta_od_sem[:, order]

    representative_targets = [0.0, 1.0, 5.0, 10.0, 20.0, 50.0]
    representative_indices: list[int] = []
    for target in representative_targets:
        idx = int(np.argmin(np.abs(delay_ps - target)))
        if idx not in representative_indices:
            representative_indices.append(idx)

    vmax = float(np.nanpercentile(np.abs(delta_od_mean), 99))
    vmax = max(vmax, 1e-6)

    fig = plt.figure(figsize=(12, 8), constrained_layout=True)
    grid = fig.add_gridspec(2, 1, height_ratios=[2.2, 1.2])
    ax_heatmap = fig.add_subplot(grid[0])
    ax_trace = fig.add_subplot(grid[1])

    im = ax_heatmap.imshow(
        delta_od_mean,
        aspect="auto",
        origin="lower",
        extent=[
            float(spectral_axis[0]),
            float(spectral_axis[-1]),
            float(delay_ps[0]),
            float(delay_ps[-1]),
        ],
        cmap="RdBu_r",
        vmin=-vmax,
        vmax=vmax,
    )
    colorbar = fig.colorbar(im, ax=ax_heatmap, pad=0.02)
    colorbar.set_label("Delta OD")
    ax_heatmap.set_title(f"{sample_name} TAS: merged delta OD")
    ax_heatmap.set_xlabel(axis_label)
    ax_heatmap.set_ylabel("Delay (ps)")

    colors = plt.cm.viridis(np.linspace(0.05, 0.95, len(representative_indices)))
    for color, idx in zip(colors, representative_indices):
        label = f"{delay_ps[idx]:.2f} ps"
        spectrum = delta_od_mean[idx]
        sem = delta_od_sem[idx]
        ax_trace.plot(spectral_axis, spectrum, color=color, lw=1.6, label=label)
        ax_trace.fill_between(
            spectral_axis,
            spectrum - sem,
            spectrum + sem,
            color=color,
            alpha=0.18,
            linewidth=0,
        )
    ax_trace.axhline(0.0, color="k", lw=0.8, alpha=0.5)
    ax_trace.set_xlabel(axis_label)
    ax_trace.set_ylabel("Delta OD")
    ax_trace.set_title("Selected delay slices (mean ± SEM across scans)")
    ax_trace.legend(ncols=3, fontsize=9, frameon=False)

    fig.savefig(run_dir / "merged_delta_od_overview.png", dpi=180)
    fig.savefig(run_dir / "merged_tas_wavelength_summary.png", dpi=180)
    plt.close(fig)


def run_transient_absorption(args: argparse.Namespace) -> Path:
    delay_points_ps = _delay_points_ps(args)
    run_dir = Path(args.output_root) / run_directory_name(
        "transient_absorption",
        label=args.label or args.sample_name,
    )
    store = ExperimentDatasetStore(run_dir)
    metadata = default_transient_absorption_metadata(args.sample_name)
    metadata.notes = args.notes
    metadata.provenance.operator = args.operator

    delay_calibration = four_pass_retroreflector_delay_calibration(
        zero_position_mm=args.zero_position_mm,
        path_multiplier=args.path_multiplier,
        operator=args.operator,
        notes="TAS CLI delay calibration",
    )
    wavelength_calibration = _maybe_build_wavelength_calibration(args)
    dark_offset_calibration = (
        None
        if args.pump_dark_file is None
        else load_detector_baseline_calibration(args.pump_dark_file)
    )
    intensity_dark_correction = (
        None
        if args.intensity_dark_file is None
        else load_channel_dark_npz(args.intensity_dark_file)
    )
    if (
        args.use_reference_channel
        and intensity_dark_correction is not None
        and intensity_dark_correction.reference is None
    ):
        raise ValueError(
            "Referenced TAS requires an intensity dark file containing both "
            "'main_integrated' and 'reference_integrated'. The supplied file "
            f"({args.intensity_dark_file}) only contains a main-channel dark. "
            "Collect a new referenced channel dark in live_cmos "
            "(`python live_cmos.py --observable-mode main_ref_normalized`, "
            "block pump and probe, then press 'c'), or omit the intensity dark file."
        )
    config = TransientAbsorptionRunConfig(
        runtime_profile=args.runtime_profile,
        expected_trigger_hz=args.expected_trigger_hz,
        lines_per_delay=args.lines_per_delay,
        pairs_per_delay=args.pairs_per_delay,
        scans=args.scans,
        use_reference_channel=args.use_reference_channel,
        demod_mode=args.demod_mode,
        pump_chop_sign=args.pump_chop_sign,
        tail_sign_start=args.tail_sign_start,
        tail_sign_stop=args.tail_sign_stop,
        tail_sign_expected=args.tail_sign_expected,
        tail_baseline_subtract=args.tail_baseline_subtract,
        min_light_voltage=args.min_light_voltage,
        divide_floor=args.divide_floor,
        ai_buffer_lines=args.ai_buffer_lines,
        timeout_s=args.read_timeout_s,
        pulse_grouping_size=args.pulse_grouping_size,
        save_subaverages=args.save_subaverages,
        save_dark_offset=bool(args.save_dark_offset),
    )
    scan_plan = default_transient_scan_plan(delay_points_ps)
    scan_plan.n_scans = int(args.scans)
    scan_plan.order_mode = str(args.scan_order)
    scan_plan.lines_per_point = int(
        config.lines_per_delay
        if config.pairs_per_delay is None
        else (
            config.pairs_per_delay + 1
            if config.demod_mode == DEMOD_MODE_TAIL_GUIDED_PAIRS
            else 2 * config.pairs_per_delay
        )
    )
    if (
        args.use_reference_channel
        and args.timing_profile == DEFAULT_TIMING_PROFILE_NAME
    ):
        args.timing_profile = "almost_full_2channel"
        print(
            "Info: --use-reference-channel requested; using timing profile "
            "'almost_full_2channel'."
        )
    controller = _build_controller(args)
    runner = TransientAbsorptionRunner(
        metadata=metadata,
        scan_plan=scan_plan,
        config=config,
        dataset_store=store,
        delay_calibration=delay_calibration,
        wavelength_calibration=wavelength_calibration,
        dark_offset_calibration=dark_offset_calibration,
        intensity_dark_correction=intensity_dark_correction,
    )

    if args.check:
        print("transient_absorption check passed.")
        print(f"Planned run directory: {run_dir}")
        print(
            f"runtime={config.runtime_profile}, timing={args.timing_profile}, "
            f"stage_mode={args.stage_mode}, scans={config.scans}, "
            f"delay_points={len(delay_points_ps)}, ref_channel={config.use_reference_channel}, "
            f"demod={config.demod_mode}, sign={config.pump_chop_sign:+.0f}, "
            f"target_pairs={runner.target_pairs_per_delay()}, "
            f"effective_lines={runner.effective_lines_per_delay()}"
        )
        print(
            f"dark_offset={'none' if dark_offset_calibration is None else dark_offset_calibration.calibration_id}, "
            f"intensity_dark={'none' if intensity_dark_correction is None else intensity_dark_correction.label}, "
            f"scan_order={scan_plan.order_mode}, grouping={config.pulse_grouping_size}, "
            f"continuous_acquisition={args.continuous_acquisition}, "
            f"configure_chopper={args.configure_chopper}, "
            f"chopper_sync_output={args.chopper_sync_output}, "
            f"chopper_warmup_s={float(args.chopper_warmup_s):.1f}"
        )
        print(controller.format_timing_diagnostics(config.expected_trigger_hz))
        return run_dir

    store.save_json("run_config.json", asdict(config))
    store.save_json("scan_plan.json", scan_plan)
    store.save_json("delay_calibration.json", delay_calibration)
    if wavelength_calibration is not None:
        store.save_json("wavelength_calibration.json", wavelength_calibration)
    if dark_offset_calibration is not None:
        store.save_json("dark_offset_calibration.json", dark_offset_calibration)
    if intensity_dark_correction is not None:
        store.save_json(
            "intensity_dark_setup.json",
            {
                "label": intensity_dark_correction.label,
                "source_path": intensity_dark_correction.source_path,
                "main_shape": list(np.asarray(intensity_dark_correction.main).shape),
                "reference_shape": (
                    []
                    if intensity_dark_correction.reference is None
                    else list(np.asarray(intensity_dark_correction.reference).shape)
                ),
            },
        )
    store.save_json(
        "daq_setup.json",
        {
            "timing_profile": args.timing_profile,
            "external_trigger": args.external_trigger,
            "trigger_phase_us": args.trigger_phase_us,
            "crop_output": args.crop_output,
            "dummy_clocks": args.dummy_clocks,
            "start_on_st_rise": args.start_on_st_rise,
            "trigger_filter": args.trigger_filter,
            "trigger_filter_min_pulse_us": args.trigger_filter_min_pulse_us,
            "trigger_sync": args.trigger_sync,
            "continuous_acquisition": args.continuous_acquisition,
            "reader_buffer_lines": args.reader_buffer_lines,
            "ai_buffer_lines": args.ai_buffer_lines,
            "chopper_sync_output": args.chopper_sync_output,
            "chopper_sync_pfi": args.chopper_sync_pfi,
            "chopper_warmup_s": args.chopper_warmup_s,
        },
    )
    store.save_json(
        "stage_setup.json",
        {
            "stage_mode": args.stage_mode,
            "xps_ip": args.xps_ip,
            "xps_port": args.xps_port,
            "xps_group": args.xps_group,
            "xps_positioner_name": args.xps_positioner_name,
            "stage_settle_s": args.stage_settle_s,
            "stage_position_tolerance_mm": args.stage_position_tolerance_mm,
        },
    )
    store.save_json(
        "chopper_setup.json",
        {
            "configure_chopper": args.configure_chopper,
            "chopper_freq_hz": args.chopper_freq_hz,
            "chopper_wheel": args.chopper_wheel,
            "chopper_sync_mode": args.chopper_sync_mode,
            "chopper_mode": args.chopper_mode,
            "chopper_sync_output": args.chopper_sync_output,
            "chopper_sync_pfi": args.chopper_sync_pfi,
        },
    )

    print(f"Run directory: {run_dir}")
    print(
        f"Stage mode: {args.stage_mode}; scans={args.scans}; delay points={len(delay_points_ps)}; "
        f"runtime={args.runtime_profile}; demod={config.demod_mode}; "
        f"sign={config.pump_chop_sign:+.0f}"
    )
    if dark_offset_calibration is not None:
        print(f"Applying dark offset baseline: {dark_offset_calibration.calibration_id}")
    if intensity_dark_correction is not None:
        print(
            "Applying intensity/channel dark: "
            f"{intensity_dark_correction.label} ({intensity_dark_correction.source_path})"
        )
    if args.configure_chopper:
        _run_chopper_setup(args)

    all_scan_results = []
    with contextlib.ExitStack() as stack:
        stage = _build_stage(args)
        stack.callback(stage.close)
        if args.chopper_sync_output:
            chopper_sync = stack.enter_context(controller.open_chopper_sync_output_session())
            if getattr(chopper_sync, "available", False):
                print(
                    "Chopper sync output active on "
                    f"/Dev1/{str(args.chopper_sync_pfi).lstrip('/')} "
                    f"(driven from {controller.legacy.trig_in})."
                )
            else:
                raise RuntimeError(
                    "Chopper sync output session did not start successfully: "
                    f"{getattr(chopper_sync, 'error_text', '')}"
                )

        if (args.configure_chopper or args.chopper_sync_output) and float(args.chopper_warmup_s) > 0:
            warmup_s = float(args.chopper_warmup_s)
            print(
                f"Waiting {warmup_s:.1f} s for chopper warmup/lock before delay acquisition..."
            )
            time.sleep(warmup_s)
            print("Chopper warmup complete.")

        continuous_reader: ContinuousLineReader | None = None
        continuous_timing_profile: str | None = None
        if args.continuous_acquisition:
            daq_session = stack.enter_context(
                controller.open_session(
                    args.runtime_profile,
                    expected_trigger_hz=args.expected_trigger_hz,
                    read_reference=args.use_reference_channel,
                    ai_buffer_lines=args.ai_buffer_lines,
                    timeout_s=args.read_timeout_s,
                )
            )
            continuous_timing_profile = daq_session.info.timing_profile
            continuous_reader = ContinuousLineReader(
                daq_session,
                read_timeout_s=min(1.0, max(0.1, float(args.read_timeout_s))),
                max_buffer_lines=args.reader_buffer_lines,
            ).start()
            stack.callback(continuous_reader.close)
            time.sleep(0.05)
            warm_discarded = continuous_reader.discard_available()
            print(
                "Continuous DAQ reader active across TAS run "
                f"(timing={continuous_timing_profile}, discarded {warm_discarded} warmup lines)."
            )

        for scan_index in range(args.scans):
            print(f"Starting scan {scan_index + 1}/{args.scans}")
            acquisition_order = scan_plan.expanded_points(seed=(args.scan_seed + scan_index))
            point_results_by_delay: dict[float, Any] = {}
            for delay_index, delay_ps in enumerate(acquisition_order):
                target_mm = float(
                    stage_position_mm_from_delay_ps(delay_ps, delay_calibration)
                )
                pre_capture_discarded = 0
                if continuous_reader is not None:
                    pre_capture_discarded += continuous_reader.discard_available()
                stage.move_absolute_mm(target_mm)
                actual_mm = stage.get_position_mm()
                if continuous_reader is not None:
                    pre_capture_discarded += continuous_reader.discard_available()
                    stack_data = _capture_from_continuous_reader(
                        reader=continuous_reader,
                        runner=runner,
                        timing_profile_name=continuous_timing_profile,
                        timeout_s=args.read_timeout_s,
                        pre_capture_discarded=pre_capture_discarded,
                    )
                else:
                    stack_data = runner.capture_delay_point(controller)
                point_result = runner.build_delay_point_result(
                    requested_delay_position_mm=target_mm,
                    actual_delay_position_mm=actual_mm,
                    stack=stack_data,
                )
                point_results_by_delay[float(delay_ps)] = point_result

                stem = f"scan_{scan_index:03d}/delay_{delay_index:04d}"
                runner.save_captured_stack(f"{stem}_raw", stack_data)
                runner.save_delay_point_result(f"{stem}_result", point_result)
                print(
                    f"  delay {delay_index + 1}/{len(acquisition_order)}: "
                    f"{delay_ps:.3f} ps target={target_mm:.6f} mm actual={actual_mm:.6f} mm "
                    f"pairs={point_result.accepted_pairs} "
                    f"tailflips={point_result.tail_flip_count} "
                    f"invalid={100.0 * point_result.invalid_pixel_fraction:.1f}%"
                )

            ordered_results = [point_results_by_delay[float(delay)] for delay in delay_points_ps]
            scan_result = runner.build_scan_result(ordered_results)
            all_scan_results.append(scan_result)
            runner.save_scan_result(f"scan_{scan_index:03d}/scan_result", scan_result)
            store.save_npz(
                f"scan_{scan_index:03d}/scan_result.npz",
                delay_position_mm=scan_result.delay_position_mm,
                delay_ps=scan_result.delay_ps,
                delta_od=scan_result.delta_od,
                delta_t_over_t=scan_result.delta_t_over_t,
                wavelength_nm=(
                    scan_result.wavelength_nm
                    if scan_result.wavelength_nm is not None
                    else np.array([], dtype=float)
                ),
                wavenumber_cm_inv=(
                    scan_result.wavenumber_cm_inv
                    if scan_result.wavenumber_cm_inv is not None
                    else np.array([], dtype=float)
                ),
            )
            store.save_json(
                f"scan_{scan_index:03d}/scan_summary.json",
                {
                    "scan_index": scan_index,
                    "n_delay_points": len(ordered_results),
                    "delay_ps": [point.delay_ps for point in ordered_results],
                    "requested_delay_position_mm": [
                        point.requested_delay_position_mm for point in ordered_results
                    ],
                    "actual_delay_position_mm": [
                        point.actual_delay_position_mm for point in ordered_results
                    ],
                    "accepted_lines": [point.accepted_lines for point in ordered_results],
                    "accepted_pairs": [point.accepted_pairs for point in ordered_results],
                    "tail_flip_count": [
                        point.tail_flip_count for point in ordered_results
                    ],
                    "invalid_pixel_fraction": [
                        point.invalid_pixel_fraction for point in ordered_results
                    ],
                    "demod_mode": (
                        ordered_results[0].demod_mode if ordered_results else None
                    ),
                    "applied_dark_offset_label": (
                        ordered_results[0].applied_dark_offset_label
                        if ordered_results
                        else None
                    ),
                    "applied_intensity_dark_label": (
                        ordered_results[0].applied_intensity_dark_label
                        if ordered_results
                        else None
                    ),
                },
            )

    merged_result = runner.merge_scan_results(all_scan_results)
    runner.save_merged_result("merged_scan_result", merged_result)
    _save_merged_overview(run_dir, merged_result, sample_name=args.sample_name)

    if args.save_dark_offset is not None:
        baseline_signal = _nanmean_no_warning(merged_result.delta_od_mean, axis=0)
        dark_cal = build_detector_baseline_calibration(
            baseline_signal,
            calibration_id=Path(args.save_dark_offset).stem,
            operator=args.operator,
            notes=(
                "Saved from transient_absorption CLI merged delta OD, averaged "
                "over delay points for 1D blocked-pump baseline subtraction."
            ),
            axis_label="detector_pixel",
        )
        store.save_npz(
            str(Path(args.save_dark_offset)),
            baseline_signal=dark_cal.baseline_signal,
            baseline_grid=merged_result.delta_od_mean,
            delay_ps=merged_result.delay_ps,
        )
        print(f"Saved dark offset baseline to {Path(args.save_dark_offset)}")

    print("Transient absorption run complete.")
    print(f"Merged output: {run_dir / 'merged_scan_result.npz'}")
    return run_dir


def main(argv: list[str] | None = None) -> None:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", type=Path, default=None)
    pre_args, _ = pre_parser.parse_known_args(argv)
    defaults = _load_config_defaults(None if pre_args.config is None else str(pre_args.config))
    parser = _build_parser(defaults)
    args = parser.parse_args(argv)
    run_transient_absorption(args)


if __name__ == "__main__":
    main()
