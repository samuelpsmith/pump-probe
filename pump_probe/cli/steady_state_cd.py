"""CLI runner for steady-state circular dichroism experiments."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from pump_probe.calibration.wavelength import (
    WavelengthAnchor,
    fit_manual_wavelength_calibration,
    load_wavelength_calibration,
)
from pump_probe.config.defaults import DATA_ROOT
from pump_probe.experiments.steady_state_cd import (
    SteadyStateCDRunConfig,
    SteadyStateCDRunner,
    default_steady_state_cd_metadata,
)
from pump_probe.hardware.daq import (
    LegacyPdaController,
    RUNTIME_PROFILE_PERSISTENT_LATEST,
    RUNTIME_PROFILE_PERSISTENT_ROBUST,
    RUNTIME_PROFILE_SAFE,
)
from pump_probe.hardware.daq.timing_profiles import (
    DEFAULT_TIMING_PROFILE_NAME,
    TIMING_PROFILES,
)
from pump_probe.io.dataset_store import ExperimentDatasetStore
from pump_probe.io.naming import run_directory_name


def _parse_anchor(text: str) -> WavelengthAnchor:
    pixel_text, wavelength_text = str(text).split(":", maxsplit=1)
    return WavelengthAnchor(
        pixel_index=float(pixel_text),
        wavelength_nm=float(wavelength_text),
        label=f"{pixel_text}:{wavelength_text}",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Acquire a steady-state odd/even modulation stack for PEM-style "
            "circular dichroism measurements."
        )
    )
    parser.add_argument("--sample-name", required=True)
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
    parser.add_argument("--lines-per-point", type=int, default=256)
    parser.add_argument("--use-reference-channel", action="store_true")
    parser.add_argument("--modulation-sign", type=float, default=1.0)
    parser.add_argument("--read-timeout-s", type=float, default=10.0)
    parser.add_argument("--ai-buffer-lines", type=int, default=256)
    parser.add_argument("--pulse-grouping-size", type=int, default=32)
    parser.add_argument("--save-subaverages", dest="save_subaverages", action="store_true", default=True)
    parser.add_argument("--no-save-subaverages", dest="save_subaverages", action="store_false")
    parser.add_argument("--external-trigger", dest="external_trigger", action="store_true", default=True)
    parser.add_argument("--no-external-trigger", dest="external_trigger", action="store_false")
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
        "--wavelength-anchor",
        dest="wavelength_anchors",
        action="append",
        type=_parse_anchor,
        default=[],
    )
    parser.add_argument("--wavelength-polynomial-order", type=int, default=None)
    parser.add_argument(
        "--wavelength-calibration-file",
        type=Path,
        default=None,
        help="Load an existing wavelength_calibration.json instead of fitting from anchors.",
    )
    parser.add_argument("--noninteractive", action="store_true")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate configuration and print the resolved run plan without acquiring data.",
    )
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
    controller.set_output_cropping(args.crop_output)
    controller.set_video_timing(
        dummy_clocks=args.dummy_clocks,
        pixel_clocks=controller.legacy.video_pixel_clocks,
        start_on_st_fall=(not args.start_on_st_rise),
    )
    return controller


def _maybe_build_wavelength_calibration(args: argparse.Namespace):
    if args.wavelength_calibration_file is not None:
        return load_wavelength_calibration(args.wavelength_calibration_file)
    if not args.wavelength_anchors:
        return None
    return fit_manual_wavelength_calibration(
        anchors=list(args.wavelength_anchors),
        polynomial_order=args.wavelength_polynomial_order,
        operator=args.operator,
        notes="manual anchors from steady_state_cd CLI",
    )


def run_steady_state_cd(args: argparse.Namespace) -> Path:
    run_dir = Path(args.output_root) / run_directory_name(
        "steady_state_cd",
        label=args.label or args.sample_name,
    )
    store = ExperimentDatasetStore(run_dir)
    metadata = default_steady_state_cd_metadata(args.sample_name)
    metadata.notes = args.notes
    metadata.provenance.operator = args.operator
    config = SteadyStateCDRunConfig(
        runtime_profile=args.runtime_profile,
        expected_trigger_hz=args.expected_trigger_hz,
        lines_per_point=args.lines_per_point,
        use_reference_channel=args.use_reference_channel,
        modulation_sign=args.modulation_sign,
        ai_buffer_lines=args.ai_buffer_lines,
        timeout_s=args.read_timeout_s,
        pulse_grouping_size=args.pulse_grouping_size,
        save_subaverages=args.save_subaverages,
    )
    wavelength_calibration = _maybe_build_wavelength_calibration(args)
    controller = _build_controller(args)
    runner = SteadyStateCDRunner(
        metadata=metadata,
        config=config,
        dataset_store=store,
        wavelength_calibration=wavelength_calibration,
    )

    if args.check:
        print("steady_state_cd check passed.")
        print(f"Planned run directory: {run_dir}")
        print(
            f"runtime={config.runtime_profile}, timing={args.timing_profile}, "
            f"lines_per_point={config.lines_per_point}, ref_channel={config.use_reference_channel}, "
            f"modulation_sign={config.modulation_sign}"
        )
        print(controller.format_timing_diagnostics(config.expected_trigger_hz))
        return run_dir

    print(f"Run directory: {run_dir}")
    if not args.noninteractive:
        input(
            "Prepare the steady-state CD condition and modulation scheme, then press Enter to acquire..."
        )

    stack = runner.capture_modulation_stack(controller)
    result = runner.build_result(stack)
    runner.save_captured_stack("steady_state_cd_raw", stack)
    runner.save_result(result)
    store.save_json("run_config.json", asdict(config))
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
        },
    )
    if wavelength_calibration is not None:
        store.save_json("wavelength_calibration.json", wavelength_calibration)

    print("Steady-state CD run complete.")
    return run_dir


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_steady_state_cd(args)


if __name__ == "__main__":
    main()
