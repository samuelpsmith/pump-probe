"""CLI runner for linear absorption experiments."""

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
from pump_probe.experiments.linear_absorption import (
    LINEAR_ABSORPTION_MAIN_ONLY,
    LINEAR_ABSORPTION_MAIN_REF_NORMALIZED,
    LinearAbsorptionRunConfig,
    LinearAbsorptionRunner,
    default_linear_absorption_metadata,
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
from pump_probe.io.export import save_linear_absorption_overview
from pump_probe.io.naming import run_directory_name


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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Acquire reference/sample spectra for a linear absorption "
            "measurement and save both raw stacks and processed absorbance."
        )
    )
    parser.add_argument("--sample-name", required=True)
    parser.add_argument("--label", default=None)
    parser.add_argument("--operator", default=None)
    parser.add_argument("--notes", default=None)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DATA_ROOT,
        help="Parent directory for run folders.",
    )
    parser.add_argument(
        "--timing-profile",
        default=DEFAULT_TIMING_PROFILE_NAME,
        choices=tuple(sorted(TIMING_PROFILES)),
    )
    parser.add_argument(
        "--runtime-profile",
        default=RUNTIME_PROFILE_SAFE,
        choices=(
            RUNTIME_PROFILE_SAFE,
            RUNTIME_PROFILE_PERSISTENT_ROBUST,
            RUNTIME_PROFILE_PERSISTENT_LATEST,
        ),
    )
    parser.add_argument("--expected-trigger-hz", type=float, default=1000.0)
    parser.add_argument("--reference-lines", type=int, default=128)
    parser.add_argument("--sample-lines", type=int, default=128)
    parser.add_argument(
        "--observable-mode",
        default=LINEAR_ABSORPTION_MAIN_ONLY,
        choices=(
            LINEAR_ABSORPTION_MAIN_ONLY,
            LINEAR_ABSORPTION_MAIN_REF_NORMALIZED,
        ),
    )
    parser.add_argument(
        "--use-reference-channel",
        action="store_true",
        help="Acquire the ref channel as well as main.",
    )
    parser.add_argument("--external-trigger", dest="external_trigger", action="store_true", default=True)
    parser.add_argument("--no-external-trigger", dest="external_trigger", action="store_false")
    parser.add_argument("--read-timeout-s", type=float, default=10.0)
    parser.add_argument("--ai-buffer-lines", type=int, default=256)
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
        "--wavelength-anchor",
        dest="wavelength_anchors",
        action="append",
        type=_parse_anchor,
        default=[],
        help="Manual wavelength anchor as PIXEL:WAVELENGTH_NM. Repeat as needed.",
    )
    parser.add_argument(
        "--wavelength-polynomial-order",
        type=int,
        default=None,
        help="Polynomial order for manual wavelength fit.",
    )
    parser.add_argument(
        "--wavelength-calibration-file",
        type=Path,
        default=None,
        help="Load an existing wavelength_calibration.json instead of fitting from anchors.",
    )
    parser.add_argument(
        "--noninteractive",
        action="store_true",
        help="Do not pause for user prompts between reference and sample captures.",
    )
    parser.add_argument(
        "--no-show-plot",
        dest="show_plot",
        action="store_false",
        default=True,
        help="Save the LAS overview PNG but do not display the plot window at the end.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate configuration and print the resolved run plan without acquiring data.",
    )
    return parser


def _maybe_pause(prompt: str, *, enabled: bool) -> None:
    if enabled:
        input(prompt)


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
        notes="manual anchors from linear_absorption CLI",
    )


def run_linear_absorption(args: argparse.Namespace) -> Path:
    run_dir = Path(args.output_root) / run_directory_name(
        "linear_absorption",
        label=args.label or args.sample_name,
    )
    store = ExperimentDatasetStore(run_dir)
    metadata = default_linear_absorption_metadata(args.sample_name)
    metadata.notes = args.notes
    metadata.provenance.operator = args.operator

    config = LinearAbsorptionRunConfig(
        runtime_profile=args.runtime_profile,
        expected_trigger_hz=args.expected_trigger_hz,
        sample_lines=args.sample_lines,
        reference_lines=args.reference_lines,
        use_reference_channel=args.use_reference_channel,
        observable_mode=args.observable_mode,
        ai_buffer_lines=args.ai_buffer_lines,
        timeout_s=args.read_timeout_s,
        pulse_grouping_size=args.pulse_grouping_size,
        save_subaverages=args.save_subaverages,
    )
    wavelength_calibration = _maybe_build_wavelength_calibration(args)
    controller = _build_controller(args)
    runner = LinearAbsorptionRunner(
        metadata=metadata,
        config=config,
        dataset_store=store,
        wavelength_calibration=wavelength_calibration,
    )

    if args.check:
        print("linear_absorption check passed.")
        print(f"Planned run directory: {run_dir}")
        print(
            f"runtime={config.runtime_profile}, timing={args.timing_profile}, "
            f"reference_lines={config.reference_lines}, sample_lines={config.sample_lines}, "
            f"ref_channel={config.use_reference_channel}, observable={config.observable_mode}, "
            f"wavelength_cal={'file' if args.wavelength_calibration_file else ('anchors' if args.wavelength_anchors else 'none')}, "
            f"show_plot={args.show_plot}"
        )
        print(controller.format_timing_diagnostics(config.expected_trigger_hz))
        return run_dir

    print(f"Run directory: {run_dir}")
    print(
        "Reference capture: sample out, pump blocked, desired steady-state condition."
    )
    _maybe_pause(
        "Prepare reference condition, then press Enter to acquire...",
        enabled=(not args.noninteractive),
    )
    reference_stack = runner.capture_reference(controller)
    print(f"Captured {reference_stack.n_lines} reference lines.")

    print("Sample capture: sample in, pump blocked, same alignment condition.")
    _maybe_pause(
        "Prepare sample condition, then press Enter to acquire...",
        enabled=(not args.noninteractive),
    )
    sample_stack = runner.capture_sample(controller)
    print(f"Captured {sample_stack.n_lines} sample lines.")

    result = runner.build_result(
        sample_stack=sample_stack,
        reference_stack=reference_stack,
    )

    runner.save_captured_stack("reference_stack", reference_stack)
    runner.save_captured_stack("sample_stack", sample_stack)
    runner.save_result(result)
    plot_path = save_linear_absorption_overview(
        run_dir,
        result,
        sample_name=args.sample_name,
        observable_mode=config.observable_mode,
        show=bool(args.show_plot),
    )
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

    print("Linear absorption run complete.")
    print("Saved:")
    print(f"  {run_dir / 'reference_stack.npz'}")
    print(f"  {run_dir / 'sample_stack.npz'}")
    print(f"  {run_dir / 'linear_absorption_result.npz'}")
    print(f"  {plot_path}")
    return run_dir


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_linear_absorption(args)


if __name__ == "__main__":
    main()
