"""Structured live CMOS/PDA tool.

This tool intentionally drives the same proven live/sweep engine used by
``DAQ_pda_hard.py`` so bench behavior stays identical while the surrounding
project migrates into the ``pump_probe`` package structure.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import time
from typing import Any

import numpy as np

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pump_probe.hardware.daq.hard_adapter import _load_legacy_hard_module


def _build_parser() -> argparse.ArgumentParser:
    """Reuse the legacy CLI surface and add a dry-run check mode."""
    module = _load_legacy_hard_module()
    parser = module._build_cli_parser()
    parser.description = (
        "Structured live CMOS/PDA tool. This wraps the proven DAQ_pda_hard "
        "live/sweep engine so behavior stays identical while the project "
        "moves into the pump_probe package."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Validate configuration, print timing/runtime diagnostics, and "
            "exit without opening acquisition tasks or moving into live/sweep."
        ),
    )
    parser.add_argument(
        "--integration-line-count",
        type=int,
        default=128,
        help=(
            "Number of accepted lines/pairs to average in the live integrated "
            "trace. Smaller responds faster; larger is smoother."
        ),
    )
    parser.add_argument(
        "--observable-mode",
        choices=("main_only", "main_ref_normalized"),
        default="main_only",
        help=(
            "Primary live observable. 'main_only' uses the main detector line. "
            "'main_ref_normalized' acquires both channels and uses main/ref as "
            "the derived referenced signal."
        ),
    )
    parser.add_argument(
        "--reference-divide-floor",
        type=float,
        default=1e-6,
        help=(
            "Minimum absolute reference-channel voltage used when forming "
            "main/ref to avoid divide blowups."
        ),
    )
    parser.add_argument(
        "--pump-chop-display",
        choices=("mod", "native"),
        default="mod",
        help=(
            "Display units for the live pump-chop panel. 'mod' shows Delta OD "
            "in mOD; 'native' shows the underlying demod value in volts for "
            "main-only or OD for referenced mode."
        ),
    )
    parser.add_argument(
        "--mod-min-light-voltage",
        type=float,
        default=0.025,
        help=(
            "Minimum dark-corrected light voltage needed for main-only mOD "
            "display normalization. Pixels below this are masked as invalid."
        ),
    )
    parser.add_argument(
        "--channel-dark-file",
        type=str,
        default=None,
        help=(
            "Optional dark file. In main-only mode this stores the intensity "
            "dark used only for mOD display normalization; in referenced mode "
            "it stores main/ref channel darks. If omitted, live_cmos uses "
            "acquisition_results/channel_dark_offset_latest.npz."
        ),
    )
    parser.add_argument(
        "--disable-channel-dark",
        dest="channel_dark_subtract",
        action="store_false",
        default=True,
        help=(
            "Disable intensity/channel dark use. In main-only mode this leaves "
            "the mOD denominator uncorrected; in referenced mode it disables "
            "main/ref channel-dark subtraction."
        ),
    )
    parser.add_argument(
        "--tail-sign-hack",
        dest="tail_sign_hack",
        action="store_true",
        default=True,
        help=(
            "Enable live-only tail-guided sign/baseline heuristic for the "
            "pump-chop trace using pixels 900:1000."
        ),
    )
    parser.add_argument(
        "--no-tail-sign-hack",
        dest="tail_sign_hack",
        action="store_false",
    )
    parser.add_argument("--tail-sign-start", type=int, default=900)
    parser.add_argument("--tail-sign-stop", type=int, default=1000)
    parser.add_argument(
        "--tail-sign-expected",
        type=float,
        default=1.0,
        help="Expected sign of the tail-region mean after correct phase assignment.",
    )
    parser.add_argument(
        "--tail-baseline-subtract",
        dest="tail_baseline_subtract",
        action="store_true",
        default=True,
        help="Zero the live tail-region mean after tail-guided sign inference.",
    )
    parser.add_argument(
        "--no-tail-baseline-subtract",
        dest="tail_baseline_subtract",
        action="store_false",
    )
    parser.add_argument(
        "--pump-chop-phase-source",
        choices=("inferred", "chopper_input"),
        default="inferred",
        help=(
            "How to assign pump-chop phase. 'inferred' uses the current "
            "software grouping logic; 'chopper_input' uses the live logic "
            "level on the configured chopper-input PFI terminal."
        ),
    )
    parser.add_argument(
        "--monitor-chopper-input",
        dest="monitor_chopper_input",
        action="store_true",
        default=False,
        help=(
            "Use a spare counter to monitor an external chopper FOUT TTL on "
            "the configured chopper-input PFI terminal and show its live rate "
            "in the lower diagnostics panel."
        ),
    )
    parser.add_argument(
        "--no-monitor-chopper-input",
        dest="monitor_chopper_input",
        action="store_false",
    )
    parser.add_argument(
        "--monitor-trigger-input",
        dest="monitor_trigger_input",
        action="store_true",
        default=True,
        help=(
            "Keep the existing PFI9 trigger-edge monitor enabled for the live "
            "rate panel and trigger-efficiency diagnostics."
        ),
    )
    parser.add_argument(
        "--no-monitor-trigger-input",
        dest="monitor_trigger_input",
        action="store_false",
    )
    parser.add_argument(
        "--chopper-input-pfi",
        type=str,
        default="PFI14",
        help=(
            "DAQ PFI terminal used for an external chopper FOUT TTL monitor. "
            "Default: PFI14."
        ),
    )
    parser.add_argument(
        "--chopper-input-counter",
        type=str,
        default="ctr2",
        help=(
            "Counter used for the external chopper input edge-rate monitor. "
            "Default: ctr2."
        ),
    )
    parser.add_argument(
        "--chopper-input-gate-ms",
        type=float,
        default=50.0,
        help=(
            "Gate time, in milliseconds, for the chopper-input edge-rate "
            "measurement. Longer is steadier; shorter responds faster."
        ),
    )
    return parser


def _configure_legacy_controller(args: argparse.Namespace, module: Any):
    """Build and configure the legacy PDA controller exactly like DAQ_pda_hard."""
    pda = module.PDAControllerDAQSimple(device="Dev1", num_pixels=1024)
    pda.enable_external_trigger(True)
    pda.set_chopper_input_terminal(in_pfi=str(args.chopper_input_pfi))

    trigger_filter_enable = False
    trigger_filter_min_pulse_width_s = 0.2e-6
    trigger_sync_enable = True
    pda.set_trigger_filter(
        enable=trigger_filter_enable,
        min_pulse_width_s=trigger_filter_min_pulse_width_s,
    )
    pda.set_trigger_sync(trigger_sync_enable)
    pda.set_retrigger_initial_delay(True)
    pda.set_chopper_sync_output(
        enable=True,
        out_pfi="PFI3",
        source_terminal=pda.trig_in,
        initial_delay_ticks=0,
        high_ticks=2,
        low_ticks=2,
    )

    pda.set_video_timing(
        dummy_clocks=14,
        pixel_clocks=pda.num_pixels,
        start_on_st_fall=True,
    )
    pda.set_output_cropping(False)

    timing_profile_name = args.timing_profile
    if (
        args.observable_mode == "main_ref_normalized"
        and timing_profile_name == module.PDAControllerDAQSimple.DEFAULT_TIMING_PROFILE
    ):
        timing_profile_name = "almost_full_2channel"
        print(
            "Info: observable_mode=main_ref_normalized requested; "
            "using timing profile 'almost_full_2channel'."
        )
    pda.apply_timing_profile(timing_profile_name)
    trigger_to_st_phase_shift_us = 0.0
    pda.set_trigger_phase_shift(trigger_to_st_phase_shift_us * 1e-6)
    return pda, timing_profile_name, trigger_to_st_phase_shift_us


def _build_live_cfg(args: argparse.Namespace, module: Any) -> dict[str, Any]:
    """Build the live configuration mapping exactly like DAQ_pda_hard.main()."""
    expected_trigger_hz = 1000.0
    acquisition_timeout_s = 50.0
    acquisition_runtime_profile = str(args.runtime_profile).strip().lower()
    runtime_settings = module._resolve_runtime_profile_settings(acquisition_runtime_profile)
    referenced_mode = args.observable_mode == "main_ref_normalized"
    tail_sign_hack_enabled = bool(args.tail_sign_hack)
    monitor_chopper_input = bool(args.monitor_chopper_input)
    monitor_trigger_input = bool(args.monitor_trigger_input) and not monitor_chopper_input
    phase_source = str(args.pump_chop_phase_source).strip().lower()
    if phase_source == "chopper_input":
        tail_sign_hack_enabled = False

    live_cfg: dict[str, Any] = {
        "integration_line_count": max(1, int(args.integration_line_count)),
        "plot_raw_line": True,
        "live_video_mode": ("both" if referenced_mode else "main"),
        "reference_processing_mode": (
            "ratio" if referenced_mode else "difference"
        ),
        "reference_ratio_floor": max(1e-12, float(args.reference_divide_floor)),
        "pump_chop_display_mode": args.pump_chop_display,
        "pump_chop_mod_min_light_v": max(
            1e-12,
            float(args.mod_min_light_voltage),
        ),
        "channel_dark_subtract": bool(args.channel_dark_subtract),
        "channel_dark_file": args.channel_dark_file,
        "pump_chop_demod": True,
        "pump_chop_sign": -1.0,
        "pump_chop_phase_source": phase_source,
        "pump_chop_use_adjacent_pairs": False,
        "pump_chop_sign_agnostic_preview": False,
        "pump_chop_tail_heuristic_enable": tail_sign_hack_enabled,
        "pump_chop_tail_heuristic_start": int(args.tail_sign_start),
        "pump_chop_tail_heuristic_stop": int(args.tail_sign_stop),
        "pump_chop_tail_heuristic_expected_sign": float(args.tail_sign_expected),
        "pump_chop_tail_heuristic_zero_baseline": bool(
            args.tail_baseline_subtract
        ),
        "pump_chop_dark_subtract": True,
        "expected_trigger_hz": expected_trigger_hz,
        "acquisition_timeout_s": acquisition_timeout_s,
        "monitor_pfi9": monitor_trigger_input,
        "pfi9_monitor_counter": "ctr2",
        "pfi9_rate_gate_s": 0.002,
        "monitor_chopper_input": monitor_chopper_input,
        "chopper_input_monitor_counter": str(args.chopper_input_counter),
        "chopper_input_rate_gate_s": max(
            1e-3,
            float(args.chopper_input_gate_ms) / 1000.0,
        ),
        "trigger_plot_history": 1200,
        "capture_hit_rate_enable": True,
        "capture_hit_rate_window_lines": 256,
        "capture_hit_threshold_fraction": 0.45,
        "capture_hit_warmup_lines": 64,
        "ordered_read_batch_lines": 16,
        "tdms_log_enable": False,
        "tdms_group_name": "PDA",
        "tdms_logging_mode": module.LoggingMode.LOG_AND_READ,
        "tdms_logging_operation": module.LoggingOperation.OPEN_OR_CREATE,
    }
    live_cfg.update(runtime_settings)
    live_cfg["persistent_ai_buffer_lines"] = max(
        int(live_cfg["persistent_ai_buffer_lines"]),
        2048,
    )
    if referenced_mode:
        live_cfg["plot_target_fps"] = min(
            float(live_cfg["plot_target_fps"]),
            15.0,
        )
        live_cfg["plot_update_every_n_lines"] = max(
            int(live_cfg["plot_update_every_n_lines"]),
            4,
        )
        live_cfg["timing_text_update_every_n_lines"] = max(
            int(live_cfg["timing_text_update_every_n_lines"]),
            80,
        )
    module._apply_live_preset(live_cfg, args.preset)

    if args.plot_fps is not None:
        live_cfg["plot_target_fps"] = max(1.0, float(args.plot_fps))
    if args.plot_every_lines is not None:
        live_cfg["plot_update_every_n_lines"] = max(1, int(args.plot_every_lines))
    if args.timing_text_every_lines is not None:
        live_cfg["timing_text_update_every_n_lines"] = max(
            1, int(args.timing_text_every_lines)
        )
    if args.autoscale_every_updates is not None:
        live_cfg["autoscale_every_n_plot_updates"] = max(
            1, int(args.autoscale_every_updates)
        )
    if args.reader_fifo_packets is not None:
        live_cfg["reader_fifo_max_packets"] = max(8, int(args.reader_fifo_packets))

    out_dir = Path(__file__).resolve().parents[2] / "acquisition_results"
    out_dir.mkdir(parents=True, exist_ok=True)
    live_cfg["tdms_file_path"] = str(
        out_dir / f"pda_retrigger_{time.strftime('%Y%m%d_%H%M%S')}.tdms"
    )
    return live_cfg


def _print_check_preview(
    *,
    pda,
    args: argparse.Namespace,
    live_cfg: dict[str, Any],
    timing_profile_name: str,
    trigger_to_st_phase_shift_us: float,
) -> None:
    """Print a hardware-free preview of the exact live/sweep configuration."""
    print("live_cmos check passed.")
    print(f"Operation: {args.operation}")
    print(f"Timing profile: {timing_profile_name}")
    print(f"Trigger->ST phase shift: {trigger_to_st_phase_shift_us:.1f} us")
    print(f"Runtime profile: {args.runtime_profile}")
    print(f"Preset: {args.preset}")
    print(
        "Monitor config: "
        f"trigger_input={live_cfg['monitor_pfi9']} "
        f"({live_cfg['pfi9_monitor_counter']}, gate={live_cfg['pfi9_rate_gate_s'] * 1e3:.1f} ms), "
        f"chopper_input={live_cfg['monitor_chopper_input']} "
        f"({args.chopper_input_pfi}, {live_cfg['chopper_input_monitor_counter']}, "
        f"gate={live_cfg['chopper_input_rate_gate_s'] * 1e3:.1f} ms)"
    )
    print(
        "Live config: "
        f"integration_line_count={live_cfg['integration_line_count']}, "
        f"observable_mode={args.observable_mode}, "
        f"mode={live_cfg['live_video_mode']}, "
        f"reference_processing_mode={live_cfg['reference_processing_mode']}, "
        f"pump_chop_display={live_cfg['pump_chop_display_mode']}, "
        f"mod_min_light_v={live_cfg['pump_chop_mod_min_light_v']:.4g}, "
        f"channel_dark_subtract={live_cfg['channel_dark_subtract']}, "
        f"pump_chop_demod={live_cfg['pump_chop_demod']}, "
        f"pump_chop_sign={live_cfg['pump_chop_sign']:+.0f}, "
        f"pump_chop_phase_source={live_cfg['pump_chop_phase_source']}, "
        f"tail_sign_hack={live_cfg['pump_chop_tail_heuristic_enable']}, "
        f"tail_window={live_cfg['pump_chop_tail_heuristic_start']}:"
        f"{live_cfg['pump_chop_tail_heuristic_stop']}, "
        f"tail_expected={live_cfg['pump_chop_tail_heuristic_expected_sign']:+.0f}, "
        f"plot_fps={live_cfg['plot_target_fps']:.1f}, "
        f"plot_every_lines={live_cfg['plot_update_every_n_lines']}, "
        f"timing_text_every_lines={live_cfg['timing_text_update_every_n_lines']}, "
        f"autoscale_every_updates={live_cfg['autoscale_every_n_plot_updates']}"
    )
    print(
        "Persistent config: "
        f"use_persistent_session={live_cfg['use_persistent_session']}, "
        f"latest_only={live_cfg['retrigger_latest_only_read']}, "
        f"overwrite_unread={live_cfg['retrigger_overwrite_unread']}, "
        f"decouple_acquisition_from_plot={live_cfg['decouple_acquisition_from_plot']}, "
        f"ordered_read_batch_lines={live_cfg.get('ordered_read_batch_lines', 1)}, "
        f"reader_fifo_max_packets={live_cfg['reader_fifo_max_packets']}, "
        f"persistent_ai_buffer_lines={live_cfg['persistent_ai_buffer_lines']}"
    )
    print("Timing diagnostics:")
    print(pda.format_timing_diagnostics(trigger_frequency_hz=live_cfg["expected_trigger_hz"]))
    if (
        live_cfg["pump_chop_tail_heuristic_enable"]
        and int(getattr(pda, "ai_sample_clock_divisor", 1)) > 1
    ):
        divisor = float(getattr(pda, "ai_sample_clock_divisor", 1))
        original_start = int(live_cfg["pump_chop_tail_heuristic_start"])
        original_stop = int(live_cfg["pump_chop_tail_heuristic_stop"])
        mapped_start = int(np.floor(original_start / divisor))
        mapped_stop = int(np.ceil(original_stop / divisor))
        mapped_start = max(
            0,
            min(int(pda.output_samples_per_line) - 1, mapped_start),
        )
        mapped_stop = max(
            mapped_start + 1,
            min(int(pda.output_samples_per_line), mapped_stop),
        )
        print(
            "Tail window during acquisition: "
            f"detector-equiv [{original_start}:{original_stop}) -> "
            f"acquired [{mapped_start}:{mapped_stop})."
        )
    if args.operation == "sweep":
        print(
            "Sweep preview: "
            f"eval={args.sweep_evaluation_seconds:.1f}s, "
            f"phase coarse={args.sweep_phase_coarse_start_us:.1f}:{args.sweep_phase_coarse_stop_us:.1f}:"
            f"{args.sweep_phase_coarse_step_us:.1f} us, "
            f"phase fine half-width={args.sweep_phase_fine_half_width_us:.1f} us, "
            f"ST delay half-width={args.sweep_st_delay_half_width_us:.1f} us"
        )


def run_live_cmos(argv: list[str] | None = None) -> None:
    """Run the structured live tool with DAQ_pda_hard-equivalent behavior."""
    module = _load_legacy_hard_module()
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.list_timing_profiles:
        print("Available timing profiles:")
        for name in sorted(module.PDAControllerDAQSimple.TIMING_PROFILES.keys()):
            desc = module.PDAControllerDAQSimple.TIMING_PROFILES[name].get("description", "")
            print(f"  {name}: {desc}")
        raise SystemExit(0)

    pda, timing_profile_name, trigger_to_st_phase_shift_us = _configure_legacy_controller(
        args, module
    )
    live_cfg = _build_live_cfg(args, module)

    if args.check:
        _print_check_preview(
            pda=pda,
            args=args,
            live_cfg=live_cfg,
            timing_profile_name=timing_profile_name,
            trigger_to_st_phase_shift_us=trigger_to_st_phase_shift_us,
        )
        return

    if args.operation == "sweep":
        print("\n=== Persistent Sweep Mode ===")
        print(
            "Sweep enforces robust persistent settings: "
            "external trigger on, main-only, demod off, "
            "latest_only=False, overwrite_unread=False."
        )
        live_cfg.update(
            {
                "live_video_mode": "main",
                "pump_chop_demod": False,
                "demod_trigger_qualified_acceptance": False,
                "use_persistent_session": True,
                "retrigger_latest_only_read": False,
                "retrigger_overwrite_unread": False,
                "decouple_acquisition_from_plot": True,
                "tdms_log_enable": False,
            }
        )

        module.run_persistent_timing_sweep(
            pda=pda,
            expected_trigger_hz=live_cfg["expected_trigger_hz"],
            evaluation_seconds=args.sweep_evaluation_seconds,
            acquisition_timeout_s=live_cfg["acquisition_timeout_s"],
            monitor_pfi9=live_cfg["monitor_pfi9"],
            pfi9_monitor_counter=live_cfg["pfi9_monitor_counter"],
            pfi9_rate_gate_s=live_cfg["pfi9_rate_gate_s"],
            phase_coarse_start_us=args.sweep_phase_coarse_start_us,
            phase_coarse_stop_us=args.sweep_phase_coarse_stop_us,
            phase_coarse_step_us=args.sweep_phase_coarse_step_us,
            phase_fine_half_width_us=args.sweep_phase_fine_half_width_us,
            phase_fine_step_us=args.sweep_phase_fine_step_us,
            st_delay_half_width_us=args.sweep_st_delay_half_width_us,
            st_delay_step_us=args.sweep_st_delay_step_us,
            ai_buffer_lines=args.sweep_ai_buffer_lines,
            queue_abort_fraction=args.sweep_queue_abort_fraction,
        )
        if not args.sweep_then_live:
            return
        print("\nSweep finished. Entering live mode with best settings...\n")

    module.run_live_plot(pda=pda, **live_cfg)


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint."""
    run_live_cmos(argv)


if __name__ == "__main__":
    main()
