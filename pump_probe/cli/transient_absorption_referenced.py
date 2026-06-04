"""Referenced transient absorption convenience entrypoint."""

from __future__ import annotations

import argparse
from pathlib import Path
import warnings

import numpy as np

from pump_probe.cli.transient_absorption import (
    _build_parser,
    _load_config_defaults,
    run_transient_absorption,
)


def _existing_live_artifact(name: str) -> Path | None:
    path = Path(__file__).resolve().parents[2] / "acquisition_results" / name
    return path if path.exists() else None


def _compatible_live_reference_dark() -> Path | None:
    path = _existing_live_artifact("channel_dark_offset_latest.npz")
    if path is None:
        return None
    try:
        with np.load(path, allow_pickle=False) as npz_file:
            if "reference_integrated" in npz_file:
                return path
            warnings.warn(
                "Referenced TAS wrapper skipped auto-loading "
                f"{path.name} because it does not contain reference_integrated. "
                "Collect a new referenced channel dark from live_cmos "
                "(main_ref_normalized, then press 'c') if you want channel-dark correction.",
                stacklevel=2,
            )
    except Exception as exc:
        warnings.warn(
            "Referenced TAS wrapper could not inspect "
            f"{path.name}; skipping auto dark-load. Detail: {exc}",
            stacklevel=2,
        )
    return None


def _referenced_defaults(config_path: Path | None) -> dict[str, object]:
    defaults = _load_config_defaults(None if config_path is None else str(config_path))
    defaults.setdefault("use_reference_channel", True)
    defaults.setdefault("timing_profile", "almost_full_2channel")
    defaults.setdefault(
        "intensity_dark_file",
        _compatible_live_reference_dark(),
    )
    defaults.setdefault(
        "pump_dark_file",
        _existing_live_artifact("pump_chop_dark_offset_latest.npz"),
    )
    return defaults


def main(argv: list[str] | None = None) -> None:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", type=Path, default=None)
    pre_args, _ = pre_parser.parse_known_args(argv)
    defaults = _referenced_defaults(pre_args.config)
    parser = _build_parser(defaults)
    parser.description = (
        "Acquire referenced transient absorption (main/ref normalized) using "
        "the shared TAS runner. This convenience entrypoint defaults to the "
        "current two-channel timing profile and reuses live_cmos dark/baseline "
        "artifacts when they are available."
    )
    args = parser.parse_args(argv)
    args.use_reference_channel = True
    if args.timing_profile == "guard_10us":
        args.timing_profile = "almost_full_2channel"
    run_transient_absorption(args)


if __name__ == "__main__":
    main()
