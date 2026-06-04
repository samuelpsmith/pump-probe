"""Pump-chop demodulation helpers shared by live tools and TAS."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import warnings

import numpy as np


DEMOD_MODE_FIXED_ODD_EVEN = "fixed_odd_even"
DEMOD_MODE_TAIL_GUIDED_PAIRS = "tail_guided_pairs"
DEMOD_MODE_CHOICES = (
    DEMOD_MODE_FIXED_ODD_EVEN,
    DEMOD_MODE_TAIL_GUIDED_PAIRS,
)


@dataclass(slots=True)
class ChannelDarkCorrection:
    """Detector intensity/channel dark offsets in voltage units."""

    main: np.ndarray
    reference: np.ndarray | None = None
    label: str | None = None
    source_path: str | None = None


@dataclass(slots=True)
class ChopDemodConfig:
    """Configuration for pump-chop demodulation."""

    mode: str = DEMOD_MODE_TAIL_GUIDED_PAIRS
    pump_chop_sign: float = -1.0
    use_reference_channel: bool = False
    tail_start: int = 900
    tail_stop: int = 1000
    tail_expected_sign: float = 1.0
    tail_baseline_subtract: bool = True
    min_light_voltage: float = 0.025
    divide_floor: float = 1e-12
    dark_correction: ChannelDarkCorrection | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ChopDemodResult:
    """Processed pump-chop result plus diagnostics."""

    pumped_signal: np.ndarray
    unpumped_signal: np.ndarray
    delta_t_over_t: np.ndarray
    delta_od: np.ndarray
    accepted_lines: int
    accepted_pairs: int
    invalid_pixel_mask: np.ndarray
    pair_delta_od_mean: np.ndarray | None = None
    pair_delta_od_sem: np.ndarray | None = None
    tail_flip_count: int = 0
    tail_means: np.ndarray = field(default_factory=lambda: np.array([], dtype=float))
    pair_count: int = 0
    mode: str = DEMOD_MODE_TAIL_GUIDED_PAIRS
    metadata: dict[str, Any] = field(default_factory=dict)


def load_channel_dark_npz(path: str | Path) -> ChannelDarkCorrection:
    """Load the live-view intensity/channel dark NPZ format."""

    dark_path = Path(path).expanduser()
    with np.load(dark_path, allow_pickle=False) as npz_file:
        if "main_integrated" not in npz_file:
            raise KeyError(f"main_integrated not present in dark file: {dark_path}")
        main = np.asarray(npz_file["main_integrated"], dtype=float)
        reference = (
            np.asarray(npz_file["reference_integrated"], dtype=float)
            if "reference_integrated" in npz_file
            else None
        )
        label = (
            str(npz_file["dark_kind"][0])
            if "dark_kind" in npz_file and np.asarray(npz_file["dark_kind"]).size
            else dark_path.stem
        )
    return ChannelDarkCorrection(
        main=main,
        reference=reference,
        label=label,
        source_path=str(dark_path),
    )


def _as_2d(name: str, value: np.ndarray) -> np.ndarray:
    arr = np.asarray(value, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"{name} must have shape (n_lines, n_pixels), got {arr.shape}.")
    return arr


def _validate_config(config: ChopDemodConfig) -> str:
    mode = str(config.mode).strip().lower()
    if mode not in DEMOD_MODE_CHOICES:
        raise ValueError(
            f"Unsupported demod mode {config.mode!r}; expected one of {DEMOD_MODE_CHOICES}."
        )
    if float(config.pump_chop_sign) == 0.0:
        raise ValueError("pump_chop_sign must be non-zero.")
    return mode


def _coerce_dark_shape(name: str, dark: np.ndarray | None, n_pixels: int) -> np.ndarray | None:
    if dark is None:
        return None
    arr = np.asarray(dark, dtype=float)
    if arr.size == int(n_pixels):
        return arr
    if arr.size > int(n_pixels):
        # Live full-window darks include leading dummy-clock samples; cropped
        # experiment stacks keep the trailing valid-pixel region.
        return arr[-int(n_pixels):]
    raise ValueError(f"{name} dark size mismatch: {arr.size} != {n_pixels}.")


def _correct_main(lines: np.ndarray, dark: ChannelDarkCorrection | None) -> np.ndarray:
    if dark is None:
        return np.asarray(lines, dtype=float)
    dark_main = _coerce_dark_shape("main", dark.main, lines.shape[1])
    return np.asarray(lines, dtype=float) - dark_main


def _correct_reference(lines: np.ndarray, dark: ChannelDarkCorrection | None) -> np.ndarray:
    if dark is None or dark.reference is None:
        return np.asarray(lines, dtype=float)
    dark_ref = _coerce_dark_shape("reference", dark.reference, lines.shape[1])
    return np.asarray(lines, dtype=float) - dark_ref


def _line_observable(
    main_lines: np.ndarray,
    ref_lines: np.ndarray | None,
    config: ChopDemodConfig,
) -> tuple[np.ndarray, np.ndarray]:
    """Return raw lines for pair differences plus corrected observable lines."""

    main = _as_2d("main_lines", main_lines)
    dark = config.dark_correction
    if config.use_reference_channel:
        if ref_lines is None:
            raise ValueError("Reference demod requested but ref_lines is None.")
        if dark is not None and dark.reference is None:
            raise ValueError(
                "Reference demod needs a dark file containing reference_integrated."
            )
        ref = _as_2d("ref_lines", ref_lines)
        if ref.shape != main.shape:
            raise ValueError(f"ref_lines shape {ref.shape} does not match {main.shape}.")
        corrected_main = _correct_main(main, dark)
        corrected_ref = _correct_reference(ref, dark)
        safe_ref = np.where(
            np.abs(corrected_ref) < config.divide_floor,
            np.nan,
            corrected_ref,
        )
        observable = corrected_main / safe_ref
        return observable, observable

    corrected_main = _correct_main(main, dark)
    return main, corrected_main


def _safe_delta(
    pumped: np.ndarray,
    unpumped: np.ndarray,
    *,
    min_light_voltage: float,
    divide_floor: float,
    require_light_floor: bool,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    pumped = np.asarray(pumped, dtype=float)
    unpumped = np.asarray(unpumped, dtype=float)
    valid = (
        np.isfinite(pumped)
        & np.isfinite(unpumped)
        & (np.abs(pumped) > divide_floor)
        & (np.abs(unpumped) > divide_floor)
        & ((pumped * unpumped) > 0.0)
    )
    if require_light_floor:
        valid &= (
            np.abs(pumped) >= min_light_voltage
        ) & (
            np.abs(unpumped) >= min_light_voltage
        )

    ratio = np.full(pumped.shape, np.nan, dtype=float)
    np.divide(pumped, unpumped, out=ratio, where=valid)
    valid &= np.isfinite(ratio) & (ratio > 0.0)

    delta_od = np.full(pumped.shape, np.nan, dtype=float)
    delta_t = np.full(pumped.shape, np.nan, dtype=float)
    delta_od[valid] = -np.log10(ratio[valid])
    delta_t[valid] = ratio[valid] - 1.0
    invalid = ~valid
    return delta_t, delta_od, invalid


def _tail_guided_delta(raw_delta: np.ndarray, config: ChopDemodConfig) -> tuple[np.ndarray, float, bool]:
    pair = np.asarray(raw_delta, dtype=float).copy()
    start = max(0, min(pair.size, int(config.tail_start)))
    stop = max(start + 1, min(pair.size, int(config.tail_stop)))
    tail = pair[start:stop]
    tail_mean = float(np.nanmean(tail)) if tail.size else float("nan")
    flipped = False
    if np.isfinite(tail_mean):
        expected = float(config.tail_expected_sign)
        if expected != 0.0 and tail_mean * expected < 0.0:
            pair = -pair
            tail_mean = -tail_mean
            flipped = True
        if config.tail_baseline_subtract:
            pair = pair - tail_mean
    pair = float(config.pump_chop_sign) * pair
    return pair, tail_mean, flipped


def _nansem(values: np.ndarray, axis: int = 0) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    count = np.sum(np.isfinite(arr), axis=axis)
    std = np.nanstd(arr, axis=axis, ddof=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        sem = std / np.sqrt(count)
    return np.where(count > 1, sem, np.zeros_like(sem, dtype=float))


def _nanmean(values: np.ndarray, axis: int = 0) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanmean(values, axis=axis)


def demodulate_chop_stack(
    main_lines: np.ndarray,
    ref_lines: np.ndarray | None = None,
    *,
    config: ChopDemodConfig,
) -> ChopDemodResult:
    """Demodulate one stack of lines into pumped/unpumped and delta OD spectra."""

    mode = _validate_config(config)
    main = _as_2d("main_lines", main_lines)
    raw_for_delta, corrected_observable = _line_observable(main, ref_lines, config)
    require_light_floor = not bool(config.use_reference_channel)
    min_light = max(float(config.min_light_voltage), float(config.divide_floor))

    if mode == DEMOD_MODE_FIXED_ODD_EVEN:
        phase0 = corrected_observable[0::2]
        phase1 = corrected_observable[1::2]
        if phase0.size == 0 or phase1.size == 0:
            raise ValueError("Need at least one phase0 and one phase1 line for demod.")
        phase0_mean = _nanmean(phase0, axis=0)
        phase1_mean = _nanmean(phase1, axis=0)
        if float(config.pump_chop_sign) >= 0.0:
            pumped = phase0_mean
            unpumped = phase1_mean
        else:
            pumped = phase1_mean
            unpumped = phase0_mean
        delta_t, delta_od, invalid = _safe_delta(
            pumped,
            unpumped,
            min_light_voltage=min_light,
            divide_floor=float(config.divide_floor),
            require_light_floor=require_light_floor,
        )
        return ChopDemodResult(
            pumped_signal=pumped,
            unpumped_signal=unpumped,
            delta_t_over_t=delta_t,
            delta_od=delta_od,
            accepted_lines=int(main.shape[0]),
            accepted_pairs=int(min(phase0.shape[0], phase1.shape[0])),
            invalid_pixel_mask=invalid,
            pair_count=int(min(phase0.shape[0], phase1.shape[0])),
            mode=mode,
            metadata=dict(config.metadata),
        )

    if main.shape[0] < 2:
        raise ValueError("Need at least two lines for tail-guided adjacent-pair demod.")

    pumped_pairs: list[np.ndarray] = []
    unpumped_pairs: list[np.ndarray] = []
    pair_delta_od: list[np.ndarray] = []
    tail_means: list[float] = []
    tail_flip_count = 0

    for idx in range(1, raw_for_delta.shape[0]):
        raw_delta = raw_for_delta[idx] - raw_for_delta[idx - 1]
        oriented_delta, tail_mean, flipped = _tail_guided_delta(raw_delta, config)
        if flipped:
            tail_flip_count += 1
        tail_means.append(tail_mean)

        baseline = 0.5 * (corrected_observable[idx] + corrected_observable[idx - 1])
        pumped = baseline + 0.5 * oriented_delta
        unpumped = baseline - 0.5 * oriented_delta
        delta_t, delta_od, _ = _safe_delta(
            pumped,
            unpumped,
            min_light_voltage=min_light,
            divide_floor=float(config.divide_floor),
            require_light_floor=require_light_floor,
        )
        pumped_pairs.append(pumped)
        unpumped_pairs.append(unpumped)
        pair_delta_od.append(delta_od)

    pumped_stack = np.stack(pumped_pairs, axis=0)
    unpumped_stack = np.stack(unpumped_pairs, axis=0)
    pumped_mean = _nanmean(pumped_stack, axis=0)
    unpumped_mean = _nanmean(unpumped_stack, axis=0)
    delta_t, delta_od, invalid = _safe_delta(
        pumped_mean,
        unpumped_mean,
        min_light_voltage=min_light,
        divide_floor=float(config.divide_floor),
        require_light_floor=require_light_floor,
    )
    pair_delta_od_stack = np.stack(pair_delta_od, axis=0)

    return ChopDemodResult(
        pumped_signal=pumped_mean,
        unpumped_signal=unpumped_mean,
        delta_t_over_t=delta_t,
        delta_od=delta_od,
        accepted_lines=int(main.shape[0]),
        accepted_pairs=int(pair_delta_od_stack.shape[0]),
        invalid_pixel_mask=invalid,
        pair_delta_od_mean=_nanmean(pair_delta_od_stack, axis=0),
        pair_delta_od_sem=_nansem(pair_delta_od_stack, axis=0),
        tail_flip_count=int(tail_flip_count),
        tail_means=np.asarray(tail_means, dtype=float),
        pair_count=int(pair_delta_od_stack.shape[0]),
        mode=mode,
        metadata=dict(config.metadata),
    )


def target_lines_for_pairs(mode: str, pairs: int) -> int:
    """Minimum line count needed to produce the requested pair count."""

    pairs = max(1, int(pairs))
    mode_key = str(mode).strip().lower()
    if mode_key == DEMOD_MODE_TAIL_GUIDED_PAIRS:
        return pairs + 1
    return 2 * pairs
