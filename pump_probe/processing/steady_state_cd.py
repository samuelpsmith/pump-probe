"""Steady-state circular dichroism processing helpers."""

from __future__ import annotations

import numpy as np

from pump_probe.processing.transient_absorption import odd_even_split


def circular_difference(
    left_signal: np.ndarray,
    right_signal: np.ndarray,
) -> np.ndarray:
    """Simple left-minus-right differential signal."""

    left = np.asarray(left_signal, dtype=float)
    right = np.asarray(right_signal, dtype=float)
    if left.shape != right.shape:
        raise ValueError(
            f"Left and right spectra must share shape; got {left.shape} and {right.shape}."
        )
    return left - right


def normalized_circular_difference(
    left_signal: np.ndarray,
    right_signal: np.ndarray,
    *,
    floor: float = 1e-12,
) -> np.ndarray:
    """Return a normalized CD-like contrast signal."""

    left = np.asarray(left_signal, dtype=float)
    right = np.asarray(right_signal, dtype=float)
    denom = np.clip(left + right, floor, None)
    return (left - right) / denom


def odd_even_circular_components(
    spectra: np.ndarray,
    *,
    sign: float = 1.0,
) -> tuple[np.ndarray, np.ndarray]:
    """Split odd/even modulation buckets into nominal left/right components."""

    odd_bucket, even_bucket = odd_even_split(spectra)
    odd_mean = np.mean(odd_bucket, axis=0)
    even_mean = np.mean(even_bucket, axis=0)
    if sign >= 0:
        return odd_mean, even_mean
    return even_mean, odd_mean
