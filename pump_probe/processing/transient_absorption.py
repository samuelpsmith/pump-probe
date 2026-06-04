"""Transient absorption processing helpers."""

from __future__ import annotations

import numpy as np


def delta_transmission(
    pumped_signal: np.ndarray,
    unpumped_signal: np.ndarray,
    *,
    floor: float = 1e-12,
) -> np.ndarray:
    """Compute fractional transmission change (pumped - unpumped) / unpumped."""

    pumped = np.asarray(pumped_signal, dtype=float)
    unpumped = np.asarray(unpumped_signal, dtype=float)
    if pumped.shape != unpumped.shape:
        raise ValueError(
            f"Pumped and unpumped spectra must share shape; got {pumped.shape} and {unpumped.shape}."
        )
    return (pumped - unpumped) / np.clip(unpumped, floor, None)


def delta_optical_density(
    pumped_signal: np.ndarray,
    unpumped_signal: np.ndarray,
    *,
    floor: float = 1e-12,
) -> np.ndarray:
    """Compute transient absorption in delta optical density units."""

    pumped = np.asarray(pumped_signal, dtype=float)
    unpumped = np.asarray(unpumped_signal, dtype=float)
    if pumped.shape != unpumped.shape:
        raise ValueError(
            f"Pumped and unpumped spectra must share shape; got {pumped.shape} and {unpumped.shape}."
        )
    return -np.log10(
        np.clip(pumped, floor, None) / np.clip(unpumped, floor, None)
    )


def odd_even_split(spectra: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Split a stack of spectra into odd/even buckets for modulation demod."""

    arr = np.asarray(spectra, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"Expected (n_lines, n_pixels) array, got shape {arr.shape}.")
    return arr[0::2], arr[1::2]
