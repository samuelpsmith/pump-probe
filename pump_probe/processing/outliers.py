"""Outlier-detection helpers for line-stack and grouped reductions."""

from __future__ import annotations

import numpy as np


def integrated_area_per_line(spectra: np.ndarray) -> np.ndarray:
    """Compute simple integrated area for each spectrum in a stack."""

    arr = np.asarray(spectra, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"Expected (n_lines, n_pixels) array, got shape {arr.shape}.")
    return np.sum(arr, axis=1)


def mad_zscores(values: np.ndarray, *, floor: float = 1e-12) -> np.ndarray:
    """Return robust z-scores based on median absolute deviation."""

    arr = np.asarray(values, dtype=float)
    median = np.median(arr)
    mad = np.median(np.abs(arr - median))
    scale = max(float(mad), float(floor))
    return 0.6744897501960817 * (arr - median) / scale


def mad_inlier_mask(values: np.ndarray, *, threshold: float = 5.0) -> np.ndarray:
    """Flag inliers using a MAD-based z-score threshold."""

    return np.abs(mad_zscores(values)) <= float(threshold)
