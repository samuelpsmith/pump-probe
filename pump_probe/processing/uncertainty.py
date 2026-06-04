"""Uncertainty-analysis helpers for grouped spectral reductions."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(slots=True)
class GroupedSpectrumSummary:
    """Grouped sub-average summary for a stack of spectra."""

    group_means: np.ndarray
    group_counts: np.ndarray
    overall_mean: np.ndarray
    overall_std: np.ndarray
    overall_sem: np.ndarray
    group_size: int


def group_spectra(
    spectra: np.ndarray,
    group_size: int,
    *,
    drop_remainder: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    """Average spectra in sequential groups."""

    arr = np.asarray(spectra, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"Expected (n_lines, n_pixels) array, got shape {arr.shape}.")
    size = max(1, int(group_size))
    if size == 1:
        counts = np.ones(arr.shape[0], dtype=int)
        return arr.copy(), counts

    groups: list[np.ndarray] = []
    counts: list[int] = []
    for start in range(0, arr.shape[0], size):
        stop = min(arr.shape[0], start + size)
        block = arr[start:stop]
        if drop_remainder and block.shape[0] < size:
            continue
        groups.append(np.mean(block, axis=0))
        counts.append(block.shape[0])

    if not groups:
        raise ValueError("No groups were produced; adjust group_size or drop_remainder.")
    return np.stack(groups, axis=0), np.asarray(counts, dtype=int)


def summarize_grouped_spectra(
    spectra: np.ndarray,
    group_size: int,
    *,
    drop_remainder: bool = False,
) -> GroupedSpectrumSummary:
    """Return grouped means plus uncertainty summaries over those groups."""

    group_means, group_counts = group_spectra(
        spectra,
        group_size,
        drop_remainder=drop_remainder,
    )
    if group_means.shape[0] > 1:
        overall_std = np.std(group_means, axis=0, ddof=1)
        overall_sem = overall_std / np.sqrt(group_means.shape[0])
    else:
        overall_std = np.zeros_like(group_means[0])
        overall_sem = np.zeros_like(group_means[0])
    return GroupedSpectrumSummary(
        group_means=group_means,
        group_counts=group_counts,
        overall_mean=np.mean(group_means, axis=0),
        overall_std=overall_std,
        overall_sem=overall_sem,
        group_size=max(1, int(group_size)),
    )
