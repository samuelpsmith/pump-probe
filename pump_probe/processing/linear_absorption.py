"""Linear absorption processing helpers."""

from __future__ import annotations

import numpy as np


def transmission_spectrum(
    sample_signal: np.ndarray,
    reference_signal: np.ndarray,
    *,
    floor: float = 1e-12,
) -> np.ndarray:
    """Compute transmission from sample/reference spectra."""

    sample = np.asarray(sample_signal, dtype=float)
    reference = np.asarray(reference_signal, dtype=float)
    if sample.shape != reference.shape:
        raise ValueError(
            f"Sample and reference spectra must share shape; got {sample.shape} and {reference.shape}."
        )
    return sample / np.clip(reference, floor, None)


def absorbance_spectrum_from_transmission(
    transmission: np.ndarray,
    *,
    floor: float = 1e-12,
) -> np.ndarray:
    """Convert transmission to absorbance / optical density."""

    tr = np.asarray(transmission, dtype=float)
    return -np.log10(np.clip(tr, floor, None))


def absorbance_spectrum(
    sample_signal: np.ndarray,
    reference_signal: np.ndarray,
    *,
    floor: float = 1e-12,
) -> np.ndarray:
    """Compute absorbance directly from sample and reference spectra."""

    transmission = transmission_spectrum(
        sample_signal,
        reference_signal,
        floor=floor,
    )
    return absorbance_spectrum_from_transmission(transmission, floor=floor)
