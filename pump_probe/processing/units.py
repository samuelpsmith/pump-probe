"""Unit-conversion helpers for wavelength, wavenumber, and delay axes."""

from __future__ import annotations

import numpy as np


SPEED_OF_LIGHT_M_PER_S = 299_792_458.0
SPEED_OF_LIGHT_MM_PER_PS = SPEED_OF_LIGHT_M_PER_S * 1e-9


def wavelength_nm_to_wavenumber_cm_inv(wavelength_nm: np.ndarray) -> np.ndarray:
    """Convert wavelength in nm to wavenumber in cm^-1."""

    wl_nm = np.asarray(wavelength_nm, dtype=float)
    return 1.0e7 / wl_nm


def wavenumber_cm_inv_to_wavelength_nm(wavenumber_cm_inv: np.ndarray) -> np.ndarray:
    """Convert wavenumber in cm^-1 to wavelength in nm."""

    wn = np.asarray(wavenumber_cm_inv, dtype=float)
    return 1.0e7 / wn


def delay_ps_from_path_length_mm(path_length_mm: np.ndarray | float) -> np.ndarray:
    """Convert optical path-length change in mm to delay in ps."""

    return np.asarray(path_length_mm, dtype=float) / SPEED_OF_LIGHT_MM_PER_PS


def path_length_mm_from_delay_ps(delay_ps: np.ndarray | float) -> np.ndarray:
    """Convert optical delay in ps to path-length change in mm."""

    return np.asarray(delay_ps, dtype=float) * SPEED_OF_LIGHT_MM_PER_PS


def ps_per_mm_from_path_multiplier(path_multiplier: float) -> float:
    """Return delay conversion for a known optical path-length multiplier.

    ``path_multiplier`` is the ratio:

        optical path-length change / mechanical stage displacement

    For example:
    - simple retroreflector (double pass): approximately 2
    - more complex layouts may be 4, 6, 8, ...

    This keeps geometry assumptions explicit instead of hiding them inside the
    experiment runner.
    """

    return float(delay_ps_from_path_length_mm(float(path_multiplier)))
