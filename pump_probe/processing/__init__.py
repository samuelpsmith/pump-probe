"""Signal-processing utilities for spectroscopy experiments."""

from pump_probe.processing.averaging import RollingSpectrumAverage
from pump_probe.processing.linear_absorption import (
    absorbance_spectrum,
    absorbance_spectrum_from_transmission,
    transmission_spectrum,
)
from pump_probe.processing.outliers import (
    integrated_area_per_line,
    mad_inlier_mask,
    mad_zscores,
)
from pump_probe.processing.steady_state_cd import (
    circular_difference,
    normalized_circular_difference,
    odd_even_circular_components,
)
from pump_probe.processing.transient_absorption import (
    delta_optical_density,
    delta_transmission,
    odd_even_split,
)
from pump_probe.processing.uncertainty import (
    GroupedSpectrumSummary,
    group_spectra,
    summarize_grouped_spectra,
)
from pump_probe.processing.units import (
    SPEED_OF_LIGHT_MM_PER_PS,
    path_length_mm_from_delay_ps,
    ps_per_mm_from_path_multiplier,
    wavelength_nm_to_wavenumber_cm_inv,
    wavenumber_cm_inv_to_wavelength_nm,
)

__all__ = [
    "RollingSpectrumAverage",
    "SPEED_OF_LIGHT_MM_PER_PS",
    "GroupedSpectrumSummary",
    "absorbance_spectrum",
    "absorbance_spectrum_from_transmission",
    "circular_difference",
    "delta_optical_density",
    "delta_transmission",
    "group_spectra",
    "integrated_area_per_line",
    "mad_inlier_mask",
    "mad_zscores",
    "normalized_circular_difference",
    "odd_even_split",
    "odd_even_circular_components",
    "path_length_mm_from_delay_ps",
    "ps_per_mm_from_path_multiplier",
    "summarize_grouped_spectra",
    "transmission_spectrum",
    "wavelength_nm_to_wavenumber_cm_inv",
    "wavenumber_cm_inv_to_wavelength_nm",
]
