"""User-facing plotting/export helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import numpy as np

if TYPE_CHECKING:
    from pump_probe.experiments.linear_absorption import LinearAbsorptionResult


def _linear_absorption_x_axis(result: "LinearAbsorptionResult") -> tuple[np.ndarray, str]:
    """Pick the best available x-axis for linear absorption plotting."""
    if result.wavelength_nm is not None and len(result.wavelength_nm) > 0:
        return np.asarray(result.wavelength_nm, dtype=float), "Wavelength (nm)"
    if result.wavenumber_cm_inv is not None and len(result.wavenumber_cm_inv) > 0:
        return np.asarray(result.wavenumber_cm_inv, dtype=float), "Wavenumber (cm^-1)"
    return np.arange(result.absorbance.size, dtype=float), "Sample Index"


def save_linear_absorption_overview(
    run_dir: Path,
    result: "LinearAbsorptionResult",
    *,
    sample_name: str,
    observable_mode: str,
    show: bool = True,
) -> Path:
    """Save and optionally display a compact linear absorption summary plot."""
    x, xlabel = _linear_absorption_x_axis(result)

    if (
        observable_mode == "main_ref_normalized"
        and result.sample_spectrum.signal_ref is not None
        and result.reference_spectrum.signal_ref is not None
    ):
        sample_display = result.sample_spectrum.signal_main / np.clip(
            result.sample_spectrum.signal_ref,
            1e-12,
            None,
        )
        reference_display = result.reference_spectrum.signal_main / np.clip(
            result.reference_spectrum.signal_ref,
            1e-12,
            None,
        )
        top_ylabel = "Main/Ref (a.u.)"
        top_title = "Sample / Reference (normalized)"
    else:
        sample_display = result.sample_spectrum.signal_main
        reference_display = result.reference_spectrum.signal_main
        top_ylabel = "Signal (V)"
        top_title = "Sample / Reference"

    fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
    ax_signal, ax_transmission, ax_absorbance = axes

    ax_signal.plot(x, reference_display, lw=1.3, label="Reference")
    ax_signal.plot(x, sample_display, lw=1.3, label="Sample")
    ax_signal.set_ylabel(top_ylabel)
    ax_signal.legend(loc="best")
    ax_signal.grid(True, alpha=0.3)

    ax_transmission.plot(x, result.transmission, lw=1.4, color="tab:green")
    ax_transmission.set_ylabel("Transmission")
    ax_transmission.set_title("Transmission")
    ax_transmission.grid(True, alpha=0.3)

    ax_absorbance.plot(x, result.absorbance, lw=1.5, color="tab:blue")
    ax_absorbance.set_ylabel("Absorbance / OD")
    ax_absorbance.set_xlabel(xlabel)
    ax_absorbance.set_title("Linear Absorption Spectrum")
    ax_absorbance.grid(True, alpha=0.3)

    ax_signal.set_title(f"Linear Absorption: {sample_name} | {top_title}")
    fig.tight_layout()
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / "linear_absorption_overview.png"
    fig.savefig(out_path, dpi=160)
    if show:
        plt.show(block=True)
    else:
        plt.close(fig)
    return out_path
