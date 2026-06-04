"""Wavelength calibration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np

from pump_probe.calibration.models import WavelengthCalibration
from pump_probe.processing.units import wavelength_nm_to_wavenumber_cm_inv


@dataclass(slots=True)
class WavelengthAnchor:
    """Manual pixel-to-wavelength anchor point."""

    pixel_index: float
    wavelength_nm: float
    label: str | None = None


def fit_manual_wavelength_calibration(
    *,
    anchors: list[WavelengthAnchor],
    pixel_count: int = 1024,
    polynomial_order: int | None = None,
    calibration_id: str = "manual_wavelength",
    operator: str | None = None,
    notes: str | None = None,
) -> WavelengthCalibration:
    """Fit a wavelength calibration from manual anchor points.

    The default polynomial order is chosen conservatively from the number of
    anchors so the caller does not have to micromanage a first-pass fit.
    """

    if len(anchors) < 2:
        raise ValueError("At least two manual wavelength anchors are required.")

    pixels = np.asarray([a.pixel_index for a in anchors], dtype=float)
    wavelengths = np.asarray([a.wavelength_nm for a in anchors], dtype=float)

    if polynomial_order is None:
        polynomial_order = min(2, len(anchors) - 1)
    order = int(polynomial_order)
    if order < 1 or order >= len(anchors):
        raise ValueError(
            "polynomial_order must be >= 1 and strictly less than the number of anchors."
        )

    coeffs = np.polyfit(pixels, wavelengths, order)
    full_pixels = np.arange(int(pixel_count), dtype=float)
    wavelength_nm = np.polyval(coeffs, full_pixels)

    anchor_labels = [a.label for a in anchors if a.label]
    notes_parts = [notes] if notes else []
    if anchor_labels:
        notes_parts.append(f"anchors={anchor_labels}")

    return WavelengthCalibration(
        calibration_id=calibration_id,
        kind="wavelength",
        created_at=datetime.now(timezone.utc).isoformat(),
        operator=operator,
        notes="; ".join(notes_parts) if notes_parts else None,
        pixel_index=full_pixels,
        wavelength_nm=wavelength_nm,
        polynomial_coefficients=np.asarray(coeffs, dtype=float),
        compatibility_tags={
            "pixel_count": str(int(pixel_count)),
            "wavenumber_axis_available": "true",
        },
    )


def calibrated_wavenumber_axis(calibration: WavelengthCalibration) -> np.ndarray:
    """Return the wavenumber axis implied by a wavelength calibration."""

    return wavelength_nm_to_wavenumber_cm_inv(calibration.wavelength_nm)


def load_wavelength_calibration(path: str | Path) -> WavelengthCalibration:
    """Load a saved wavelength calibration JSON file."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    coeffs = payload.get("polynomial_coefficients")
    return WavelengthCalibration(
        calibration_id=str(payload["calibration_id"]),
        kind=str(payload["kind"]),
        created_at=str(payload["created_at"]),
        source_run_ids=list(payload.get("source_run_ids", [])),
        operator=payload.get("operator"),
        notes=payload.get("notes"),
        software_version=payload.get("software_version"),
        git_commit=payload.get("git_commit"),
        compatibility_tags=dict(payload.get("compatibility_tags", {})),
        pixel_index=np.asarray(payload["pixel_index"], dtype=float),
        wavelength_nm=np.asarray(payload["wavelength_nm"], dtype=float),
        polynomial_coefficients=(
            None if coeffs is None else np.asarray(coeffs, dtype=float)
        ),
    )
