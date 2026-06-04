"""Central default paths and naming conventions."""

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parent

DATA_ROOT = REPO_ROOT / "data"
CALIBRATION_ROOT = REPO_ROOT / "calibrations"
PLOT_ROOT = REPO_ROOT / "plots"

DEFAULT_WAVELENGTH_CAL_FILE = CALIBRATION_ROOT / "wavelength_default.json"
DEFAULT_DELAY_CAL_FILE = CALIBRATION_ROOT / "delay_default.json"
DEFAULT_PUMP_DARK_FILE = CALIBRATION_ROOT / "pump_dark_default.npz"
