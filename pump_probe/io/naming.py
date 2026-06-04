"""Run and file naming helpers."""

from datetime import datetime


def timestamp_slug(dt: datetime | None = None) -> str:
    """Return a compact timestamp string suitable for run folders."""
    dt = datetime.now() if dt is None else dt
    return dt.strftime("%Y%m%d_%H%M%S")


def run_directory_name(experiment_type: str, label: str | None = None) -> str:
    """Build a readable run-directory name."""
    suffix = "" if not label else f"_{label}"
    return f"{timestamp_slug()}_{experiment_type}{suffix}"
