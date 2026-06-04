"""Scan plan dataclasses for steady-state and time-resolved experiments."""

from dataclasses import dataclass, field
import random


@dataclass(slots=True)
class ScanAxis:
    """Axis definition for a scan."""

    name: str
    units: str
    values: list[float]


@dataclass(slots=True)
class ScanPlan:
    """General scan plan that can support ordered or randomized acquisition."""

    axis: ScanAxis
    n_scans: int = 1
    order_mode: str = "ordered"
    settle_time_s: float = 0.2
    lines_per_point: int = 256
    pulse_grouping_size: int = 2
    save_subaverages: bool = True
    metadata: dict[str, str] = field(default_factory=dict)

    def expanded_points(self, seed: int | None = None) -> list[float]:
        """Return one scan's points in the requested acquisition order."""
        values = list(self.axis.values)
        if self.order_mode == "ordered":
            return values
        if self.order_mode == "randomized":
            rng = random.Random(seed)
            rng.shuffle(values)
            return values
        raise ValueError(f"Unsupported order_mode: {self.order_mode}")
