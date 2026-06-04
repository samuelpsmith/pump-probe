"""Base classes for time-resolved experiments."""

from dataclasses import dataclass

from pump_probe.experiments.base import ExperimentBase
from pump_probe.experiments.scan_plan import ScanPlan


@dataclass(slots=True)
class TimeResolvedExperimentBase(ExperimentBase):
    """Marker base class for time-resolved experiment runners."""

    scan_plan: ScanPlan
