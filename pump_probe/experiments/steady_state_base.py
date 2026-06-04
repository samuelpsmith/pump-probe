"""Base classes for steady-state experiments."""

from dataclasses import dataclass

from pump_probe.experiments.base import ExperimentBase


@dataclass(slots=True)
class SteadyStateExperimentBase(ExperimentBase):
    """Marker base class for steady-state experiment runners."""
