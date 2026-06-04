"""Experiment metadata dataclasses."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class RunProvenance:
    """Software and operator information for a run."""

    operator: str | None = None
    software_version: str | None = None
    git_commit: str | None = None


@dataclass(slots=True)
class ExperimentMetadata:
    """Top-level metadata shared by steady-state and time-resolved runs."""

    experiment_type: str
    sample_name: str
    started_at: str
    notes: str | None = None
    provenance: RunProvenance = field(default_factory=RunProvenance)
    tags: dict[str, str] = field(default_factory=dict)
