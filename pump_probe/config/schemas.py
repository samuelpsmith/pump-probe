"""Dataclass-based configuration schemas shared by tools and experiments."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class DaqRuntimeConfig:
    """DAQ runtime behavior shared by tools and experiments."""

    timing_profile: str
    runtime_profile: str
    expected_trigger_hz: float = 1000.0
    integration_lines: int = 128
    pump_chop_sign: float = -1.0
    use_reference_channel: bool = False


@dataclass(slots=True)
class ToolViewConfig:
    """Lightweight live-tool display configuration."""

    plot_fps: float = 25.0
    plot_every_lines: int = 2
    timing_text_every_lines: int = 40
    autoscale_every_updates: int = 8
    plot_raw_line: bool = True
    live_video_mode: str = "main"


@dataclass(slots=True)
class AcquisitionConfig:
    """Acquisition settings independent of a specific experiment family."""

    lines_per_point: int
    settle_time_s: float = 0.2
    save_group_statistics: bool = True
    save_subaverages: bool = True
    pulse_grouping_size: int = 2


@dataclass(slots=True)
class ExperimentConfig:
    """Top-level experiment configuration container."""

    experiment_type: str
    sample_name: str
    family: str
    output_root: Path
    daq: DaqRuntimeConfig
    acquisition: AcquisitionConfig
    extra: dict[str, Any] = field(default_factory=dict)
