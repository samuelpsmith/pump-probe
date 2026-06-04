"""Config-driven command-line experiment entry points."""

from pump_probe.cli.linear_absorption import main as linear_absorption_main
from pump_probe.cli.steady_state_cd import main as steady_state_cd_main
from pump_probe.cli.transient_absorption import main as transient_absorption_main

__all__ = [
    "linear_absorption_main",
    "steady_state_cd_main",
    "transient_absorption_main",
]
