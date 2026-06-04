"""Abstract chopper controller interfaces."""

from typing import Protocol


class ChopperController(Protocol):
    """Protocol for phase-locked or internally driven chopper devices."""

    def configure(self) -> None:
        ...

    def get_status(self) -> dict[str, object]:
        ...
