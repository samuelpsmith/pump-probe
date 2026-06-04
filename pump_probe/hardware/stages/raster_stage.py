"""Raster-stage interface definitions."""

from typing import Protocol


class RasterStage(Protocol):
    """Protocol for the WLG raster stage."""

    def get_position(self) -> tuple[float, float]:
        ...

    def move_absolute(self, x: float, y: float) -> None:
        ...
