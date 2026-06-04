"""Averaging helpers used by tools and experiment runners."""

from __future__ import annotations

from collections import deque

import numpy as np


class RollingSpectrumAverage:
    """Rolling average for fixed-length spectra.

    This is intentionally lightweight so live tools can reuse it without
    dragging in the fuller experiment reduction pipeline.
    """

    def __init__(self, window_size: int) -> None:
        self.window_size = max(1, int(window_size))
        self._buffer: deque[np.ndarray] = deque()
        self._sum: np.ndarray | None = None

    @property
    def count(self) -> int:
        return len(self._buffer)

    def append(self, spectrum: np.ndarray) -> None:
        arr = np.asarray(spectrum, dtype=float)
        if self._sum is None:
            self._sum = np.zeros_like(arr, dtype=float)
        elif self._sum.shape != arr.shape:
            raise ValueError(
                f"Expected spectrum shape {self._sum.shape}, got {arr.shape}."
            )

        self._buffer.append(arr.copy())
        self._sum += arr
        if len(self._buffer) > self.window_size:
            dropped = self._buffer.popleft()
            self._sum -= dropped

    def mean(self) -> np.ndarray | None:
        if self._sum is None or not self._buffer:
            return None
        return self._sum / float(len(self._buffer))
