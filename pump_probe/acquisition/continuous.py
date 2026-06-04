"""Continuous line-reader utilities for scan experiments."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import threading
import time
from typing import Any

from pump_probe.hardware.daq.line_models import RawLine


@dataclass(slots=True)
class ContinuousLineReader:
    """Continuously drain a DAQ session into a bounded FIFO buffer."""

    session: Any
    read_timeout_s: float = 10.0
    max_buffer_lines: int = 8192
    _buffer: deque[RawLine] = field(init=False, repr=False)
    _cond: threading.Condition = field(init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _stop: threading.Event = field(init=False, repr=False)
    _error: BaseException | None = field(default=None, init=False, repr=False)
    _read_count: int = field(default=0, init=False)
    _discarded_count: int = field(default=0, init=False)
    _max_depth: int = field(default=0, init=False)
    _started_at_s: float | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.max_buffer_lines = max(2, int(self.max_buffer_lines))
        self._buffer = deque(maxlen=self.max_buffer_lines)
        self._cond = threading.Condition()
        self._stop = threading.Event()

    def start(self) -> "ContinuousLineReader":
        if self._thread is not None:
            return self
        self._started_at_s = time.perf_counter()
        self._thread = threading.Thread(
            target=self._run,
            name="ContinuousLineReader",
            daemon=True,
        )
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                line = self.session.read_line(timeout=float(self.read_timeout_s))
            except BaseException as exc:  # noqa: BLE001 - surface acquisition failures.
                with self._cond:
                    self._error = exc
                    self._cond.notify_all()
                return
            with self._cond:
                if len(self._buffer) == self._buffer.maxlen:
                    self._buffer.popleft()
                    self._discarded_count += 1
                self._buffer.append(line)
                self._read_count += 1
                self._max_depth = max(self._max_depth, len(self._buffer))
                self._cond.notify_all()

    def discard_available(self) -> int:
        """Drop currently buffered lines and return the number discarded."""

        with self._cond:
            count = len(self._buffer)
            self._buffer.clear()
            self._discarded_count += count
            return count

    def collect_lines(self, n_lines: int, *, timeout_s: float) -> list[RawLine]:
        """Collect exactly ``n_lines`` FIFO lines from the continuously read buffer."""

        n_lines = max(1, int(n_lines))
        deadline = time.perf_counter() + max(0.0, float(timeout_s))
        out: list[RawLine] = []
        with self._cond:
            while len(out) < n_lines:
                while self._buffer and len(out) < n_lines:
                    out.append(self._buffer.popleft())
                if len(out) >= n_lines:
                    break
                if self._error is not None:
                    raise RuntimeError("Continuous DAQ reader stopped with an error.") from self._error
                remaining = deadline - time.perf_counter()
                if remaining <= 0.0:
                    raise TimeoutError(
                        f"Timed out collecting {n_lines} lines; collected {len(out)}."
                    )
                self._cond.wait(timeout=min(0.25, remaining))
        return out

    def stats(self) -> dict[str, Any]:
        with self._cond:
            elapsed = (
                max(1e-9, time.perf_counter() - self._started_at_s)
                if self._started_at_s is not None
                else float("nan")
            )
            return {
                "read_count": int(self._read_count),
                "discarded_count": int(self._discarded_count),
                "buffer_depth": int(len(self._buffer)),
                "max_buffer_depth": int(self._max_depth),
                "reader_rate_hz": (
                    float(self._read_count) / elapsed
                    if isinstance(elapsed, float) and elapsed > 0
                    else float("nan")
                ),
                "error": None if self._error is None else repr(self._error),
            }

    def close(self, join_timeout_s: float = 2.0) -> None:
        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=max(0.0, float(join_timeout_s)))
        self._thread = None
