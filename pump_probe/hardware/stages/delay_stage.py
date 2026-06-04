"""Delay-stage interface definitions and adapters."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class DelayStage(Protocol):
    """Protocol for the stage responsible for pump-probe delay."""

    def get_position_mm(self) -> float:
        ...

    def move_absolute_mm(self, position_mm: float) -> None:
        ...

    def close(self) -> None:
        ...


class ManualDelayStage:
    """Prompt-driven stage backend for manual delay-line motion."""

    def __init__(self, *, initial_position_mm: float | None = None) -> None:
        self._last_position_mm = (
            None if initial_position_mm is None else float(initial_position_mm)
        )

    def get_position_mm(self) -> float:
        if self._last_position_mm is None:
            raise RuntimeError("Manual stage position is unknown until the first move.")
        return float(self._last_position_mm)

    def move_absolute_mm(self, position_mm: float) -> None:
        input(
            f"Move the delay stage to {float(position_mm):.6f} mm, then press Enter to continue..."
        )
        self._last_position_mm = float(position_mm)

    def close(self) -> None:
        return None


def _load_xps_controller_class():
    module_path = Path(__file__).resolve().parents[3] / "stepper_control" / "xps_q_controller.py"
    module_dir = str(module_path.parent)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    spec = importlib.util.spec_from_file_location("pump_probe_legacy_xps", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load XPS controller module from {module_path}.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.XPSLinearStageController


class XPSDelayStage:
    """Shared-package wrapper around the existing XPS linear stage controller."""

    def __init__(
        self,
        *,
        ip: str = "192.168.254.254",
        port: int = 5001,
        timeout_ms: int = 100,
        group: str = "DS",
        auto_initialize: bool = True,
        auto_home: bool = True,
        positioner_name: str | None = None,
        move_settle_s: float = 0.2,
        position_tolerance_mm: float = 0.002,
        verbose: bool = True,
    ) -> None:
        ctrl_cls = _load_xps_controller_class()
        self._ctrl = ctrl_cls(
            ip=ip,
            port=port,
            timeout_ms=timeout_ms,
            group=group,
            auto_initialize=auto_initialize,
            auto_home=auto_home,
            verbose=verbose,
        )
        self.positioner_name = positioner_name
        self.move_settle_s = max(0.0, float(move_settle_s))
        self.position_tolerance_mm = max(0.0, float(position_tolerance_mm))

    @staticmethod
    def _coerce_position_mm(payload) -> float:
        if isinstance(payload, (list, tuple)):
            if len(payload) >= 2:
                return float(payload[1])
            if len(payload) == 1:
                return float(payload[0])
        return float(payload)

    def get_position_mm(self) -> float:
        return self._coerce_position_mm(self._ctrl.get_position())

    def get_limits_mm(self) -> tuple[float, float] | None:
        try:
            lower, upper, _ = self._ctrl.get_user_travel_limits(
                positioner_name=self.positioner_name
            )
            return float(lower), float(upper)
        except Exception:
            return None

    def move_absolute_mm(self, position_mm: float) -> None:
        target = float(position_mm)
        limits = self.get_limits_mm()
        if limits is not None:
            lower, upper = limits
            if not (lower <= target <= upper):
                raise ValueError(
                    f"Requested target {target:.6f} mm is outside XPS limits "
                    f"[{lower:.6f}, {upper:.6f}]"
                )
        self._ctrl.move_absolute([target])
        if self.move_settle_s > 0:
            time.sleep(self.move_settle_s)
        actual = self.get_position_mm()
        if abs(actual - target) > self.position_tolerance_mm:
            raise RuntimeError(
                f"XPS stage did not settle within tolerance: target={target:.6f} mm, "
                f"actual={actual:.6f} mm, tol={self.position_tolerance_mm:.6f} mm"
            )

    def close(self) -> None:
        self._ctrl.close()
