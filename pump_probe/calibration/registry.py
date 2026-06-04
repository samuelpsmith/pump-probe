"""Runtime registry for calibration objects."""

from collections import defaultdict

from pump_probe.calibration.models import CalibrationRecord


class CalibrationRegistry:
    """Simple in-memory registry keyed by calibration id and kind."""

    def __init__(self) -> None:
        self._by_id: dict[str, CalibrationRecord] = {}
        self._by_kind: dict[str, list[CalibrationRecord]] = defaultdict(list)

    def register(self, calibration: CalibrationRecord) -> None:
        self._by_id[calibration.calibration_id] = calibration
        self._by_kind[calibration.kind].append(calibration)

    def get(self, calibration_id: str) -> CalibrationRecord:
        return self._by_id[calibration_id]

    def by_kind(self, kind: str) -> list[CalibrationRecord]:
        return list(self._by_kind.get(kind, ()))
