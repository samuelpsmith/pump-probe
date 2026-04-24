from xps_q_new import XPS


class XPSQController:
    """
    Socket-based controller for Newport XPS-Q.
    """

    def __init__(
        self,
        ip="192.168.254.254",
        port=5001,
        timeout_ms=100,
        group="DS",
        auto_initialize=True,
        auto_home=True,
        verbose=True,
    ):
        self.ip = str(ip)
        self.port = int(port)
        self.timeout_ms = int(timeout_ms)
        self.group = str(group)
        self.verbose = bool(verbose)

        self.xps = XPS()
        self.sid = self.xps.TCP_ConnectToServer(self.ip, self.port, self.timeout_ms)

        if self.sid == -1:
            raise RuntimeError(
                "XPS TCP connection failed. "
                f"ip={self.ip}, port={self.port}, timeout_ms={self.timeout_ms}"
            )
        if self.verbose:
            print(f"Connected to XPS. Socket ID: {self.sid}")

        if auto_initialize:
            init_ret = self.xps.GroupInitialize(self.sid, self.group)
            if self.verbose:
                print(f"GroupInitialize({self.group}) -> {init_ret}")

        if auto_home:
            home_ret = self.xps.GroupHomeSearch(self.sid, self.group)
            if self.verbose:
                print(f"GroupHomeSearch({self.group}) -> {home_ret}")

        if self.verbose:
            status = self.xps.GroupStatusGet(self.sid, self.group)
            print(f"GroupStatusGet({self.group}) -> {status}")

    def move_absolute(self, positions):
        """
        positions should be a list/tuple, e.g. [-50] for single-axis groups.
        """
        return self.xps.GroupMoveAbsolute(self.sid, self.group, positions)

    def _discover_group_positioners(self):
        """
        Return discovered positioner object names for this group from ObjectsListGet.
        """
        result = self.xps.ObjectsListGet(self.sid)
        if not isinstance(result, (list, tuple)) or len(result) < 2:
            return []
        err = int(result[0])
        if err != 0:
            return []

        raw = str(result[1])
        text = (
            raw.replace("\r", "\n")
            .replace(";", "\n")
            .replace(",", "\n")
            .replace("\t", "\n")
        )
        tokens = [tok.strip() for tok in text.split("\n") if tok.strip()]

        group_prefix = f"{self.group}."
        group_prefix_lower = group_prefix.lower()
        names = []
        seen = set()
        for tok in tokens:
            tok_lower = tok.lower()
            if tok_lower.startswith(group_prefix_lower):
                if tok_lower not in seen:
                    seen.add(tok_lower)
                    names.append(tok)

        return names

    def get_user_travel_limits(self, positioner_name=None):
        """
        Return configured user travel limits as (min_target, max_target).

        If positioner_name is omitted, try common names for single-axis groups.
        """
        candidates = []
        if positioner_name:
            candidates.append(str(positioner_name))
        else:
            candidates.extend(self._discover_group_positioners())
            candidates.extend(
                [
                    f"{self.group}.Pos",
                    f"{self.group}.pos",
                    self.group,
                ]
            )

        last_error = None
        deduped = []
        seen = set()
        for candidate in candidates:
            key = str(candidate).lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(candidate)
        candidates = deduped

        for candidate in candidates:
            result = self.xps.PositionerUserTravelLimitsGet(self.sid, candidate)
            if not isinstance(result, (list, tuple)) or len(result) < 1:
                last_error = f"Unexpected limits payload for {candidate!r}: {result!r}"
                continue

            try:
                err = int(result[0])
            except Exception:
                last_error = f"Unexpected error field for {candidate!r}: {result!r}"
                continue

            if err != 0:
                last_error = f"{candidate!r} -> error {err}, payload={result!r}"
                continue

            if len(result) < 3:
                last_error = (
                    f"{candidate!r} -> success code but missing min/max payload: {result!r}"
                )
                continue

            if err == 0:
                return float(result[1]), float(result[2]), candidate

        raise RuntimeError(
            "Unable to read XPS user travel limits. "
            f"Tried {candidates!r}. Last error: {last_error}"
        )

    def get_position(self):
        return self.xps.GroupPositionCurrentGet(self.sid, self.group, 1)

    # Backward-compatible method name used by older scripts.
    def get_pos(self):
        return self.get_position()

    def close(self):
        if self.sid != -1:
            self.xps.TCP_CloseSocket(self.sid)
            self.sid = -1
            if self.verbose:
                print("Closed XPS socket.")


# Backward-compatible alias used by older scripts.
xps_controller = XPSQController
XPSLinearStageController = XPSQController


if __name__ == "__main__":
    # Move range depends on configured stage/group.
    ctrl = XPSQController()
    print(ctrl.get_position())

