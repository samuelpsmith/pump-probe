import time
import traceback

from xps_q_controller import XPSLinearStageController


def _log(message):
    print(f"[xps {time.strftime('%H:%M:%S')}] {message}")


def run_xps_stage(
    ip="192.168.254.254",
    port=5001,
    timeout_ms=100,
    group="DS",
    probe_only=True,
    move_target=None,
    positioner_name=None,
):
    ctrl = None
    try:
        _log("Starting XPS stage runner.")
        _log(f"Target: ip={ip}, port={port}, group={group}")
        ctrl = XPSLinearStageController(
            ip=ip,
            port=port,
            timeout_ms=timeout_ms,
            group=group,
            auto_initialize=True,
            auto_home=True,
            verbose=True,
        )

        pos = ctrl.get_position()
        _log(f"Current position: {pos!r}")
        try:
            min_target, max_target, used_name = ctrl.get_user_travel_limits(
                positioner_name=positioner_name
            )
            _log(
                "User travel limits "
                f"({used_name}): min={min_target:.6f}, max={max_target:.6f}"
            )
        except Exception as limits_exc:
            min_target = max_target = None
            _log(f"Could not read user travel limits: {limits_exc}")

        if not probe_only and move_target is not None:
            if not isinstance(move_target, (list, tuple)) or len(move_target) != 1:
                raise ValueError(
                    "move_target must be a single-element list/tuple for this stage, "
                    f"got {move_target!r}"
                )
            target = float(move_target[0])
            if min_target is not None and max_target is not None:
                if not (min_target <= target <= max_target):
                    raise ValueError(
                        f"Requested target {target} is outside allowed range "
                        f"[{min_target}, {max_target}]"
                    )
            _log(f"Move absolute -> {move_target!r}")
            res = ctrl.move_absolute(move_target)
            _log(f"Move result: {res!r}")
            _log(f"Position after move: {ctrl.get_position()!r}")
        else:
            _log("Probe-only mode: no move command issued.")
    except Exception as exc:
        _log(f"FAILED: {exc}")
        traceback.print_exc()
    finally:
        if ctrl is not None:
            ctrl.close()
        _log("Done.")


if __name__ == "__main__":
    # Set probe_only=False and a target list like [-50] to command motion.
    #run_xps_stage(probe_only=True, move_target=None)
    run_xps_stage(probe_only=False, move_target=[-50])
