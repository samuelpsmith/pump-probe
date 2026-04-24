import os
import platform
import argparse
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path


def _log(message):
    timestamp = time.strftime("%H:%M:%S")
    print(f"[chopper {timestamp}] {message}", flush=True)


def _safe_call(label, fn, *args, **kwargs):
    try:
        result = fn(*args, **kwargs)
        _log(f"{label}: OK -> {result!r}")
        return True, result
    except Exception as exc:
        _log(f"{label}: FAILED -> {exc}")
        traceback.print_exc()
        return False, None


def _candidate_python32_paths():
    home = Path.home()
    env_override = os.environ.get("PUMP_PROBE_PYTHON32", "").strip()
    candidates = []
    if env_override:
        candidates.append(Path(env_override))

    candidates.extend(
        [
            home / "OneDrive" / "Desktop" / "python_TAS" / ".venv" / "Scripts" / "python.exe",
            home / "AppData" / "Local" / "Programs" / "Python" / "Python313-32" / "python.exe",
            home / "AppData" / "Local" / "Programs" / "Python" / "Python312-32" / "python.exe",
            home / "AppData" / "Local" / "Programs" / "Python" / "Python311-32" / "python.exe",
        ]
    )

    seen = set()
    unique = []
    for p in candidates:
        key = str(p).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(p)
    return unique


def _interpreter_is_compatible_32bit(python_exe):
    if not python_exe.exists():
        return False, "missing file"
    probe_code = (
        "import json, platform\n"
        "result={'arch': platform.architecture()[0], 'has_clr': False, 'clr_error': ''}\n"
        "try:\n"
        "    import clr\n"
        "    result['has_clr'] = True\n"
        "except Exception as exc:\n"
        "    result['clr_error'] = f'{type(exc).__name__}: {exc}'\n"
        "print(json.dumps(result))\n"
    )
    probe_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_py32_probe.py", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(probe_code)
            probe_path = tmp.name
        proc = subprocess.run(
            [str(python_exe), probe_path],
            capture_output=True,
            text=True,
            timeout=8,
        )
    except Exception as exc:
        return False, f"probe failed ({exc})"
    finally:
        if probe_path and os.path.exists(probe_path):
            try:
                os.remove(probe_path)
            except OSError:
                pass

    stdout_lines = [ln.strip() for ln in (proc.stdout or "").splitlines() if ln.strip()]
    json_line = None
    for ln in reversed(stdout_lines):
        if ln.startswith("{") and ln.endswith("}"):
            json_line = ln
            break
    if not json_line:
        stderr_tail = (proc.stderr or "").strip().splitlines()[-1:] if proc.stderr else []
        tail = stderr_tail[0] if stderr_tail else "no parseable probe output"
        return False, f"probe output unusable ({tail})"

    try:
        import json

        parsed = json.loads(json_line)
    except Exception as exc:
        return False, f"probe json parse error ({exc})"

    arch = str(parsed.get("arch", "")).lower()
    has_clr = bool(parsed.get("has_clr", False))
    clr_error = str(parsed.get("clr_error", "")).strip()
    if "32bit" not in arch:
        return False, f"not 32-bit ({arch or 'unknown'})"
    if not has_clr:
        return False, f"pythonnet/clr unavailable ({clr_error or 'unknown'})"
    if proc.returncode != 0:
        return True, f"ok (non-zero exit {proc.returncode}, tolerated)"
    return True, "ok"


def _maybe_relaunch_with_python32():
    if os.environ.get("PUMP_PROBE_DISABLE_PY32_RELAUNCH", "0") == "1":
        _log("Auto 32-bit relaunch disabled by PUMP_PROBE_DISABLE_PY32_RELAUNCH=1.")
        return None

    current_arch = platform.architecture()[0].lower()
    if "32bit" in current_arch:
        return None

    _log(f"Detected {current_arch} Python. Chopper driver stack expects 32-bit runtime.")
    for candidate in _candidate_python32_paths():
        ok, reason = _interpreter_is_compatible_32bit(candidate)
        _log(f"Probe {candidate}: {reason}")
        if not ok:
            continue
        _log(f"Relaunching with 32-bit interpreter: {candidate}")
        cmd = [str(candidate), str(Path(__file__).resolve()), *sys.argv[1:]]
        result = subprocess.run(cmd)
        return result.returncode

    _log(
        "Warning: no compatible 32-bit Python interpreter with pythonnet found. "
        "Continuing in current interpreter (device discovery may fail). "
        "Set PUMP_PROBE_PYTHON32 to an explicit path if needed."
    )
    return None


def _build_cli():
    parser = argparse.ArgumentParser(
        description="Newport 3502 chopper setup/diagnostic runner."
    )
    parser.add_argument(
        "--freq-hz",
        type=float,
        default=500.0,
        help="Requested chopping frequency in Hz (default: 500).",
    )
    parser.add_argument(
        "--wheel",
        default="42/30",
        help="Wheel selection label or numeric enum (default: 42/30).",
    )
    parser.add_argument(
        "--sync",
        default="ext+",
        help="Sync mode label or numeric enum (default: EXT+).",
    )
    parser.add_argument(
        "--mode",
        default="normal",
        help="Controller mode label or numeric enum (default: normal).",
    )
    return parser


def _resolve_enum(value, mapping):
    text = str(value).strip()
    if text.lstrip("+-").isdigit():
        return int(text)
    key = text.lower().replace(" ", "")
    if key in mapping:
        return mapping[key]
    raise ValueError(
        f"Unsupported value {value!r}. Supported labels: {sorted(mapping.keys())}"
    )


if __name__ == "__main__":
    args = _build_cli().parse_args()

    reexec_code = _maybe_relaunch_with_python32()
    if reexec_code is not None:
        sys.exit(reexec_code)

    import Newport_3502_chopper_controller as npc

    # Numeric enums are from Newport 3502 command library usage in this project.
    # Keep labels human-readable and allow numeric override from CLI for safety.
    wheel_map = {
        "42/30": 4,
        "4230": 4,
        "4": 4,
    }
    sync_map = {
        "int": 1,
        "internal": 1,
        "ext": 2,
        "external": 2,
        "ext+": 3,
        "external+": 3,
    }
    mode_map = {
        "normal": 0,
        "0": 0,
    }

    target_frequency_hz = float(args.freq_hz)
    target_wheel = _resolve_enum(args.wheel, wheel_map)
    target_sync = _resolve_enum(args.sync, sync_map)
    target_mode = _resolve_enum(args.mode, mode_map)

    _log("Starting chopper diagnostic runner.")
    _log(f"Python: {sys.version.split()[0]}")
    _log(f"Working directory: {os.getcwd()}")
    _log(
        "Requested settings: "
        f"wheel={args.wheel!r} -> {target_wheel}, "
        f"sync={args.sync!r} -> {target_sync}, "
        f"mode={args.mode!r} -> {target_mode}, "
        f"freq={target_frequency_hz:.3f} Hz"
    )

    controller = None
    exit_code = 0

    try:
        ok, controller = _safe_call(
            "Instantiate controller", npc.newport_3502_chopper_controller
        )
        if not ok or controller is None:
            _log("Controller construction failed. Stopping.")
            sys.exit(1)

        device_key = getattr(controller, "devicekey", None)
        _log(f"Controller device key: {device_key!r}")

        if not device_key:
            _log(
                "Warning: device key is empty/None. "
                "This usually means no chopper was discovered by the driver."
            )
            if hasattr(controller, "driver"):
                _safe_call("Driver.DiscoverDevices()", controller.driver.DiscoverDevices)
                _safe_call(
                    "Driver.GetFirstDeviceKey()", controller.driver.GetFirstDeviceKey
                )
            _log("Skipping set/get frequency because no valid device key was found.")
            exit_code = 2
        else:
            _safe_call("Set wheel type", controller.setWheelType, target_wheel)
            _safe_call("Set sync mode", controller.setSync, target_sync)
            _safe_call("Set controller mode", controller.setMode, target_mode)
            _safe_call("Set synth frequency", controller.setSynthFreq, target_frequency_hz)
            _safe_call("Get wheel type", controller.getWheelType)
            ok_read, measured_frequency = _safe_call(
                "Get synth frequency", controller.getSynthFreq
            )
            if ok_read:
                _log(
                    "Frequency summary: "
                    f"requested={target_frequency_hz:.3f} Hz, "
                    f"reported={float(measured_frequency):.3f} Hz"
                )
            _log("Start/configure command complete. Exiting.")

    except KeyboardInterrupt:
        _log("Interrupted by user.")
        exit_code = 130
    except Exception as exc:
        _log(f"Unhandled exception: {exc}")
        traceback.print_exc()
        exit_code = 1
    finally:
        if controller is not None and hasattr(controller, "close"):
            _safe_call("Close controller", controller.close)
        _log(f"Done. Exit code={exit_code}")

    sys.exit(exit_code)
