import time
import os
import platform
import subprocess
import sys
import tempfile
from pathlib import Path


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
        return False, "probe output unusable"

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
    return True, "ok"


def _ensure_python32_with_clr():
    if os.environ.get("PUMP_PROBE_DISABLE_PY32_RELAUNCH", "0") == "1":
        return None

    try:
        import clr  # noqa: F401
        return None
    except Exception:
        pass

    current_arch = platform.architecture()[0].lower()
    print(
        f"[conex] Current interpreter ({current_arch}) missing pythonnet/clr; "
        "searching for compatible 32-bit Python."
    )
    for candidate in _candidate_python32_paths():
        ok, reason = _interpreter_is_compatible_32bit(candidate)
        print(f"[conex] Probe {candidate}: {reason}")
        if not ok:
            continue
        print(f"[conex] Relaunching with compatible interpreter: {candidate}")
        cmd = [str(candidate), str(Path(__file__).resolve()), *sys.argv[1:]]
        result = subprocess.run(cmd)
        return result.returncode

    print(
        "[conex] No compatible 32-bit Python with pythonnet/clr was found.\n"
        "[conex] Set PUMP_PROBE_PYTHON32 to a known-good interpreter path."
    )
    return 2


def run_conex_raster(
    com_port="COM5",
    loops=10000,
    position_1=0.1,
    position_2=3.0,
    acceleration=4.0,
    velocity=0.08,
    start_window=True,
):
    import Newport_connex_pp_controller as npp

    print("[conex] Starting CONEX raster controller.")
    print(f"[conex] COM port: {com_port}")
    print(
        "[conex] Raster config: "
        f"loops={loops}, p1={position_1}, p2={position_2}, "
        f"acc={acceleration}, vel={velocity}"
    )

    controller = npp.ConexPPRasterController(com_port)
    controller.raster(loops, position_1, position_2, acceleration, velocity)

    if start_window:
        print("[conex] Launching position window.")
        controller.start_raster_window()
    print("[conex] Raster routine complete.")


if __name__ == "__main__":
    reexec_code = _ensure_python32_with_clr()
    if reexec_code is not None:
        sys.exit(reexec_code)
    run_conex_raster()
