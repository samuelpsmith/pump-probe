# pump-probe

Tools for pump-probe laser spectroscopy experiments controlled by Python.

## What Is Included

- `live_cmos.py`: lightweight shared-package live CMOS/PDA setup tool built on the new `pump_probe/` architecture.
- `linear_absorption.py`: shared-package linear absorption runner that saves raw stacks plus processed absorbance.
- `transient_absorption.py`: shared-package transient absorption runner with explicit delay lists and per-delay raw-stack saving.
- `steady_state_cd.py`: shared-package steady-state circular dichroism runner for odd/even polarization modulation experiments.
- `pump_probe/`: shared package for DAQ adapters, calibrations, processing, and experiment runners under active refactor.
- `DAQ_pda_simple.py`: legacy stand-alone NI-DAQmx control and live plotting script for CMOS/PDA-style line acquisition.
- `DAQ_pda_hard.py`: hardware-timed/retrigger-focused CMOS/PDA runner with persistent session mode, timing sweep tools, and richer diagnostics.
- `stepper_control/conex_raster.py`: CONEX-PP raster stage runner (WLG crystal raster).
- `stepper_control/chopper.py`: Newport 3502 chopper diagnostic/control runner.
- `stepper_control/xps_stage.py`: XPS-Q stage diagnostic/control runner.
- `stepper_control/WLG_stepper.py`: legacy alias that forwards to `conex_raster.py`.
- `stepper_control/` dependencies: CONEX/chopper/XPS wrappers and required `x86_drivers` folder.
- `serial_control/serial_controllers.py`: serial/VISA controllers (including MC2000B chopper via DLL wrapper).

## Hardware Naming Note

Historically, "stepper" was used for two different motion systems:

- CONEX-PP raster stage (used for WLG crystal rastering)
- XPS-Q motion stage controller

To reduce confusion, these now have separate entry scripts.
Serial/VISA controllers are kept in `serial_control/`.

Canonical controller names used in this repo:

- CONEX raster stage: `ConexPPRasterController`
- XPS linear stage: `XPSLinearStageController` / `XPSQController`
- Newport serial stage (VISA): `NewportSerialStageController`
- Thorlabs MC2000B chopper: `ThorlabsMC2000BChopperController`

Legacy names remain as compatibility aliases for older scripts.

## Requirements

- Python 3.10+
- NI-DAQmx driver installed on the host machine
- Compatible NI DAQ hardware (for example NI-6363)

Install Python packages:

```powershell
pip install -r requirements.txt
```

## Run

```powershell
python DAQ_pda_simple.py
```

For the new lightweight shared-package live tool:

```powershell
python live_cmos.py
```

For the first shared-package experiment runner:

```powershell
python linear_absorption.py --sample-name MySample
```

For the first shared-package transient absorption runner:

```powershell
python transient_absorption.py --sample-name MySample --zero-position-mm 0.0 --delay-ps 0 --delay-ps 1 --delay-ps 10
```

Or from a JSON config file:

```powershell
python transient_absorption.py --config configs/examples/transient_absorption_example.json
```

For the first shared-package steady-state CD runner:

```powershell
python steady_state_cd.py --sample-name MySample
```

For the hardware-timed retrigger script:

```powershell
python DAQ_pda_hard.py
```

Useful `DAQ_pda_hard.py` options:

```powershell
# Select runtime mode (default is persistent_robust_test)
python DAQ_pda_hard.py --runtime-profile persistent_robust_test

# Quick usage presets
python DAQ_pda_hard.py --preset signal_only
python DAQ_pda_hard.py --preset trigger_debug

# Tune live plot cadence
python DAQ_pda_hard.py --plot-fps 25 --plot-every-lines 2 --timing-text-every-lines 40 --autoscale-every-updates 8

# Run persistent timing sweep
python DAQ_pda_hard.py --operation sweep
```

For the stepper raster script:

```powershell
python stepper_control/conex_raster.py
```

For Newport 3502 chopper diagnostics/control:

```powershell
python stepper_control/chopper.py
```

For XPS-Q stage diagnostics/control:

```powershell
python stepper_control/xps_stage.py
```

Edit the configuration block near the bottom of `DAQ_pda_simple.py` to set:

- device and channel names
- trigger mode/filtering
- timing profile
- capture window
- live plotting mode (`main`, `ref`, or `both`)

`live_cmos.py` exposes these options directly on the CLI instead of relying on
an in-file configuration block.

`linear_absorption.py` walks through a two-step acquisition by default:

1. acquire the reference condition (`sample out`, pump blocked)
2. acquire the sample condition (`sample in`, pump blocked)

It saves:

- raw accepted reference lines
- raw accepted sample lines
- processed transmission / absorbance
- run metadata and DAQ setup

`transient_absorption.py` currently uses a manual-stage workflow by default:

1. provide an explicit delay list on the CLI or from a file
2. move the delay stage to each prompted position
3. acquire odd/even demod data for each delay point

It also supports:

- XPS-driven stage motion (`--stage-mode xps`)
- merged multi-scan outputs with scan-to-scan statistics
- optional dark-offset subtraction from a saved baseline file
- optional saving of a new dark-offset baseline from a blocked-pump run
- grouped subaverages and simple outlier masks for each raw delay-point stack

It saves:

- raw accepted lines for every delay point
- processed per-delay pump-on / pump-off / delta spectra
- per-scan aggregated `delta_OD` and `delta_T/T`
- merged multi-scan `delta_OD` / `delta_T/T` means, std, sem, and per-scan stacks
- run metadata, DAQ setup, and delay calibration
- an overview PNG of the merged `delta_OD`

`steady_state_cd.py` currently acquires one odd/even modulation stack and saves:

- raw accepted lines
- grouped sub-averages and simple outlier metrics
- left/right bucket means
- raw and normalized circular-difference spectra

## License

MIT License. See `LICENSE`.
