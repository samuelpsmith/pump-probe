# pump-probe

Tools for pump-probe laser spectroscopy experiments controlled by Python.

## What Is Included

- `DAQ_pda_simple.py`: stand-alone NI-DAQmx control and live plotting script for CMOS/PDA-style line acquisition.
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

## License

MIT License. See `LICENSE`.
