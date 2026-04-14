# pump-probe

Tools for pump-probe laser spectroscopy experiments controlled by Python.

## What Is Included

- `DAQ_pda_simple.py`: stand-alone NI-DAQmx control and live plotting script for CMOS/PDA-style line acquisition.

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

Edit the configuration block near the bottom of `DAQ_pda_simple.py` to set:

- device and channel names
- trigger mode/filtering
- timing profile
- capture window
- live plotting mode (`main`, `ref`, or `both`)

## License

MIT License. See `LICENSE`.
