# Transient Absorption Project Design

## Purpose

This document lays out a proposed architecture for growing the current
`pump-probe` codebase from a bench-debugging toolkit into a reliable optical
spectroscopy platform.

The codebase is no longer just about making the detector read out. It is now
supporting a family of experiments that share hardware, calibrations, data
products, and uncertainty requirements.

The current system already has several working pieces:

- live CMOS/DAQ acquisition and timing control in `DAQ_pda_hard.py`
- chopper control and synchronization in `stepper_control/chopper.py`
- stage-control scripts for delay and raster hardware in `stepper_control/`

Those pieces are strong prototypes, but they are still organized mostly around
individual devices. The next stage of the project should organize the code
around **tools**, **experiments**, **calibrations**, and **data products**.


## Scope

This design targets three time horizons.

### Near term

- reliable linear absorption measurements
- reliable transient absorption measurements
- reproducible saving of calibrations and datasets
- clear separation between live setup tools and experiment-acquisition tools
- a path toward steady-state CD without redesigning the architecture

### Medium term

- main/ref referenced detection
- automatic multi-scan averaging and scan comparison
- instrument-response characterization
- pump wavelength and experimental-parameter sweeps
- better uncertainty and outlier handling

### Long term

- multi-state modulation experiments
- polarization/circular dichroism experiments
- support for more complex pulse-sequence labeling and demodulation
- richer calibration families beyond wavelength/delay/dark offsets


## Conceptual Split: Steady-State vs Time-Resolved

Yes, I recommend that we conceptually split the experiment layer into:

1. **steady-state experiments**
2. **time-resolved experiments**

This is a better top-level split than dividing only by specific observables.

### Steady-state experiments

These do not require a delay line.

Examples:

- linear absorption
- steady-state circular dichroism
- later, other steady-state modulated observables

### Time-resolved experiments

These use a delay axis or equivalent temporal control.

Examples:

- transient absorption
- blank-response / instrument-response characterization
- later, more advanced pump-probe pulse-sequence experiments

### How this changes design principles

This split changes the architecture in useful ways:

- the delay stage becomes an **optional capability**, not a global assumption
- steady-state and time-resolved experiments can share the same DAQ,
  calibration, metadata, and storage layers
- experiment selection becomes more intuitive for users
- future CD work fits naturally without being awkwardly forced into a TAS-only
  worldview


## Scientific Use Cases

### 1. Linear Absorption

The user wants to:

- block the pump
- measure probe spectrum without sample
- measure probe spectrum with sample
- convert pixel index to wavelength or wavenumber
- convert detector signal to transmission, absorbance, or optical density
- repeat the linear absorption measurement at the beginning and end of a TAS run

Important note:

- the first implementation can be single-line only
- we should still design the processing layer so a later switch to `main/ref`
  referenced detection does not require rewriting the experiment structure

### 2. Transient Absorption

The user wants to:

- chop pump at `500 Hz` while laser/line acquisition runs at `1 kHz`
- measure odd/even differential signal from the line detector
- subtract a stored blocked-pump dark/differential offset
- move the delay stage in discrete steps
- convert stage position to optical delay in ps
- average for a configurable number of pulses or accepted lines
- repeat scans across the delay axis
- merge multiple scans later into a final dataset

Important note:

- the live DAQ view is primarily a setup/diagnostic tool
- the actual experiment runner should prioritize reproducibility and metadata,
  not fast interactivity

### 3. Steady-State Circular Dichroism

The user wants a future path toward steady-state CD.

That implies:

- no delay stage in the core measurement loop
- polarization-sensitive acquisition or modulation
- polarization calibration data
- a state-labeled or modulation-aware processing path

This is a strong reason to keep the architecture flexible with respect to both
calibration types and demodulation state labels.

### 4. Future Modulation Experiments

The user expects eventually to support experiments where the signal is encoded
in more than two states, for example:

- polarization modulation
- circular dichroism experiments
- multiple synchronized modulators
- pump wavelength scans

This means the architecture should avoid hard-coding "odd/even only" logic into
the core experiment model.


## Design Principles

1. Separate setup tools from production acquisition
   - `DAQ_pda_hard.py` can remain a powerful live setup/debug tool.
   - Actual experiment scripts should be smaller, stricter, and easier to
     audit.

2. Separate hardware control from experiment logic
   - DAQ, chopper, delay stage, and raster stage should each have device-layer
     interfaces.
   - Experiment code should say "acquire a line" or "move to delay" rather than
     dealing directly with DAQmx task details or vendor DLL calls.

3. Treat calibrations as first-class data
   - wavelength calibration
   - pixel mask / bad-pixel mask
   - dark offsets
   - sample-in / sample-out references
   - delay-zero and stage-to-ps calibration
   - polarization calibration
   - future instrument-response calibrations

4. Preserve reproducibility
   - every dataset should record:
     - timing profile
     - DAQ mode
     - integration count
     - chopper configuration
     - stage positions
     - calibration versions used
     - software version / git commit

5. Preserve information rather than prematurely discarding it
   - keep more raw and intermediate data when in doubt
   - save enough sub-averaging/statistical structure to support later
     uncertainty analysis and outlier review
   - assume we may later learn that some currently "unimportant" quantity is
     scientifically useful

6. Design for future labeled-state demodulation
   - today: two-state odd/even pump chop
   - later: N-state modulation with labeled demod channels

7. Separate **tools** from **experiments**
   - tools are flexible, interactive, bench-facing utilities
   - experiments are reproducible, config-driven acquisition procedures
   - tools may share acquisition code with experiments, but they should not
     become the experiment layer


## Proposed Project Structure

The current repo is still script-heavy. The next architecture should migrate
toward a package layout like this:

```text
pump_probe/
  config/
    defaults.py
    schemas.py
    profiles.py
  hardware/
    daq/
      controller.py
      acquisition_session.py
      timing_profiles.py
      line_models.py
      task_builders.py
    chopper/
      controller.py
      config.py
    stages/
      delay_stage.py
      raster_stage.py
      calibration.py
  calibration/
    wavelength.py
    delay.py
    detector.py
    baseline.py
    polarization.py
    models.py
    registry.py
  processing/
    linear_absorption.py
    transient_absorption.py
    steady_state_cd.py
    demodulation.py
    averaging.py
    outliers.py
    uncertainty.py
    units.py
  experiments/
    base.py
    steady_state_base.py
    time_resolved_base.py
    linear_absorption.py
    steady_state_cd.py
    transient_absorption.py
    scan_plan.py
    metadata.py
  io/
    dataset_store.py
    naming.py
    serialization.py
    export.py
  tools/
    live_cmos.py
    live_demod.py
    chopper_status.py
    stage_probe.py
  cli/
    linear_absorption.py
    steady_state_cd.py
    transient_absorption.py
    calibrate_wavelength.py
    calibrate_delay.py
    calibrate_polarization.py
  ui/
    live_status.py
    experiment_status.py
docs/
  transient_absorption_design.md
```

This does **not** all need to be implemented immediately. It is a target
structure to guide refactoring.


## Current Code Roles

### Keep

- `DAQ_pda_hard.py`
  - near-term live diagnostic and setup interface
  - timing validation
  - pump-chop sanity check
  - trigger monitoring

- `stepper_control/chopper.py`
  - direct chopper configuration and status

- `stepper_control/xps_stage.py`
  - direct delay-stage probing and move validation

### Deprecate / Remove Later

- `DAQ_pda_simple.py`
  - now largely obsolete
  - should likely be removed once the shared DAQ acquisition module exists and
    `DAQ_pda_hard.py` has been refactored to use it

### Likely Refactor Targets

- DAQ acquisition logic in `DAQ_pda_hard.py`
- stage abstractions spread across the `stepper_control` folder
- calibration persistence, which is currently mostly implicit/manual

### Recommended Interpretation

The project should have three top-level conceptual categories:

1. **hardware**
   - thin wrappers over specific devices

2. **tools**
   - live plotting, timing checks, direct status/config utilities
   - flexible bench-facing workflows

3. **experiments**
   - structured, config-driven, reproducible acquisition procedures

This lets us preserve the useful role of `DAQ_pda_hard.py` without forcing it
to be the final experiment orchestration layer.


## Core Data Model

The project should operate on a few explicit in-memory objects.

### 1. RawLine

Represents one accepted detector read.

Suggested fields:

- `sample_index`
- `voltage_main`
- `voltage_ref` (optional)
- `timestamp`
- `trigger_counter` or trigger metadata if available
- `state_label` (optional now, required later for full robustness)
- `stage_position_mm` (optional)
- `timing_profile_name`

### 2. IntegratedSpectrum

Represents an average over many raw lines.

Suggested fields:

- `signal_main`
- `signal_ref` (optional)
- `signal_diff`
- `n_lines`
- `n_pulse_groups`
- `grouping_size`
- `acquisition_duration_s`
- `wavelength_nm` (optional attached axis)
- `wavenumber_cm_inv` (optional attached axis)

### 3. Calibration Objects

Separate calibration payloads:

- `WavelengthCalibration`
- `DelayCalibration`
- `DetectorBaselineCalibration`
- `LinearAbsorptionReference`
- `PolarizationCalibration`
- future calibration types registered under a common calibration interface

Each should carry:

- creation date
- operator notes
- software version
- source dataset(s)
- fit parameters
- units
- optional hardware/profile compatibility tags

### 4. Experiment Dataset

Each experiment should save a structured dataset with:

- run metadata
- scan axis
- raw or partially reduced data
- processed results
- references to calibrations used
- uncertainty/statistical products


## Calibration Design

### Calibration Philosophy

The system should be explicitly flexible to future calibration profiles.

That means:

- calibration objects should share a common envelope
- each calibration type can define its own payload
- experiments should declare which calibration types they require and which are
  optional

This will let us add, for example:

- polarization calibration for steady-state CD
- pump-power calibration
- spectrometer-response calibration
- detector nonlinearity calibration

without redesigning the whole experiment layer.

### A. Pixel to Wavelength / Wavenumber

We need a formal wavelength-calibration workflow.

Possible strategies:

1. Manual known-feature mapping
   - user identifies pixel positions of known spectral lines/features
   - fit polynomial pixel -> wavelength

2. Import from prior calibration file
   - user selects a saved calibration produced on the same spectrometer geometry

3. Lamp-based calibration later
   - if a calibration lamp is introduced, the code should support it without
     changing the data model

Output:

- `pixel -> wavelength_nm`
- derived `pixel -> wavenumber_cm^-1`

Implementation note:
- the wavelength calibration should be stored independently of individual
  experiments, but each experiment should record exactly which calibration file
  was used

### B. Voltage to Transmission / Absorbance / Optical Density

For single-line linear absorption, the initial pipeline can be:

- measure `I0(pixel)` without sample
- measure `I(pixel)` with sample
- compute transmission:
  - `T = I / I0`
- compute absorbance / optical density:
  - `A = -log10(T)`

We should support optional corrections:

- detector dark subtraction
- blocked-light baseline subtraction
- future `main/ref` normalization:
  - `I_norm = main / ref`

Important design choice:
- store both raw voltage and processed absorbance products
- never store only the final absorbance and discard the raw intermediate

### C. Delay Calibration

We need a formal conversion from stage position to time delay in ps.

This should **not** be hard-coded into the experiment script.

Recommended model:

- `delay_ps = scale_ps_per_mm * (position_mm - zero_position_mm)`

Where:

- `zero_position_mm` is the stage position corresponding to time zero
- `scale_ps_per_mm` depends on stage geometry

Current note:

- retroreflector geometry
- likely 4-pass, but still to be confirmed

This means the actual `mm -> ps` factor must be treated as a calibration value,
not assumed from a common textbook case.

The code should allow:

- coarse zero entry by user
- refined zero from blank/sample response later
- different scale factors for different mechanical geometries

### D. Pump-Chop Dark Offset

The newly added blocked-pump differential offset is exactly the right direction.

We should treat this as a saved calibration artifact:

- not just a live convenience
- a named baseline object that can be associated with a run

Recommended metadata:

- saved differential spectrum
- timing profile
- chopper mode
- DAQ mode
- line integration count
- operator note: `pump blocked`

### E. Polarization Calibration

For steady-state CD and later polarization-sensitive experiments, we should
assume we will eventually need calibration objects such as:

- modulator state labeling
- polarization transfer / retardance corrections
- baseline asymmetry corrections

This does not need to be implemented first, but the calibration registry should
be designed so it can absorb this naturally.


## Experiment Layers

### Experiment Families

I recommend a first-class split between:

1. **steady-state experiments**
2. **time-resolved experiments**

### 1. Live Setup Layer

Purpose:

- align beam
- verify spectrum presence
- verify trigger/chopper timing
- tune integration count
- inspect S/N
- inspect differential baseline

Primary tool:

- `DAQ_pda_hard.py`

This tool should remain a flexible bench instrument rather than being forced
into the full experiment orchestration role.

### 2. Linear Absorption Experiment Layer

Suggested workflow:

1. choose or create wavelength calibration
2. acquire sample-out reference spectrum
3. acquire sample-in spectrum
4. save raw and integrated spectra
5. compute:
   - transmission
   - absorbance / optical density
6. save processed results plus metadata

Suggested outputs:

- raw integrated voltage spectra
- wavelength-calibrated spectra
- absorbance spectrum
- plot exports
- machine-readable dataset

Design note:
- linear absorption should be its own experiment type
- the first implementation can be main-only
- the data model should still allow a later referenced-detection variant using
  the same observable pipeline

### 3. Steady-State CD Experiment Layer

Suggested workflow:

1. choose or create wavelength calibration
2. choose or create polarization calibration
3. acquire modulated steady-state spectra
4. save raw state-resolved or demodulated spectra
5. compute the desired steady-state observable
6. save processed results plus metadata

Design note:
- steady-state CD should be modeled as a distinct experiment type, even if some
  observables overlap with linear absorption, because the acquisition logic and
  required calibrations differ

### 4. Transient Absorption Experiment Layer

Suggested workflow:

1. load wavelength calibration
2. load delay calibration
3. optionally acquire/attach blocked-pump differential offset
4. define delay scan axis
5. define integration count per point
6. for each delay:
   - move stage
   - wait for settling
   - acquire N accepted lines
   - form pump-on/pump-off differential
   - save per-point result
7. repeat for multiple scans
8. merge scans into final dataset
9. export TA matrix and summary plots

Important design notes:

- the experiment runner should record both:
  - per-delay averaged result
  - the scan index / replicate identity
- we will want to be able to reject outliers from occasional white-light
  generation errors


## Transient Absorption Signal Model

Near-term, the demodulation path can be:

- odd/even line bucketing
- consistent sign convention
- optional saved blocked-pump offset subtraction

Important risk note from bench validation:

- odd/even demodulation is only trustworthy if the acquisition stream preserves
  a consistent physical pump-on/pump-off ordering
- restarting the DAQ session at every delay point can re-anchor the odd/even
  phase differently from point to point, causing apparent sign flips between
  delay points
- keeping one persistent DAQ session alive across an entire TAS scan is a
  higher-principle fix because it preserves one parity stream and avoids
  restarting the generated PFI3 chopper reference at every point
- however, PFI3 is only the reference signal generated by the DAQ; it is not by
  itself proof of the actual optical chop state at the sample
- for a true hardware-labeled demodulation path, the DAQ should eventually read
  a measured chopper-state TTL (for example the Newport controller's real chop
  output such as `F Outer` or another suitable rear-panel TTL output) or a
  photodiode monitor placed after the chopper
- the architecture should therefore support both:
  - a persistent-session parity-preserving fallback path
  - a preferred explicit state-labeled path using a real hardware modulation
    label per accepted line

Then for each delay point:

- average many odd/even differential lines
- optionally average many independent groups within that point to estimate
  uncertainty

Recommended outputs at each delay:

- `delta_signal_voltage(pixel)`
- `n_accepted_lines`
- `integration_time_s`
- optional `std(pixel)` across sub-averages
- optional line-group statistics for uncertainty estimation and outlier review

Later processing can convert raw differential voltage to more physical
observables such as:

- `?T/T`
- `?A`

That conversion should live in the processing layer, not the device layer.


## Referenced Detection Roadmap

Even though the first linear absorption implementation will use single-line
measurements, the design should assume that `main/ref` will become important.

There are two distinct future use cases:

1. Linear absorption
   - use `main/ref` to stabilize source fluctuations

2. Transient absorption
   - compute differential signal from normalized probe values
   - for example using:
     - `main / ref` before pump-on/pump-off differencing

Architecture implication:
- processing code should operate on an abstract signal object that may be
  either:
  - raw main-only
  - raw main/ref
  - normalized main/ref

That will let us add referenced detection without replacing experiment logic.


## Scan Planning

We should formalize the notion of a scan plan rather than embedding stage moves
directly in an acquisition loop.

Suggested `ScanPlan` concepts:

- scan axis name: `delay_ps`
- point list
- repeats / scans
- integration count per point
- optional settle time after move
- optional randomized order for later drift studies

For TAS, the first useful plan is:

- monotonic delay list
- fixed integration count
- repeat N scans

Later we can support:

- logarithmic delay grids
- dense near-zero and coarse long-delay grids
- pump wavelength nested loops
- randomized delay ordering to decorrelate from long-term drift

Recommended plan modes:

- `ordered`
- `randomized`
- later, possibly `blocked-randomized` or `interleaved-reference`


## Data Storage Strategy

We should store both raw-ish and processed forms.

### Recommended dataset layout

```text
data/
  2026-04-29/
    run_001_linear_absorption_begin/
      metadata.json
      raw_reference.npz
      raw_sample.npz
      processed_absorption.npz
      plots/
    run_002_tas_scan/
      metadata.json
      scan_000.npz
      scan_001.npz
      merged_result.npz
      plots/
```

### File format recommendation

Near-term:

- `json` for metadata
- `npz` for arrays

Possible later expansion:

- `h5` / `hdf5` for large structured datasets

The choice should optimize for:

- easy inspection in Python
- low implementation overhead
- stable metadata capture
- high retention of raw and semi-raw information

### What `npz` and `hdf5` Mean

`npz`
- a NumPy archive format
- essentially a zipped bundle of named arrays
- very easy to read/write from Python
- simple and lightweight
- good for early-stage scientific code where we want low overhead

`hdf5`
- a hierarchical scientific data format
- supports nested groups, large datasets, metadata-like attributes, and more
  elaborate layouts
- excellent for mature scientific projects with large structured datasets
- more powerful, but also more design-heavy

Recommendation:
- start with `npz + json`
- move to `hdf5` later only if dataset complexity or size clearly demands it

### Data Retention Principle

The project should lean toward keeping more data, not less.

Suggested default:

- save final processed results
- save intermediate averaged products
- save enough raw or grouped raw information to:
  - estimate uncertainty later
  - detect drift later
  - revisit reduction choices later

We can always add pruning later, but discarded data cannot be recovered.


## Metadata Requirements

Every experiment dataset should capture at least:

- date/time
- operator
- sample name / notes
- DAQ timing profile
- DAQ runtime mode
- chopper settings
- trigger settings
- detector integration count
- delay-stage configuration
- wavelength calibration identifier
- delay calibration identifier
- dark-offset identifier
- software git commit


## User Experience Design

The codebase should have two different UX layers.

### A. Tool UX

Purpose:

- direct manual control
- plotting and sanity checks
- troubleshooting

Examples:

- `python DAQ_pda_hard.py`
- `python stepper_control/chopper.py`
- `python stepper_control/xps_stage.py`

Design preference:
- tools should be primarily CLI + plots
- avoid making users depend heavily on hidden hotkeys
- warnings and status should be visible but not so noisy that users get lost in
  them

### B. Experiment UX

Purpose:

- fewer knobs
- more structured execution
- reproducible outputs

Examples:

- `python -m pump_probe.cli.linear_absorption --config configs/linear_abs_001.yaml`
- `python -m pump_probe.cli.transient_absorption --config configs/tas_001.yaml`
- `python -m pump_probe.cli.steady_state_cd --config configs/cd_001.yaml`

This is a major design principle:
- **bench tools should be flexible**
- **experiment runners should be strict**


## Configuration Strategy

Experiment scripts should eventually read structured configs, likely YAML or
JSON.

Suggested config sections:

- sample metadata
- hardware selection
- timing profile
- calibration files
- scan plan
- integration settings
- output paths

Example:

```yaml
experiment:
  type: transient_absorption
  sample_name: example_film
daq:
  timing_profile: guard_10us
  runtime_profile: persistent_robust_test
  integration_lines: 512
  pump_chop_sign: -1
calibration:
  wavelength_file: calibrations/wavelength_2026-04-29.json
  delay_file: calibrations/delay_zero_2026-04-29.json
  pump_dark_file: calibrations/pump_dark_2026-04-29.npz
scan:
  mode: ordered
  delay_ps: [-2, -1, 0, 1, 2, 5, 10, 20]
  n_scans: 4
  settle_time_s: 0.2
```


## Processing and Analysis Strategy

We should distinguish clearly between:

1. online reduction
   - enough to monitor the experiment

2. saved intermediate products
   - enough to reprocess later without repeating acquisition

3. offline analysis
   - final plots, fitting, merging, and publication-quality outputs

Recommended online products:

- integrated live line
- integrated odd/even differential
- simple S/N proxies

Recommended saved products:

- per-delay averaged spectra
- replicate/scan identity
- optional sub-averages for error bars
- grouped statistics that preserve uncertainty information
- optional retained raw or semi-raw line groups when storage permits

Recommended offline products:

- merged TA matrix
- kinetic traces
- spectra at selected delays
- baseline-corrected and calibrated outputs

### Uncertainty and Statistics

Uncertainty analysis should be treated as a design requirement, not an
afterthought.

We should aim to preserve enough information to support:

- mean
- standard deviation
- standard error
- confidence intervals
- scan-to-scan reproducibility
- outlier detection
- later re-weighting or robust averaging

In practice this means:

- saving grouped sub-averages
- saving per-scan results, not just merged results
- keeping counts of accepted lines / pulse groups
- keeping quality metrics and acquisition warnings
- determining and printing DAQ measurement figures for each acquisition mode,
  including at minimum code width / volts-per-code, effective least-significant
  digit, and any mode-specific resolution or digitization assumptions that
  affect uncertainty interpretation

For TAS specifically, occasional white-light-generation failures should be
handled with explicit statistics and outlier logic rather than informal visual
judgment alone.


## Handling Instrument Response and Blank Measurements

Blank measurements should be treated as a formal experiment type, not just a
note in a lab book.

Suggested future experiment:

- `blank_response`

Purpose:

- identify coherent artifacts
- estimate time zero
- estimate instrument response width

This does not need to be in the first implementation, but the data model should
already support:

- delay scans without sample-specific assumptions
- saving processed differential traces for later fitting


## Future Compatibility: Multi-State Modulation

To support circular dichroism or other modulation schemes later, we should not
let the transient absorption experiment runner assume that modulation is always
binary.

Long-term abstraction:

- `StateLabeledLine`
  - line plus a state label such as:
    - `pump_on`
    - `pump_off`
    - `left_circular`
    - `right_circular`
    - `reference_only`

- `Demodulator`
  - consumes labeled lines
  - produces one or more derived channels

Near-term implementation can still use binary odd/even demod, but the
processing layer should be written with this future shape in mind.


## Reliability and Safety Considerations

1. Hardware safety
   - stage moves should support soft limits and optional confirmation for large
     moves
   - scripts should not kill unknown processes
   - experiment runner should have explicit start/stop behavior for chopper and
     acquisition

2. Acquisition integrity
   - record actual accepted line counts
   - record warnings about parity resets, queue growth, and dropped lines
   - make experiment runs fail loudly when integrity is not adequate

3. Reproducibility
   - save calibrations and metadata automatically
   - avoid hidden state from prior live sessions


## Proposed Implementation Phases

### Phase 1: Architecture Refactor and Foundations

Estimated effort:
- about 1 to 2 weeks

Goals:
- create package/module structure
- extract shared DAQ acquisition logic out of `DAQ_pda_hard.py`
- preserve a lightweight live plotting tool built on the same acquisition core
- mark `DAQ_pda_simple.py` for removal/deprecation

Deliverables:
- shared DAQ acquisition module
- tools layer
- experiment base layer skeleton
- calibration/data directories and conventions

### Phase 2: Calibration Infrastructure

Estimated effort:
- about 1 week

Goals:
- define calibration file formats
- implement wavelength calibration loader/model
- implement delay calibration loader/model
- implement saved differential dark-offset calibration model

Deliverables:
- calibration schemas
- load/save helpers
- first CLI calibration workflows

### Phase 3: Linear Absorption MVP

Estimated effort:
- about 1 week

Goals:
- sample-in / sample-out single-line acquisition
- transmission / absorbance processing
- metadata-rich saved outputs

Deliverables:
- linear absorption experiment runner
- quick-look plots
- reproducible datasets

### Phase 4: Transient Absorption MVP

Estimated effort:
- about 2 weeks

Goals:
- scripted delay scan
- configurable averaging
- multiple repeated scans
- saved blocked-pump dark subtraction
- saved per-scan and merged outputs

Deliverables:
- TAS experiment runner
- scan-plan support
- merged TA matrix outputs
- basic uncertainty products

### Phase 5: Uncertainty and Robustness Pass

Estimated effort:
- about 1 week

Goals:
- grouped statistics
- outlier handling
- scan comparison
- quality metrics and warnings in saved outputs

Deliverables:
- robust averaging helpers
- uncertainty-aware saved products

### Phase 6: Steady-State CD Planning / First Implementation

Estimated effort:
- about 1 to 2 weeks after modulation hardware/calibration details are clear

Goals:
- define CD experiment type
- define polarization calibration requirements
- implement steady-state CD acquisition and reduction path

Deliverables:
- steady-state CD experiment runner
- polarization calibration model

### Total Near-Term Program

For linear absorption + TAS with strong foundations:
- roughly 6 to 8 weeks of focused development

For adding first steady-state CD support on top:
- roughly 1 to 2 additional weeks once hardware/calibration details are known


## Immediate Recommendations

The next implementation steps I would prioritize are:

1. add a `docs/`, `data/`, and `calibration` mindset to the repo
2. refactor DAQ acquisition logic out of `DAQ_pda_hard.py` into shared modules
3. keep a lightweight live plotting tool built on the same acquisition core
4. build a formal wavelength-calibration data model
5. build a formal delay-calibration data model
6. create a first real `linear_absorption` experiment runner
7. create a first real `transient_absorption` experiment runner
8. plan the steady-state CD experiment type in parallel so the architecture
   stays modulation-friendly


## Remaining Planning Inputs

To fully plan a project of this scope, the most useful remaining inputs are:

1. confirmed delay-line geometry
   - especially the true pass count / optical path relation

2. wavelength-calibration source
   - existing mapping vs manual points for the first calibration

3. steady-state CD hardware details
   - what modulation hardware is used
   - what labels/states we expect
   - what calibration quantities are required

4. expected run sizes
   - rough number of delay points
   - rough number of scans
   - rough integration sizes
   - this helps us decide when `npz + json` stops being enough

5. desired default outputs
   - what users expect to see saved automatically after each experiment

6. user metadata expectations
   - operator/sample/project naming conventions
   - this matters for reproducible dataset layout


## Notes from Current Decisions

At the time of this draft, the following preferences have been expressed:

- primary experiment UX should be CLI + config
- tool UX should be CLI + plots
- linear absorption should start main-only, but data structures should support
  later referenced detection
- TAS averaging should be exposed as lines per delay point, while also reporting
  pulse-group information such as grouping size
- saving more data is preferred over prematurely trimming data
- random scan ordering should be supported for drift-decoupling studies
- the delay line is believed to be a retroreflector geometry, probably 4-pass,
  but still needs confirmation


## Summary

The codebase is at the point where the scientific workflows are becoming clear.
That is exactly the right time to split the system conceptually into:

- hardware wrappers
- live setup tools
- experiment runners
- calibration models
- saved datasets
- offline processing

If we do that carefully now, we can support near-term linear absorption and TAS,
add steady-state CD naturally, and leave room for future multi-state modulation
experiments without needing another full rewrite.
