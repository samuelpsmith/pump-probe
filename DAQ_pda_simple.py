import numpy as np
import nidaqmx
import matplotlib.pyplot as plt
import time
from contextlib import nullcontext
from collections import deque
from nidaqmx.constants import (
    AcquisitionType,
    Edge,
    Level,
    TerminalConfiguration,
)


class PDAControllerDAQSimple:
    """
    Simple NI-6363 PDA/CMOS control path:
    - ctr0 outputs ST on PFI8
    - ctr1 outputs CLK on PFI4
    - AI is sampled by ctr1 internal output (one sample per CLK pulse)
    - AI and CLK are started from ST internal output for fixed phase alignment
    """

    INITIAL_GUESS_TIMING = {
        "description": "Initial first-pass timing with 2.0 MHz-compatible clocking.",
        # Current best values from initial_guess profile in daq_pda_pt2.py
        "st_high_time": 50e-6,# sets the ST fall time
        "st_low_time": 2.5e-3, # the remainder of the ST pulse cycle
        "st_initial_delay": 0.0, #sets the ST rise time
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
    }

    IMPROVED_GUESS_TIMING = {
        "description": "Built for SPEED.",
        "st_high_time": 4e-6,
        "st_low_time": 1000e-6, #2.372e-3
        "st_initial_delay": 0.0,
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
    }

    IMPROVED_GUESS_TIMING_2CHANNEL = {
        "description": "Built for SPEED.",
        "st_high_time": 4e-6,
        "st_low_time": 1000e-6, #2.372e-3
        "st_initial_delay": 0.0,
        "clk_high_time": 500e-9,
        "clk_low_time": 500e-9,
        "clk_initial_delay": 0.0,
    }


    TOOFAST_GUESS_TIMING = {
        "description": "Possibily detrimental?.",
        "st_high_time": 2e-6, #minimum is 4us
        "st_low_time": 1e-7, # minimum is 7.5us
        "st_initial_delay": 0.0, #minimum is 0
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
    }

    # Named profiles make it explicit which timing set is active.
    TIMING_PROFILES = {
        "initial_guess": INITIAL_GUESS_TIMING,
        "improved_guess": IMPROVED_GUESS_TIMING,
        "improved_guess_2channel": IMPROVED_GUESS_TIMING_2CHANNEL,
        "toofast_guess": TOOFAST_GUESS_TIMING,
    }
    DEFAULT_TIMING_PROFILE = "improved_guess_2channel"

    def __init__(
        self,
        device="Dev1",
        num_pixels=1024,
        ai_main="ai2",
        ai_ref="ai0",
        trig_pfi="PFI9",
        st_pfi="PFI8",
        clk_pfi="PFI4",
    ):
        self.device = str(device)
        self.num_pixels = int(num_pixels)

        self.video_main = f"{self.device}/{ai_main}"
        self.video_ref = f"{self.device}/{ai_ref}"
        self.trig_in = f"/{self.device}/{trig_pfi}"
        self.st_out_term = f"/{self.device}/{st_pfi}"
        self.clk_out_term = f"/{self.device}/{clk_pfi}"

        self.st_counter = f"{self.device}/ctr0"
        self.clk_counter = f"{self.device}/ctr1"
        self.st_internal_output = f"/{self.device}/Ctr0InternalOutput"
        self.ai_sample_clk_src = f"/{self.device}/Ctr1InternalOutput"

        self.st_high_time = 0.0
        self.st_low_time = 0.0
        self.st_initial_delay = 0.0
        self.clk_high_time = 0.0
        self.clk_low_time = 0.0
        self.clk_initial_delay = 0.0

        self.ai_min = -10.0
        self.ai_max = 10.0
        self.ai_terminal_config = TerminalConfiguration.RSE
        self.ai_max_conversion_rate = 2.0e6  # NI-6363 single-channel budget
        # Stay below the hard conversion ceiling for better settling margin.
        self.ai_recommended_utilization = 0.85
        self._scan_rate_warned_channels = set()

        self.use_external_trigger = False
        self.trigger_edge = Edge.RISING
        # Keep filter off by default; many external sync pulses are narrower
        # than microseconds and would be rejected by aggressive filtering.
        self.trigger_filter_enable = False
        self.trigger_filter_min_pulse_width_s = 0.2e-6
        self._trigger_filter_warned = False

        # Legacy-like windowing around active video
        self.ai_ignored_samples = 0 #Was 16
        self.ai_trailing_samples = 0 # Was 10
        self.crop_output_to_valid_pixels = False
        self.timing_profile_name = self.DEFAULT_TIMING_PROFILE

        self.apply_initial_guess_timing()

    # -----------------------------
    # Configuration
    # -----------------------------
    def apply_timing_profile(self, profile_name):
        key = str(profile_name).strip().lower()
        if key not in self.TIMING_PROFILES:
            valid = ", ".join(sorted(self.TIMING_PROFILES))
            raise ValueError(
                f"Unknown timing profile '{profile_name}'. Valid options: {valid}"
            )
        self.timing_profile_name = key
        self.apply_initial_guess_timing()

    def apply_initial_guess_timing(self):
        cfg = self.TIMING_PROFILES.get(
            self.timing_profile_name, self.INITIAL_GUESS_TIMING
        )
        self.set_st_timing(
            high_time_s=cfg["st_high_time"],
            low_time_s=cfg["st_low_time"],
            initial_delay_s=cfg["st_initial_delay"],
        )
        self.set_clk_timing(
            high_time_s=cfg["clk_high_time"],
            low_time_s=cfg["clk_low_time"],
            initial_delay_s=cfg["clk_initial_delay"],
        )

    def set_st_timing(self, high_time_s, low_time_s, initial_delay_s=0.0):
        self.st_high_time = float(high_time_s)
        self.st_low_time = float(low_time_s)
        self.st_initial_delay = float(initial_delay_s)

    def set_clk_timing(self, high_time_s, low_time_s, initial_delay_s=0.0):
        self.clk_high_time = float(high_time_s)
        self.clk_low_time = float(low_time_s)
        self.clk_initial_delay = float(initial_delay_s)

    def enable_external_trigger(self, enable=True, edge=Edge.RISING):
        self.use_external_trigger = bool(enable)
        self.trigger_edge = edge

    def set_trigger_filter(self, enable=True, min_pulse_width_s=2e-6):
        self.trigger_filter_enable = bool(enable)
        self.trigger_filter_min_pulse_width_s = float(min_pulse_width_s)
        if self.trigger_filter_min_pulse_width_s < 0:
            raise ValueError("min_pulse_width_s must be >= 0.")

    def _apply_trigger_filter_to_start_trigger(self, start_trigger):
        """
        Apply digital filter on external trigger path when supported.
        """
        try:
            start_trigger.dig_edge_dig_fltr_enable = bool(self.trigger_filter_enable)
            if self.trigger_filter_enable:
                start_trigger.dig_edge_dig_fltr_min_pulse_width = float(
                    self.trigger_filter_min_pulse_width_s
                )
        except Exception as exc:
            if not self._trigger_filter_warned:
                print(
                    "Warning: could not apply trigger digital filter on "
                    f"{self.trig_in}: {exc}"
                )
                self._trigger_filter_warned = True

    def set_line_windowing(self, enable=True, ignored_samples=16, trailing_samples=10):
        if not enable:
            self.ai_ignored_samples = 0
            self.ai_trailing_samples = 0
            return
        self.ai_ignored_samples = max(0, int(ignored_samples))
        self.ai_trailing_samples = max(0, int(trailing_samples))

    def set_capture_window_samples(self, total_samples, ignored_samples=0):
        """
        Set total acquired samples per line directly.

        total_samples = num_pixels + ignored_samples + trailing_samples
        """
        total = int(total_samples)
        ignored = int(ignored_samples)
        if total < self.num_pixels:
            raise ValueError(
                f"total_samples ({total}) must be >= num_pixels ({self.num_pixels})."
            )
        if ignored < 0:
            raise ValueError("ignored_samples must be >= 0.")

        trailing = total - self.num_pixels - ignored
        if trailing < 0:
            raise ValueError(
                "ignored_samples is too large for requested total_samples and num_pixels."
            )
        self.set_line_windowing(
            enable=True,
            ignored_samples=ignored,
            trailing_samples=trailing,
        )

    def set_output_cropping(self, enable=True):
        self.crop_output_to_valid_pixels = bool(enable)

    @property
    def clk_rate(self):
        return 1.0 / (self.clk_high_time + self.clk_low_time)

    @property
    def ai_samples_per_line(self):
        return int(self.num_pixels + self.ai_ignored_samples + self.ai_trailing_samples)

    @property
    def output_samples_per_line(self):
        if self.crop_output_to_valid_pixels:
            return int(self.num_pixels)
        return int(self.ai_samples_per_line)

    @property
    def sample_window_s(self):
        # Window duration between first and last sample edges.
        return max(0, self.ai_samples_per_line - 1) / self.clk_rate

    def estimate_line_timing(self, trigger_frequency_hz=None):
        sample_window_s = self.sample_window_s
        total_trigger_to_end_s = (
            self.st_initial_delay + self.clk_initial_delay + sample_window_s
        )
        timing = {
            "clk_rate_hz": self.clk_rate,
            "clk_period_s": 1.0 / self.clk_rate,
            "st_high_s": self.st_high_time,
            "st_low_s": self.st_low_time,
            "st_initial_delay_s": self.st_initial_delay,
            "clk_initial_delay_s": self.clk_initial_delay,
            "sample_window_s": sample_window_s,
            "total_trigger_to_end_s": total_trigger_to_end_s,
            "samples_per_line_read": self.ai_samples_per_line,
            "samples_per_line_output": self.output_samples_per_line,
        }
        if trigger_frequency_hz is not None and trigger_frequency_hz > 0:
            period_s = 1.0 / float(trigger_frequency_hz)
            timing["trigger_frequency_hz"] = float(trigger_frequency_hz)
            timing["trigger_period_s"] = period_s
            timing["timing_margin_s"] = period_s - total_trigger_to_end_s
        return timing

    def get_timing_diagnostics(self, trigger_frequency_hz=None):
        clk_period_s = 1.0 / self.clk_rate
        st_rise_s = self.st_initial_delay
        st_fall_s = self.st_initial_delay + self.st_high_time
        # clk_initial_delay is relative to ST-triggered CLK task start.
        clk_start_s = self.st_initial_delay + self.clk_initial_delay
        st_to_clk_s = clk_start_s - st_fall_s
        st_to_clk_clks = st_to_clk_s / clk_period_s
        capture_window_s = self.sample_window_s
        capture_end_s = clk_start_s + capture_window_s

        timing = self.estimate_line_timing(trigger_frequency_hz=trigger_frequency_hz)
        return {
            "reference_event": (
                "PFI9 trigger edge" if self.use_external_trigger else "software start"
            ),
            "trigger_in_pfi9_s": 0.0 if self.use_external_trigger else None,
            "st_rise_s": st_rise_s,
            "st_fall_s": st_fall_s,
            "clk_start_s": clk_start_s,
            "st_end_to_clk_start_s": st_to_clk_s,
            "st_end_to_clk_start_clocks": st_to_clk_clks,
            "capture_window_s": capture_window_s,
            "capture_end_s": capture_end_s,
            "clk_rate_hz": self.clk_rate,
            "samples_per_line_read": self.ai_samples_per_line,
            "timing_margin_s": timing.get("timing_margin_s"),
            "trigger_period_s": timing.get("trigger_period_s"),
        }

    def format_timing_diagnostics(self, trigger_frequency_hz=None):
        d = self.get_timing_diagnostics(trigger_frequency_hz=trigger_frequency_hz)
        lines = [
            f"Ref: {d['reference_event']}",
            (
                "Trigger In (PFI9): 0.0 us"
                if d["trigger_in_pfi9_s"] is not None
                else "Trigger In (PFI9): disabled"
            ),
            f"ST rise: {d['st_rise_s'] * 1e6:.1f} us",
            f"ST fall: {d['st_fall_s'] * 1e6:.1f} us",
            f"CLK start: {d['clk_start_s'] * 1e6:.1f} us",
            (
                "ST fall -> CLK start: "
                f"{d['st_end_to_clk_start_s'] * 1e6:.1f} us "
                f"({d['st_end_to_clk_start_clocks']:.1f} clocks)"
            ),
            (
                "Capture window: "
                f"{d['capture_window_s'] * 1e6:.1f} us "
                f"({d['samples_per_line_read']} samples)"
            ),
            f"Capture end: {d['capture_end_s'] * 1e6:.1f} us",
        ]
        if d["timing_margin_s"] is not None and d["trigger_period_s"] is not None:
            lines.append(
                "Margin @ "
                f"{(1.0 / d['trigger_period_s']):.0f} Hz: "
                f"{d['timing_margin_s'] * 1e6:.1f} us"
            )
        return "\n".join(lines)

    def _validate_scan_rate(self, num_ai_channels=1):
        f_clk = self.clk_rate
        if f_clk <= 0.0:
            raise ValueError("CLK rate must be > 0.")
        channels = max(1, int(num_ai_channels))
        hard_limit_scan_rate = self.ai_max_conversion_rate / channels
        if f_clk > hard_limit_scan_rate:
            raise ValueError(
                f"Requested sample clock {f_clk:.3f} Hz exceeds NI-6363 "
                f"limit for {channels} channel(s): {hard_limit_scan_rate:.3f} Hz."
            )
        recommended_max_scan_rate = hard_limit_scan_rate * float(
            self.ai_recommended_utilization
        )
        if f_clk > recommended_max_scan_rate and channels not in self._scan_rate_warned_channels:
            print(
                "Warning: sample clock is near NI-6363 conversion ceiling "
                f"for {channels} channel(s): requested={f_clk:.0f} Hz, "
                f"recommended<={recommended_max_scan_rate:.0f} Hz "
                f"(hard max {hard_limit_scan_rate:.0f} Hz). "
                "You may see settling warnings (200011) or reduced accuracy."
            )
            self._scan_rate_warned_channels.add(channels)

    # -----------------------------
    # Task Builders
    # -----------------------------
    def _build_ai_task(self, read_reference=False):
        ai_task = nidaqmx.Task("PDA_AI_SIMPLE")
        ai_task.ai_channels.add_ai_voltage_chan(
            physical_channel=self.video_main,
            min_val=self.ai_min,
            max_val=self.ai_max,
            terminal_config=self.ai_terminal_config,
        )
        if read_reference:
            ai_task.ai_channels.add_ai_voltage_chan(
                physical_channel=self.video_ref,
                min_val=self.ai_min,
                max_val=self.ai_max,
                terminal_config=self.ai_terminal_config,
            )

        # AI is explicitly clocked from the counter-generated CLK pulses.
        ai_task.timing.cfg_samp_clk_timing(
            rate=self.clk_rate,
            source=self.ai_sample_clk_src,
            active_edge=Edge.RISING,
            sample_mode=AcquisitionType.FINITE,
            samps_per_chan=self.ai_samples_per_line,
        )
        return ai_task

    def _build_st_task(self):
        st_task = nidaqmx.Task("PDA_ST_SIMPLE")
        st_task.co_channels.add_co_pulse_chan_time(
            counter=self.st_counter,
            idle_state=Level.LOW,
            initial_delay=self.st_initial_delay,
            low_time=self.st_low_time,
            high_time=self.st_high_time,
        )
        st_task.co_channels[0].co_pulse_term = self.st_out_term
        st_task.timing.cfg_implicit_timing(
            sample_mode=AcquisitionType.FINITE,
            samps_per_chan=1,
        )
        return st_task

    def _build_clk_task(self):
        clk_task = nidaqmx.Task("PDA_CLK_SIMPLE")
        clk_task.co_channels.add_co_pulse_chan_time(
            counter=self.clk_counter,
            idle_state=Level.LOW,
            initial_delay=self.clk_initial_delay,
            low_time=self.clk_low_time,
            high_time=self.clk_high_time,
        )
        clk_task.co_channels[0].co_pulse_term = self.clk_out_term
        clk_task.timing.cfg_implicit_timing(
            sample_mode=AcquisitionType.FINITE,
            samps_per_chan=self.ai_samples_per_line,
        )
        return clk_task

    def _apply_start_trigger_chain(self, ai_task, st_task, clk_task):
        ai_task.triggers.start_trigger.cfg_dig_edge_start_trig(
            trigger_source=self.st_internal_output,
            trigger_edge=Edge.RISING,
        )
        clk_task.triggers.start_trigger.cfg_dig_edge_start_trig(
            trigger_source=self.st_internal_output,
            trigger_edge=Edge.RISING,
        )
        if self.use_external_trigger:
            st_task.triggers.start_trigger.cfg_dig_edge_start_trig(
                trigger_source=self.trig_in,
                trigger_edge=self.trigger_edge,
            )
            self._apply_trigger_filter_to_start_trigger(st_task.triggers.start_trigger)

    @staticmethod
    def _set_retriggerable(task, enable):
        task.triggers.start_trigger.retriggerable = bool(enable)

    def _extract_valid_pixels(self, data):
        arr = np.asarray(data, dtype=float)
        if not self.crop_output_to_valid_pixels:
            return arr

        start = int(self.ai_ignored_samples)
        stop = int(start + self.num_pixels)
        if arr.ndim == 1:
            if arr.size < stop:
                raise RuntimeError(
                    f"Expected at least {stop} samples, got {arr.size}."
                )
            return arr[start:stop].copy()
        if arr.ndim == 2:
            if arr.shape[1] < stop:
                raise RuntimeError(
                    f"Expected at least {stop} samples/channel, got {arr.shape[1]}."
                )
            return arr[:, start:stop].copy()
        return arr

    def _format_ai_read_data(self, data, read_reference=False):
        data = self._extract_valid_pixels(data)
        if not read_reference:
            if data.ndim == 2 and data.shape[0] == 1:
                return data[0].copy()
            return data
        if not (data.ndim == 2 and data.shape[0] == 2):
            raise RuntimeError(
                "Dual-channel read expected shape (2, n_samples), "
                f"got {tuple(data.shape)}."
            )
        return {
            "main": data[0].copy(),
            "reference": data[1].copy(),
        }

    # -----------------------------
    # Acquisition
    # -----------------------------
    def acquire_line(self, timeout=10.0, read_reference=False):
        num_ai_channels = 2 if read_reference else 1
        self._validate_scan_rate(num_ai_channels=num_ai_channels)

        with self._build_ai_task(read_reference=read_reference) as ai_task, \
             self._build_st_task() as st_task, \
             self._build_clk_task() as clk_task:

            self._apply_start_trigger_chain(ai_task, st_task, clk_task)

            # Arm in downstream-to-upstream order.
            ai_task.start()
            clk_task.start()
            st_task.start()

            data = ai_task.read(
                number_of_samples_per_channel=self.ai_samples_per_line,
                timeout=timeout,
            )

        return self._format_ai_read_data(data, read_reference=read_reference)


class _RetriggerLineSession:
    """
    Persistent retriggered acquisition session.

    Arms AI/CLK/ST once, then returns one line per trigger with much lower
    software overhead than rebuilding tasks every line.
    """

    def __init__(self, pda, ai_buffer_lines=1024, read_reference=False):
        self.pda = pda
        self.ai_buffer_lines = max(64, int(ai_buffer_lines))
        self.read_reference = bool(read_reference)
        self.ai_task = None
        self.st_task = None
        self.clk_task = None
        self._buffer_size_warned = False

    def __enter__(self):
        if not self.pda.use_external_trigger:
            raise ValueError(
                "Persistent retrigger session requires external trigger enabled."
            )

        channels = 2 if self.read_reference else 1
        self.pda._validate_scan_rate(num_ai_channels=channels)

        self.ai_task = self.pda._build_ai_task(read_reference=self.read_reference)
        # Give retriggered reads a larger cushion before overwrite.
        # Buffer units are samples/channel.
        try:
            self.ai_task.in_stream.input_buf_size = int(
                self.ai_buffer_lines * self.pda.ai_samples_per_line
            )
        except Exception as exc:
            if not self._buffer_size_warned:
                print(
                    "Warning: could not set explicit AI input buffer size; "
                    f"continuing with driver default. Detail: {exc}"
                )
                self._buffer_size_warned = True
        self.st_task = self.pda._build_st_task()
        self.clk_task = self.pda._build_clk_task()

        self.pda._apply_start_trigger_chain(self.ai_task, self.st_task, self.clk_task)
        self.pda._set_retriggerable(self.ai_task, True)
        self.pda._set_retriggerable(self.clk_task, True)
        self.pda._set_retriggerable(self.st_task, True)

        # Arm downstream first.
        self.ai_task.start()
        self.clk_task.start()
        self.st_task.start()
        return self

    def read_line(self, timeout=10.0):
        data = self.ai_task.read(
            number_of_samples_per_channel=self.pda.ai_samples_per_line,
            timeout=float(timeout),
        )
        return self.pda._format_ai_read_data(data, read_reference=self.read_reference)

    def close(self):
        for task in (self.st_task, self.clk_task, self.ai_task):
            if task is None:
                continue
            try:
                task.stop()
            except Exception:
                pass
            try:
                task.close()
            except Exception:
                pass
        self.ai_task = None
        self.st_task = None
        self.clk_task = None

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class _PFI9EdgeMonitorSession:
    """
    Monitor PFI9 digital activity using a spare counter.

    Reports edge-rate (Hz). This is not an analog-voltage waveform.
    """

    def __init__(self, pda, counter="ctr2"):
        self.pda = pda
        self.counter = f"{self.pda.device}/{str(counter).lstrip('/')}"
        self.task = None
        self.last_count = 0
        self.last_t = 0.0
        self.available = False
        self.error_text = ""

    def __enter__(self):
        try:
            self.task = nidaqmx.Task("PDA_PFI9_MON")
            self.task.ci_channels.add_ci_count_edges_chan(
                counter=self.counter,
                edge=Edge.RISING,
                initial_count=0,
            )
            self.task.ci_channels[0].ci_count_edges_term = self.pda.trig_in
            self.task.start()
            self.last_count = int(self.task.read())
            self.last_t = time.perf_counter()
            self.available = True
        except Exception as exc:
            self.error_text = str(exc)
            self.available = False
            self.close()
        return self

    def read_rate(self):
        if self.task is None or not self.available:
            return float("nan"), None
        now = time.perf_counter()
        count = int(self.task.read())
        dt = max(now - self.last_t, 1e-6)
        dc = count - self.last_count
        if dc < 0:
            dc = 0
        rate_hz = float(dc / dt)
        self.last_t = now
        self.last_count = count
        return rate_hz, count

    def close(self):
        if self.task is not None:
            try:
                self.task.stop()
            except Exception:
                pass
            try:
                self.task.close()
            except Exception:
                pass
        self.task = None

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def run_live_plot(
    pda,
    integration_line_count=128,
    plot_raw_line=True,
    live_video_mode="main",
    expected_trigger_hz=1000.0,
    acquisition_timeout_s=10.0,
    use_persistent_session=False,
    plot_update_every_n_lines=4,
    timing_text_update_every_n_lines=12,
    autoscale_every_n_plot_updates=3,
    monitor_pfi9=True,
    pfi9_monitor_counter="ctr2",
    trigger_plot_history=240,
):
    def _is_buffer_overwrite_error(exc):
        code = getattr(exc, "error_code", None)
        if code == -200222:
            return True
        text = str(exc).lower()
        return ("-200222" in text) or ("input buffer overwrite" in text)

    mode_key = str(live_video_mode).strip().lower()
    if mode_key not in ("main", "ref", "both"):
        raise ValueError("live_video_mode must be 'main', 'ref', or 'both'.")
    read_reference = mode_key == "both"
    ref_only = mode_key == "ref"
    pda._validate_scan_rate(num_ai_channels=(2 if read_reference else 1))

    x = np.arange(pda.output_samples_per_line)

    plt.ion()
    if monitor_pfi9:
        fig, (ax, ax_trig) = plt.subplots(
            2,
            1,
            figsize=(10, 5.7),
            gridspec_kw={"height_ratios": [3.2, 1.3]},
            sharex=False,
        )
    else:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax_trig = None
    single_label = "Main" if not ref_only else "Ref"
    raw_line_plot, = ax.plot(
        x,
        np.zeros_like(x, dtype=float),
        linewidth=1.0,
        alpha=0.4,
        label=f"{single_label} raw",
    )
    integrated_line_plot, = ax.plot(
        x,
        np.zeros_like(x, dtype=float),
        linewidth=1.8,
        label=f"{single_label} integrated ({integration_line_count} lines)",
    )
    if read_reference:
        ref_raw_line_plot, = ax.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.0,
            alpha=0.35,
            label="Ref raw",
        )
        ref_integrated_line_plot, = ax.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.6,
            linestyle="--",
            label=f"Ref integrated ({integration_line_count} lines)",
        )
        diff_raw_line_plot, = ax.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.0,
            alpha=0.40,
            color="tab:gray",
            label="Main-Ref raw",
        )
        diff_integrated_line_plot, = ax.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.9,
            color="black",
            label=f"Main-Ref integrated ({integration_line_count} lines)",
        )
    else:
        ref_raw_line_plot = None
        ref_integrated_line_plot = None
        diff_raw_line_plot = None
        diff_integrated_line_plot = None

    if not plot_raw_line:
        raw_line_plot.set_visible(False)
        if ref_raw_line_plot is not None:
            ref_raw_line_plot.set_visible(False)
        if diff_raw_line_plot is not None:
            diff_raw_line_plot.set_visible(False)

    ax.set_title("CMOS Video (Simple Mode)")
    ax.set_xlabel("Sample Index")
    ax.set_ylabel("Voltage (V)")
    ax.set_xlim(0, max(1, x.size - 1))
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper right")
    timing_text = ax.text(
        0.015,
        0.98,
        pda.format_timing_diagnostics(trigger_frequency_hz=expected_trigger_hz),
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=8,
        family="monospace",
        bbox={"facecolor": "white", "alpha": 0.70, "edgecolor": "none"},
    )
    mode_text = ax.text(
        0.985,
        0.98,
        "",
        transform=ax.transAxes,
        va="top",
        ha="right",
        fontsize=9,
        family="monospace",
        bbox={"facecolor": "#d9f2d9", "alpha": 0.85, "edgecolor": "none"},
    )
    if ax_trig is not None:
        trigger_plot_history = max(32, int(trigger_plot_history))
        trig_rate_plot, = ax_trig.plot(
            np.arange(trigger_plot_history),
            np.zeros(trigger_plot_history, dtype=float),
            linewidth=1.2,
            label="PFI9 Edge Rate (Hz)",
        )
        ax_trig.axhline(
            expected_trigger_hz,
            color="tab:red",
            linestyle="--",
            linewidth=1.0,
        )
        ax_trig.set_xlim(0, max(1, trigger_plot_history - 1))
        ax_trig.set_ylabel("PFI9 Hz")
        ax_trig.set_xlabel("Recent Updates")
        ax_trig.grid(True, alpha=0.3)
        ax_trig.legend(loc="upper right")
    else:
        trig_rate_plot = None
    fig.tight_layout()
    plt.show(block=False)

    use_fast_session = bool(use_persistent_session and pda.use_external_trigger)
    line_mode = (
        "persistent retriggered session"
        if use_fast_session
        else "per-line task build/start"
    )
    mode_badge_label = "FAST"
    mode_badge_color = "#d9f2d9"
    if not use_fast_session:
        mode_badge_label = "SAFE"
        mode_badge_color = "#f6f2d9"
    mode_text.set_text(mode_badge_label)
    mode_text.set_bbox(
        {"facecolor": mode_badge_color, "alpha": 0.85, "edgecolor": "none"}
    )

    print("Starting continuous acquisition. Press Ctrl+C to stop.")
    print(f"External trigger on {pda.trig_in}: {pda.use_external_trigger}")
    print(f"Live video mode: {mode_key}")
    if pda.use_external_trigger:
        print(
            "Trigger filter: "
            f"enabled={pda.trigger_filter_enable}, "
            f"min_pulse={pda.trigger_filter_min_pulse_width_s * 1e6:.2f} us"
        )
    print(f"Acquisition mode: {line_mode}")
    print(
        f"Timing: ST high={pda.st_high_time * 1e6:.1f} us, "
        f"ST low={pda.st_low_time * 1e6:.1f} us, "
        f"ST delay={pda.st_initial_delay * 1e6:.1f} us, "
        f"CLK high={pda.clk_high_time * 1e9:.1f} ns, "
        f"CLK low={pda.clk_low_time * 1e9:.1f} ns, "
        f"CLK delay={pda.clk_initial_delay * 1e6:.1f} us"
    )
    print(
        f"Samples/line read={pda.ai_samples_per_line}, "
        f"plotted={pda.output_samples_per_line}"
    )
    print("Timing diagnostics:")
    print(pda.format_timing_diagnostics(trigger_frequency_hz=expected_trigger_hz))

    line_buffer = deque(maxlen=max(1, int(integration_line_count)))
    integration_sum = np.zeros_like(x, dtype=float)
    ref_line_buffer = deque(maxlen=max(1, int(integration_line_count)))
    ref_integration_sum = np.zeros_like(x, dtype=float)
    line_rate_hz_buffer = deque(maxlen=32)
    pfi9_rate_hz_buffer = deque(maxlen=max(32, int(trigger_plot_history)))
    pending_lines_buffer = deque(maxlen=128)
    line_counter = 0
    plot_update_counter = 0
    live_line_rate_hz = 0.0
    trigger_eff = 1.0
    max_pending_lines = 0.0
    plot_update_every_n_lines = max(1, int(plot_update_every_n_lines))
    timing_text_update_every_n_lines = max(1, int(timing_text_update_every_n_lines))
    autoscale_every_n_plot_updates = max(1, int(autoscale_every_n_plot_updates))
    queue_probe_warned = False
    fallback_close_warned = False

    original_video_main = pda.video_main
    if ref_only:
        # Repoint single-channel reads to the configured reference input.
        pda.video_main = pda.video_ref

    try:
        session_context = (
            _RetriggerLineSession(
                pda,
                ai_buffer_lines=2048,
                read_reference=read_reference,
            )
            if use_fast_session
            else nullcontext(None)
        )
        monitor_context = (
            _PFI9EdgeMonitorSession(pda, counter=pfi9_monitor_counter)
            if monitor_pfi9
            else nullcontext(None)
        )
        with session_context as session, monitor_context as pfi9_monitor:
            if pfi9_monitor is not None:
                if getattr(pfi9_monitor, "available", False):
                    print(
                        f"PFI9 monitor enabled on {pfi9_monitor.counter} "
                        f"(source: {pda.trig_in})."
                    )
                elif getattr(pfi9_monitor, "error_text", ""):
                    print(
                        "PFI9 monitor unavailable; continuing without it: "
                        f"{pfi9_monitor.error_text}"
                    )
            while True:
                pending_lines = float("nan")
                if session is not None:
                    try:
                        avail_samples = float(session.ai_task.in_stream.avail_samp_per_chan)
                        pending_lines = avail_samples / float(max(1, pda.ai_samples_per_line))
                        pending_lines_buffer.append(pending_lines)
                        max_pending_lines = max(max_pending_lines, pending_lines)
                    except Exception as exc:
                        if not queue_probe_warned:
                            print(
                                "Warning: could not read queued-line diagnostic "
                                f"from AI stream. Detail: {exc}"
                            )
                            queue_probe_warned = True

                acq_start = time.perf_counter()
                try:
                    if session is None:
                        data = pda.acquire_line(
                            timeout=acquisition_timeout_s,
                            read_reference=read_reference,
                        )
                    else:
                        data = session.read_line(timeout=acquisition_timeout_s)
                except Exception as exc:
                    if session is not None and _is_buffer_overwrite_error(exc):
                        median_pending = (
                            float(np.median(pending_lines_buffer))
                            if pending_lines_buffer
                            else float("nan")
                        )
                        expected = float(expected_trigger_hz) if expected_trigger_hz > 0 else float("nan")
                        print(
                            "\nFast-mode diagnostics before fallback: "
                            f"read_rate~{live_line_rate_hz:.1f} Hz, "
                            f"expected_trigger~{expected:.1f} Hz, "
                            f"median_queue~{median_pending:.1f} lines, "
                            f"max_queue~{max_pending_lines:.1f} lines, "
                            f"buffer~{session.ai_buffer_lines} lines."
                        )
                        print(
                            "\nWarning: retriggered session hit buffer overwrite "
                            "(-200222). Falling back to safe per-line mode."
                        )
                        try:
                            session.close()
                        except Exception as exc:
                            if not fallback_close_warned:
                                print(
                                    "Warning: fast-session close during fallback "
                                    f"reported an error. Detail: {exc}"
                                )
                                fallback_close_warned = True
                        session = None
                        line_mode = "per-line task build/start (fallback)"
                        print(f"Acquisition mode: {line_mode}")
                        mode_badge_label = "SAFE-FALLBACK"
                        mode_badge_color = "#f7d9d9"
                        mode_text.set_text(mode_badge_label)
                        mode_text.set_bbox(
                            {"facecolor": mode_badge_color, "alpha": 0.85, "edgecolor": "none"}
                        )
                        continue
                    raise
                if read_reference:
                    if not (isinstance(data, dict) and "main" in data and "reference" in data):
                        raise RuntimeError(
                            "Expected {'main','reference'} data when live_video_mode='both'."
                        )
                    line = np.asarray(data["main"], dtype=float)
                    ref_line = np.asarray(data["reference"], dtype=float)
                else:
                    line = np.asarray(data, dtype=float)
                    ref_line = None
                acq_elapsed_s = time.perf_counter() - acq_start
                if acq_elapsed_s > 0:
                    line_rate_hz_buffer.append(1.0 / acq_elapsed_s)
                live_line_rate_hz = (
                    float(np.median(line_rate_hz_buffer)) if line_rate_hz_buffer else 0.0
                )
                if pda.use_external_trigger and expected_trigger_hz > 0:
                    trigger_eff = min(1.0, live_line_rate_hz / expected_trigger_hz)
                else:
                    trigger_eff = 1.0
                pfi9_rate_hz = float("nan")
                if pfi9_monitor is not None and getattr(pfi9_monitor, "available", False):
                    pfi9_rate_hz, _ = pfi9_monitor.read_rate()
                    if np.isfinite(pfi9_rate_hz):
                        pfi9_rate_hz_buffer.append(pfi9_rate_hz)

                line_counter += 1
                size_changed = False
                if line.size != x.size:
                    x = np.arange(line.size)
                    raw_line_plot.set_xdata(x)
                    integrated_line_plot.set_xdata(x)
                    if ref_raw_line_plot is not None:
                        ref_raw_line_plot.set_xdata(x)
                    if ref_integrated_line_plot is not None:
                        ref_integrated_line_plot.set_xdata(x)
                    if diff_raw_line_plot is not None:
                        diff_raw_line_plot.set_xdata(x)
                    if diff_integrated_line_plot is not None:
                        diff_integrated_line_plot.set_xdata(x)
                    ax.set_xlim(0, max(1, line.size - 1))
                    line_buffer.clear()
                    integration_sum = np.zeros_like(x, dtype=float)
                    ref_line_buffer.clear()
                    ref_integration_sum = np.zeros_like(x, dtype=float)
                    size_changed = True

                if len(line_buffer) == line_buffer.maxlen:
                    integration_sum -= line_buffer.popleft()
                line_buffer.append(line.copy())
                integration_sum += line
                integrated_line = integration_sum / float(len(line_buffer))
                if ref_line is not None:
                    if len(ref_line_buffer) == ref_line_buffer.maxlen:
                        ref_integration_sum -= ref_line_buffer.popleft()
                    ref_line_buffer.append(ref_line.copy())
                    ref_integration_sum += ref_line
                    ref_integrated_line = (
                        ref_integration_sum / float(len(ref_line_buffer))
                    )
                else:
                    ref_integrated_line = None

                if ref_line is not None:
                    diff_line = line - ref_line
                    diff_integrated_line = integrated_line - ref_integrated_line
                else:
                    diff_line = None
                    diff_integrated_line = None

                should_update_plot = (
                    size_changed or (line_counter % plot_update_every_n_lines == 0)
                )
                if not should_update_plot:
                    continue

                plot_update_counter += 1
                raw_line_plot.set_ydata(line)
                integrated_line_plot.set_ydata(integrated_line)
                if ref_raw_line_plot is not None and ref_line is not None:
                    ref_raw_line_plot.set_ydata(ref_line)
                if (
                    ref_integrated_line_plot is not None
                    and ref_integrated_line is not None
                ):
                    ref_integrated_line_plot.set_ydata(ref_integrated_line)
                if diff_raw_line_plot is not None and diff_line is not None:
                    diff_raw_line_plot.set_ydata(diff_line)
                if (
                    diff_integrated_line_plot is not None
                    and diff_integrated_line is not None
                ):
                    diff_integrated_line_plot.set_ydata(diff_integrated_line)

                ax.set_title(
                    "CMOS Video (Simple Mode) "
                    f"| mode={mode_key} "
                    f"| st_delay={pda.st_initial_delay * 1e6:.1f} us "
                    f"| clk_delay={pda.clk_initial_delay * 1e6:.1f} us "
                    f"| integrated N={len(line_buffer)} "
                    f"| rate={live_line_rate_hz:.1f} Hz "
                    f"| eff={100.0 * trigger_eff:.1f}%"
                    + (
                        f" | q~{pending_lines:.1f} lines"
                        if session is not None and np.isfinite(pending_lines)
                        else ""
                    )
                    + (
                        f" | pfi9~{pfi9_rate_hz:.1f} Hz"
                        if np.isfinite(pfi9_rate_hz)
                        else ""
                    )
                )
                if line_counter == 1 or (
                    line_counter % timing_text_update_every_n_lines == 0
                ):
                    timing_summary = pda.format_timing_diagnostics(
                        trigger_frequency_hz=expected_trigger_hz
                    )
                    timing_summary += f"\nLive line rate: {live_line_rate_hz:.1f} Hz"
                    if pda.use_external_trigger and expected_trigger_hz > 0:
                        timing_summary += (
                            f"\nTrigger efficiency: {100.0 * trigger_eff:.1f}% "
                            f"({live_line_rate_hz:.1f}/{expected_trigger_hz:.1f} Hz)"
                        )
                    else:
                        timing_summary += (
                            "\nTrigger efficiency: n/a (external trigger disabled)"
                        )
                    if session is not None and pending_lines_buffer:
                        timing_summary += (
                            f"\nQueue (median/max): "
                            f"{np.median(pending_lines_buffer):.1f}/"
                            f"{max_pending_lines:.1f} lines"
                        )
                    if pfi9_rate_hz_buffer:
                        timing_summary += (
                            f"\nPFI9 rate (median): {np.median(pfi9_rate_hz_buffer):.1f} Hz"
                        )
                    timing_text.set_text(timing_summary)

                if ax_trig is not None and trig_rate_plot is not None and pfi9_rate_hz_buffer:
                    trig_arr = np.asarray(pfi9_rate_hz_buffer, dtype=float)
                    trig_rate_plot.set_xdata(np.arange(trig_arr.size))
                    trig_rate_plot.set_ydata(trig_arr)
                    ax_trig.set_xlim(0, max(1, trig_arr.size - 1))

                if plot_update_counter % autoscale_every_n_plot_updates == 0:
                    ax.relim(visible_only=True)
                    ax.autoscale_view(scalex=False, scaley=True)
                    if ax_trig is not None and trig_rate_plot is not None and pfi9_rate_hz_buffer:
                        ax_trig.relim(visible_only=True)
                        ax_trig.autoscale_view(scalex=False, scaley=True)
                fig.canvas.draw_idle()
                plt.pause(0.001)

    except KeyboardInterrupt:
        print("\nStopped continuous acquisition.")
    finally:
        pda.video_main = original_video_main
        plt.ioff()
        plt.show()


if __name__ == "__main__":
    # -----------------------------
    # Simple runtime config
    # -----------------------------
    pda = PDAControllerDAQSimple(device="Dev1", num_pixels=1024)
    timing_profile_name = "improved_guess_2channel"
    pda.apply_timing_profile(timing_profile_name)

    # Toggle this to compare free-run vs external trigger gated operation.
    pda.enable_external_trigger(True)
    # External trigger digital filtering (helps reject PFI9 glitches/ringing).
    # Start disabled. If needed, enable with a small width such as 0.1-0.5 us.
    trigger_filter_enable = False
    trigger_filter_min_pulse_width_s = 0.2e-6
    pda.set_trigger_filter(
        enable=trigger_filter_enable,
        min_pulse_width_s=trigger_filter_min_pulse_width_s,
    )

    # Capture window control:
    # total_samples=1050 reproduces legacy-like 16 ignored + 10 trailing.
    # Set total_samples=1024 for no extra capture margin.
    capture_window_total_samples = 1024 #1024
    capture_window_ignored_samples = 0
    pda.set_capture_window_samples(
        total_samples=capture_window_total_samples,
        ignored_samples=capture_window_ignored_samples,
    )
    pda.set_output_cropping(False)

    # Live plot settings
    integration_line_count = 128
    plot_raw_line = True
    # "main", "ref", or "both"
    live_video_mode = "both"
    expected_trigger_hz = 1000.0
    acquisition_timeout_s = 50.0
    # Biggest efficiency gain when external trigger is enabled:
    # arm tasks once and read one line per retrigger.
    # Keep False by default for robustness; set True to test fast mode.
    use_persistent_session = False
    # Plot throttling to reduce GUI overhead.
    plot_update_every_n_lines = 1
    timing_text_update_every_n_lines = 12
    autoscale_every_n_plot_updates = 3
    monitor_pfi9 = True
    pfi9_monitor_counter = "ctr2"
    trigger_plot_history = 240

    run_live_plot(
        pda=pda,
        integration_line_count=integration_line_count,
        plot_raw_line=plot_raw_line,
        live_video_mode=live_video_mode,
        expected_trigger_hz=expected_trigger_hz,
        acquisition_timeout_s=acquisition_timeout_s,
        use_persistent_session=use_persistent_session,
        plot_update_every_n_lines=plot_update_every_n_lines,
        timing_text_update_every_n_lines=timing_text_update_every_n_lines,
        autoscale_every_n_plot_updates=autoscale_every_n_plot_updates,
        monitor_pfi9=monitor_pfi9,
        pfi9_monitor_counter=pfi9_monitor_counter,
        trigger_plot_history=trigger_plot_history,
    )
