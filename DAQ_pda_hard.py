"""
DAQ_pda_hard.py
================

Hardware-timed NI-DAQmx runner for Hamamatsu line-scan CMOS readout.

This script supports two operational modes:
1) Live acquisition/plotting (`--operation live`)
2) Persistent retrigger timing sweep (`--operation sweep`)

Key architecture:
- ST and CLK are generated from NI counters.
- AI sampling is clocked by the CLK counter internal output (hardware timing).
- External trigger (PFI9) can retrigger the ST/CLK/AI chain.

Common usage:
- Live default run:
  `python DAQ_pda_hard.py`
- Live with explicit plot cadence:
  `python DAQ_pda_hard.py --plot-fps 25 --plot-every-lines 2 --timing-text-every-lines 40 --autoscale-every-updates 8`
- Timing sweep:
  `python DAQ_pda_hard.py --operation sweep`
"""

import numpy as np
import nidaqmx
import matplotlib.pyplot as plt
import time
import argparse
import threading
import csv
import json
from pathlib import Path
from contextlib import nullcontext
from collections import deque
from nidaqmx.constants import (
    AcquisitionType,
    Edge,
    ExportAction,
    LineGrouping,
    Level,
    TerminalConfiguration,
    LoggingMode,
    LoggingOperation,
    OverwriteMode,
    Polarity,
    ReadRelativeTo,
)

RUNTIME_PROFILE_PERSISTENT_LATEST = "persistent_latest_test"
RUNTIME_PROFILE_PERSISTENT_ROBUST = "persistent_robust_test"
RUNTIME_PROFILE_SAFE = "safe"
RUNTIME_PROFILE_CHOICES = (
    RUNTIME_PROFILE_PERSISTENT_LATEST,
    RUNTIME_PROFILE_PERSISTENT_ROBUST,
    RUNTIME_PROFILE_SAFE,
)
RUNTIME_PROFILE_DEFAULT = RUNTIME_PROFILE_PERSISTENT_ROBUST

LIVE_PRESET_DEFAULT = "default"
LIVE_PRESET_SIGNAL_ONLY = "signal_only"
LIVE_PRESET_TRIGGER_DEBUG = "trigger_debug"
LIVE_PRESET_CHOICES = (
    LIVE_PRESET_DEFAULT,
    LIVE_PRESET_SIGNAL_ONLY,
    LIVE_PRESET_TRIGGER_DEBUG,
)


class PDAControllerDAQSimple:
    """
    Simple NI-6363 PDA/CMOS control path:
    - ctr0 outputs ST on PFI8
    - ctr1 outputs CLK on PFI4
    - AI is sampled either by ctr1 internal output or by a divided ctr2 clock
    - optional chopper sync output is routed on PFI3
    - optional external chopper FOUT TTL input can be observed on PFI14
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

    TAS_GUESS_TIMING = {
        "description": "Built for SPEED.",
        "st_high_time": 4e-6,
        "st_low_time": 1000e-6, #2.372e-3
        "st_initial_delay": 0.0,
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

    MAIN_1KHZ_SAFE_TIMING = {
        "description": (
            "1 kHz-safe main-channel default: 2.0 MHz clock "
            "with positive trigger-period margin."
        ),
        "st_high_time": 4e-6,
        "st_low_time": 950e-6,
        "st_initial_delay": 0.0,
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
    }

    TXT_1KHZ_REBASED_TIMING = {
        "description": (
            "Rebased from 1 kHz legacy text configs (Pulse2-like): "
            "trigger->ST delay ~474 us with NI-6363/2 MHz-compatible line timing."
        ),
        # Derived from 2.5 MHz-tick legacy values:
        #   High_Ticks=125  -> 50 us
        #   Low_Ticks=1185  -> 474 us
        #   Initial_Delay   -> 474 us
        "st_high_time": 50e-6,
        "st_low_time": 474e-6,
        "st_initial_delay": 0e-6,
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
    }


    TOOFAST_GUESS_TIMING = {
        "description": "Possibily detrimental?.",
        "st_high_time": 4e-6, #minimum is 4us
        "st_low_time": 600e-6, #minimum is 7.5us had a working value at 9.98e-4
        "st_initial_delay": 0, #minimum is 0
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
    }

    GUARD_10US_TIMING = {
        "description": (
            "Near-1 kHz guard profile: ST period tuned to ~990 us "
            "(~10 us margin at 1 kHz when trigger->ST phase shift is 0 us)."
        ),
        "st_high_time": 4e-6,
        "st_low_time": 986e-6,
        "st_initial_delay": 0.0,
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
    }

    SHORT_LINE_SINGLE_CHANNEL_TIMING = {
        "description": (
            "Guard-derived short-line single-channel profile: 2 MHz clock, "
            "14 dummy clocks, 505 valid pixel clocks, and ~264 us capture end."
        ),
        "st_high_time": 4e-6,
        "st_low_time": 986e-6,
        "st_initial_delay": 0.0,
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
        "dummy_clocks": 14,
        "pixel_clocks": 505,
        "start_on_st_fall": True,
    }

    DECIMATED_2CHANNEL_TIMING = {
        "description": (
            "Full-span two-channel decimated profile: detector CLK at 2 MHz, "
            "AI sample clock divided by 2 to 1 MHz, 14 dummy clocks, 1024 valid "
            "pixel clocks, and ~523 us capture end."
        ),
        "st_high_time": 4e-6,
        "st_low_time": 986e-6,
        "st_initial_delay": 0.0,
        "clk_high_time": 250e-9,
        "clk_low_time": 250e-9,
        "clk_initial_delay": 0.0,
        "dummy_clocks": 14,
        "pixel_clocks": 1024,
        "start_on_st_fall": True,
        "ai_sample_clock_divisor": 2,
    }

    ALMOST_FULL_2CHANNEL_TIMING = {
        "description": (
            "Near-full two-channel profile: 1 MHz CLK/AI sampling, 14 dummy "
            "clocks, 972 valid pixel clocks, and ~990 us capture end."
        ),
        "st_high_time": 4e-6,
        "st_low_time": 986e-6,
        "st_initial_delay": 0.0,
        "clk_high_time": 500e-9,
        "clk_low_time": 500e-9,
        "clk_initial_delay": 0.0,
        "dummy_clocks": 14,
        "pixel_clocks": 972,
        "start_on_st_fall": True,
        "ai_sample_clock_divisor": 1,
    }

    ALMOST_FULL_SINGLE_CHANNEL_TIMING = {
        "description": (
            "Single-channel diagnostic equivalent of almost_full_2channel: "
            "1 MHz CLK/AI sampling, 14 dummy clocks, 972 valid pixel clocks, "
            "and ~990 us capture end."
        ),
        "st_high_time": 4e-6,
        "st_low_time": 986e-6,
        "st_initial_delay": 0.0,
        "clk_high_time": 500e-9,
        "clk_low_time": 500e-9,
        "clk_initial_delay": 0.0,
        "dummy_clocks": 14,
        "pixel_clocks": 972,
        "start_on_st_fall": True,
        "ai_sample_clock_divisor": 1,
    }

    # Named profiles make it explicit which timing set is active.
    TIMING_PROFILES = {
        "main_1khz_safe": MAIN_1KHZ_SAFE_TIMING,
        "txt_1khz_rebased": TXT_1KHZ_REBASED_TIMING,
        "initial_guess": INITIAL_GUESS_TIMING,
        "improved_guess": IMPROVED_GUESS_TIMING,
        "improved_guess_2channel": IMPROVED_GUESS_TIMING_2CHANNEL,
        "toofast_guess": TOOFAST_GUESS_TIMING,
        "guard_10us": GUARD_10US_TIMING,
        "short_line_single_channel": SHORT_LINE_SINGLE_CHANNEL_TIMING,
        "decimated_2channel": DECIMATED_2CHANNEL_TIMING,
        "almost_full_2channel": ALMOST_FULL_2CHANNEL_TIMING,
        "almost_full_single_channel": ALMOST_FULL_SINGLE_CHANNEL_TIMING,
    }
    DEFAULT_TIMING_PROFILE = "guard_10us"

    def __init__(
        self,
        device="Dev1",
        num_pixels=1024,
        ai_main="ai2",
        ai_ref="ai0",
        trig_pfi="PFI9",
        st_pfi="PFI8",
        clk_pfi="PFI4",
        chopper_sync_pfi="PFI3",
        chopper_input_pfi="PFI14",
    ):
        self.device = str(device)
        self.num_pixels = int(num_pixels)

        self.video_main = f"{self.device}/{ai_main}"
        self.video_ref = f"{self.device}/{ai_ref}"
        self.trig_in = f"/{self.device}/{trig_pfi}"
        self.st_out_term = f"/{self.device}/{st_pfi}"
        self.clk_out_term = f"/{self.device}/{clk_pfi}"
        self.chopper_sync_out_term = f"/{self.device}/{chopper_sync_pfi}"
        self.chopper_input_term = f"/{self.device}/{chopper_input_pfi}"

        self.st_counter = f"{self.device}/ctr0"
        self.clk_counter = f"{self.device}/ctr1"
        self.ai_sample_clock_counter = f"{self.device}/ctr2"
        self.chopper_sync_counter = f"{self.device}/ctr3"
        self.st_internal_output = f"/{self.device}/Ctr0InternalOutput"
        self.detector_clk_internal_output = f"/{self.device}/Ctr1InternalOutput"
        self.ai_sample_clock_internal_output = f"/{self.device}/Ctr2InternalOutput"
        self.ai_sample_clk_src = self.detector_clk_internal_output

        self.st_high_time = 0.0
        self.st_low_time = 0.0
        self.st_initial_delay = 0.0
        # Keep profile/explicit ST delay and trigger phase shift separate.
        # Effective ST initial delay used by hardware is:
        #   base_st_initial_delay + trigger_phase_shift_s
        self.base_st_initial_delay = 0.0
        self.trigger_phase_shift_s = 0.0
        self.clk_high_time = 0.0
        self.clk_low_time = 0.0
        self.clk_initial_delay = 0.0
        self.ai_sample_clock_divisor = 1
        self.ai_sample_clock_initial_delay = 0.0

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
        self.trigger_sync_enable = True
        self._trigger_sync_warned = False
        self.retrigger_enable_initial_delay = True
        self._retrigger_delay_warned = False
        self.chopper_sync_enable = False
        self.chopper_sync_source_terminal = self.trig_in
        self.chopper_sync_initial_delay_ticks = 0
        self.chopper_sync_high_ticks = 2
        self.chopper_sync_low_ticks = 2
        self._chopper_sync_warned = False

        # Video timing model:
        # - AI starts on ST falling edge by default.
        # - Sampling begins on first CLK rising edge after that edge.
        # - First `video_dummy_clocks` samples are pre-video clocks, then pixels.
        self.ai_start_trigger_edge = Edge.FALLING
        self.video_dummy_clocks = 14
        self.video_pixel_clocks = self.num_pixels
        self.video_output_samples = self.num_pixels

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
        st_high = cfg.get("st_high_period", cfg["st_high_time"])
        st_low = cfg.get("st_low_period", cfg["st_low_time"])
        clk_high = cfg.get("clk_high_period", cfg["clk_high_time"])
        clk_low = cfg.get("clk_low_period", cfg["clk_low_time"])
        self.set_st_timing(
            high_time_s=st_high,
            low_time_s=st_low,
            initial_delay_s=cfg["st_initial_delay"],
        )
        self.set_clk_timing(
            high_time_s=clk_high,
            low_time_s=clk_low,
            initial_delay_s=cfg["clk_initial_delay"],
        )
        self.set_ai_sample_clock_divisor(
            cfg.get("ai_sample_clock_divisor", 1),
            initial_delay_s=cfg.get(
                "ai_sample_clock_initial_delay",
                cfg["clk_initial_delay"],
            ),
        )
        self.set_video_timing(
            dummy_clocks=cfg.get("dummy_clocks", 14),
            pixel_clocks=cfg.get("pixel_clocks", self.num_pixels),
            start_on_st_fall=cfg.get("start_on_st_fall", True),
        )

    def set_st_timing(self, high_time_s, low_time_s, initial_delay_s=0.0):
        self.st_high_time = float(high_time_s)
        self.st_low_time = float(low_time_s)
        self.base_st_initial_delay = float(initial_delay_s)
        self.st_initial_delay = (
            self.base_st_initial_delay + self.trigger_phase_shift_s
        )

    def set_clk_timing(self, high_time_s, low_time_s, initial_delay_s=0.0):
        self.clk_high_time = float(high_time_s)
        self.clk_low_time = float(low_time_s)
        self.clk_initial_delay = float(initial_delay_s)

    def set_ai_sample_clock_divisor(self, divisor=1, initial_delay_s=None):
        """
        Set the AI sampling clock relative to the detector CLK.

        divisor=1 keeps legacy behavior: AI samples directly from the detector
        CLK counter internal output. divisor>1 uses ctr2 as a separate AI
        sample clock, allowing the detector to continue clocking pixels faster
        than the analog input scan rate.
        """
        div = int(divisor)
        if div < 1:
            raise ValueError("ai_sample_clock_divisor must be >= 1.")
        self.ai_sample_clock_divisor = div
        self.ai_sample_clock_initial_delay = (
            self.clk_initial_delay
            if initial_delay_s is None
            else float(initial_delay_s)
        )
        self.ai_sample_clk_src = (
            self.detector_clk_internal_output
            if div == 1
            else self.ai_sample_clock_internal_output
        )

    @property
    def st_high_period_s(self):
        return float(self.st_high_time)

    @property
    def st_low_period_s(self):
        return float(self.st_low_time)

    @property
    def clk_high_period_s(self):
        return float(self.clk_high_time)

    @property
    def clk_low_period_s(self):
        return float(self.clk_low_time)

    def set_trigger_phase_shift(self, trigger_to_st_delay_s=0.0):
        """
        Deterministic phase shift from external trigger edge to ST rising edge.

        This is additive with the base ST initial delay from the active timing
        profile (or last set_st_timing call):

            effective_st_initial_delay
              = base_st_initial_delay + trigger_phase_shift_s

        Since CLK is started from ST internal output, shifting ST shifts the
        whole ST/CLK/AI chain together in absolute time while preserving
        ST->CLK relative delay.
        """
        self.trigger_phase_shift_s = float(trigger_to_st_delay_s)
        self.st_initial_delay = (
            self.base_st_initial_delay + self.trigger_phase_shift_s
        )

    def enable_external_trigger(self, enable=True, edge=Edge.RISING):
        self.use_external_trigger = bool(enable)
        self.trigger_edge = edge

    def set_trigger_filter(self, enable=True, min_pulse_width_s=2e-6):
        self.trigger_filter_enable = bool(enable)
        self.trigger_filter_min_pulse_width_s = float(min_pulse_width_s)
        if self.trigger_filter_min_pulse_width_s < 0:
            raise ValueError("min_pulse_width_s must be >= 0.")

    def set_trigger_sync(self, enable=True):
        self.trigger_sync_enable = bool(enable)

    def set_chopper_sync_output(
        self,
        enable=True,
        out_pfi="PFI3",
        source_terminal=None,
        initial_delay_ticks=0,
        high_ticks=2,
        low_ticks=2,
    ):
        """
        Configure a hardware-timed chopper sync output.

        By default this exports a terminal-count pulse every two edges of the
        external trigger stream on ``PFI9`` using a spare counter and routes
        the resulting 500 Hz TTL sync pulse train to ``PFI3``. This keeps the
        chopper reference derived from the same source as the line-acquisition
        trigger without adding per-line software load.
        """
        self.chopper_sync_enable = bool(enable)
        self.chopper_sync_out_term = f"/{self.device}/{str(out_pfi).lstrip('/')}"
        self.chopper_sync_source_terminal = (
            self.trig_in if source_terminal in (None, "") else str(source_terminal)
        )
        self.chopper_sync_initial_delay_ticks = max(0, int(initial_delay_ticks))
        self.chopper_sync_high_ticks = max(1, int(high_ticks))
        self.chopper_sync_low_ticks = max(1, int(low_ticks))

    def set_chopper_input_terminal(self, in_pfi="PFI14"):
        """
        Configure the PFI terminal used for incoming chopper FOUT TTL.
        """
        self.chopper_input_term = f"/{self.device}/{str(in_pfi).lstrip('/')}"

    def set_retrigger_initial_delay(self, enable=True):
        self.retrigger_enable_initial_delay = bool(enable)

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
        try:
            start_trigger.dig_edge_dig_sync_enable = bool(self.trigger_sync_enable)
        except Exception as exc:
            if not self._trigger_sync_warned:
                print(
                    "Warning: could not apply trigger digital synchronization on "
                    f"{self.trig_in}: {exc}"
                )
                self._trigger_sync_warned = True

    def set_line_windowing(self, enable=True, ignored_samples=16, trailing_samples=10):
        if not enable:
            self.ai_ignored_samples = 0
            self.ai_trailing_samples = 0
            return
        self.ai_ignored_samples = max(0, int(ignored_samples))
        self.ai_trailing_samples = max(0, int(trailing_samples))

    def set_capture_window_samples(self, total_samples, ignored_samples=0, valid_samples=None):
        """
        Set total acquired samples per line directly.

        total_samples = valid_samples + ignored_samples + trailing_samples
        """
        total = int(total_samples)
        ignored = int(ignored_samples)
        valid = int(self.video_pixel_clocks if valid_samples is None else valid_samples)
        if valid <= 0:
            raise ValueError("valid_samples must be > 0.")
        if total < valid:
            raise ValueError(
                f"total_samples ({total}) must be >= valid_samples ({valid})."
            )
        if ignored < 0:
            raise ValueError("ignored_samples must be >= 0.")

        trailing = total - valid - ignored
        if trailing < 0:
            raise ValueError(
                "ignored_samples is too large for requested total_samples and valid_samples."
            )
        self.set_line_windowing(
            enable=True,
            ignored_samples=ignored,
            trailing_samples=trailing,
        )
        self.video_output_samples = valid

    def set_output_cropping(self, enable=True):
        self.crop_output_to_valid_pixels = bool(enable)

    def set_video_timing(
        self,
        dummy_clocks=14,
        pixel_clocks=None,
        start_on_st_fall=True,
    ):
        """
        Configure video acquisition relative to ST/CLK timing.

        start_on_st_fall=True:
          AI start trigger uses ST falling edge, so the first acquired sample is
          the first CLK rising edge after ST goes low.

        dummy_clocks:
          Number of leading clocks to keep before valid pixel clocks.

        pixel_clocks:
          Number of valid pixel clocks to acquire after dummy clocks. This may
          be less than num_pixels for short-line/ROI timing tests.
        """
        dummy = int(dummy_clocks)
        pixels = int(self.num_pixels if pixel_clocks is None else pixel_clocks)
        if dummy < 0:
            raise ValueError("dummy_clocks must be >= 0.")
        if pixels <= 0:
            raise ValueError("pixel_clocks must be > 0.")
        if pixels > int(self.num_pixels):
            raise ValueError(
                f"pixel_clocks ({pixels}) must be <= num_pixels ({self.num_pixels})."
            )

        self.video_dummy_clocks = dummy
        self.video_pixel_clocks = pixels
        self.ai_start_trigger_edge = (
            Edge.FALLING if bool(start_on_st_fall) else Edge.RISING
        )
        sample_divisor = max(1, int(self.ai_sample_clock_divisor))
        ignored_samples = int(np.ceil(dummy / float(sample_divisor)))
        valid_samples = int(np.ceil(pixels / float(sample_divisor)))
        total_samples = int(np.ceil((dummy + pixels) / float(sample_divisor)))
        # Keep dummy clocks in front of the valid-pixel region so optional
        # cropping can return exactly the requested pixel-clock count.
        self.set_capture_window_samples(
            total_samples=total_samples,
            ignored_samples=ignored_samples,
            valid_samples=valid_samples,
        )

    @property
    def clk_rate(self):
        return 1.0 / (self.clk_high_time + self.clk_low_time)

    @property
    def ai_sample_rate(self):
        return self.clk_rate / float(max(1, int(self.ai_sample_clock_divisor)))

    @property
    def uses_separate_ai_sample_clock(self):
        return int(self.ai_sample_clock_divisor) != 1

    @property
    def st_period_s(self):
        return float(self.st_high_time + self.st_low_time)

    @property
    def ai_samples_per_line(self):
        return int(self.video_output_samples + self.ai_ignored_samples + self.ai_trailing_samples)

    @property
    def output_samples_per_line(self):
        if self.crop_output_to_valid_pixels:
            return int(self.video_output_samples)
        return int(self.ai_samples_per_line)

    def output_sample_axis(self, sample_count=None):
        count = self.output_samples_per_line if sample_count is None else int(sample_count)
        divisor = max(1, int(self.ai_sample_clock_divisor))
        return np.arange(count, dtype=float) * float(divisor)

    @property
    def sample_window_s(self):
        # Window duration between first and last sample edges.
        return max(0, self.ai_samples_per_line - 1) / self.ai_sample_rate

    @property
    def pre_ai_clock_count(self):
        capture = self._compute_capture_timing()
        pre = (
            (capture["first_sample_s"] - capture["clk_start_s"])
            / capture["clk_period_s"]
        )
        return max(0, int(round(pre)))

    @property
    def pre_ai_sample_clock_count(self):
        capture = self._compute_capture_timing()
        pre = (
            (capture["first_sample_s"] - capture["ai_sample_clk_start_s"])
            / capture["ai_sample_period_s"]
        )
        return max(0, int(round(pre)))

    @property
    def clk_pulses_per_line(self):
        detector_clocks_per_line = int(self.video_dummy_clocks + self.video_pixel_clocks)
        return int(detector_clocks_per_line + self.pre_ai_clock_count)

    @property
    def ai_sample_clock_pulses_per_line(self):
        return int(self.ai_samples_per_line + self.pre_ai_sample_clock_count)

    def _compute_capture_timing(self):
        clk_period_s = 1.0 / self.clk_rate
        ai_sample_period_s = 1.0 / self.ai_sample_rate
        st_rise_s = self.st_initial_delay
        st_fall_s = self.st_initial_delay + self.st_high_time
        clk_start_s = self.st_initial_delay + self.clk_initial_delay
        ai_sample_clk_start_s = (
            self.st_initial_delay + self.ai_sample_clock_initial_delay
        )

        # AI starts from ST internal edge, then samples on the first AI-sample
        # clock edge that occurs after that AI trigger edge.
        ai_start_event_s = (
            st_fall_s
            if self.ai_start_trigger_edge == Edge.FALLING
            else st_rise_s
        )
        strict_after = self.ai_start_trigger_edge == Edge.FALLING
        if ai_start_event_s < ai_sample_clk_start_s:
            first_sample_s = ai_sample_clk_start_s
        else:
            rel = (ai_start_event_s - ai_sample_clk_start_s) / ai_sample_period_s
            if strict_after:
                n_edges = int(np.floor(rel + 1e-12)) + 1
            else:
                n_edges = int(np.ceil(rel - 1e-12))
            first_sample_s = (
                ai_sample_clk_start_s + max(0, n_edges) * ai_sample_period_s
            )

        capture_window_s = self.sample_window_s
        capture_end_s = first_sample_s + capture_window_s
        return {
            "clk_period_s": clk_period_s,
            "ai_sample_period_s": ai_sample_period_s,
            "st_rise_s": st_rise_s,
            "st_fall_s": st_fall_s,
            "clk_start_s": clk_start_s,
            "ai_sample_clk_start_s": ai_sample_clk_start_s,
            "ai_start_event_s": ai_start_event_s,
            "first_sample_s": first_sample_s,
            "capture_window_s": capture_window_s,
            "capture_end_s": capture_end_s,
        }

    def estimate_line_timing(self, trigger_frequency_hz=None):
        capture = self._compute_capture_timing()
        sample_window_s = capture["capture_window_s"]
        total_trigger_to_end_s = capture["capture_end_s"]
        st_period_s = self.st_period_s
        limiting_cycle_s = max(st_period_s, total_trigger_to_end_s)
        timing = {
            "clk_rate_hz": self.clk_rate,
            "clk_period_s": 1.0 / self.clk_rate,
            "ai_sample_rate_hz": self.ai_sample_rate,
            "ai_sample_period_s": 1.0 / self.ai_sample_rate,
            "ai_sample_clock_divisor": int(self.ai_sample_clock_divisor),
            "ai_sample_clock_source": str(self.ai_sample_clk_src),
            "st_high_s": self.st_high_time,
            "st_low_s": self.st_low_time,
            "st_period_s": st_period_s,
            "st_initial_delay_s": self.st_initial_delay,
            "clk_initial_delay_s": self.clk_initial_delay,
            "ai_sample_clock_initial_delay_s": self.ai_sample_clock_initial_delay,
            "sample_window_s": sample_window_s,
            "total_trigger_to_end_s": total_trigger_to_end_s,
            "limiting_cycle_s": limiting_cycle_s,
            "pre_ai_clock_count": self.pre_ai_clock_count,
            "pre_ai_sample_clock_count": self.pre_ai_sample_clock_count,
            "clk_pulses_per_line": self.clk_pulses_per_line,
            "ai_sample_clock_pulses_per_line": self.ai_sample_clock_pulses_per_line,
            "samples_per_line_read": self.ai_samples_per_line,
            "samples_per_line_output": self.output_samples_per_line,
        }
        if trigger_frequency_hz is not None and trigger_frequency_hz > 0:
            period_s = 1.0 / float(trigger_frequency_hz)
            timing["trigger_frequency_hz"] = float(trigger_frequency_hz)
            timing["trigger_period_s"] = period_s
            timing["timing_margin_s"] = period_s - total_trigger_to_end_s
            timing["timing_margin_limited_s"] = period_s - limiting_cycle_s
        return timing

    def get_timing_diagnostics(self, trigger_frequency_hz=None):
        capture = self._compute_capture_timing()
        clk_period_s = capture["clk_period_s"]
        st_rise_s = capture["st_rise_s"]
        st_fall_s = capture["st_fall_s"]
        clk_start_s = capture["clk_start_s"]
        st_to_clk_s = clk_start_s - st_fall_s
        st_to_clk_clks = st_to_clk_s / clk_period_s
        capture_window_s = capture["capture_window_s"]
        capture_start_s = capture["first_sample_s"]
        capture_end_s = capture["capture_end_s"]
        ai_start_event_s = capture["ai_start_event_s"]
        ai_sample_period_s = capture["ai_sample_period_s"]
        ai_sample_clk_start_s = capture["ai_sample_clk_start_s"]
        dummy_clock_span_s = float(self.video_dummy_clocks) * clk_period_s
        video_valid_start_s = capture_start_s + dummy_clock_span_s
        video_valid_end_s = (
            video_valid_start_s
            + max(0, self.video_output_samples - 1) * ai_sample_period_s
        )

        timing = self.estimate_line_timing(trigger_frequency_hz=trigger_frequency_hz)
        return {
            "reference_event": (
                "PFI9 trigger edge" if self.use_external_trigger else "software start"
            ),
            "trigger_in_pfi9_s": 0.0 if self.use_external_trigger else None,
            "st_rise_s": st_rise_s,
            "st_fall_s": st_fall_s,
            "clk_start_s": clk_start_s,
            "ai_sample_clk_start_s": ai_sample_clk_start_s,
            "ai_start_edge_label": (
                "ST falling" if self.ai_start_trigger_edge == Edge.FALLING else "ST rising"
            ),
            "ai_start_event_s": ai_start_event_s,
            "capture_start_s": capture_start_s,
            "st_end_to_clk_start_s": st_to_clk_s,
            "st_end_to_clk_start_clocks": st_to_clk_clks,
            "capture_window_s": capture_window_s,
            "capture_end_s": capture_end_s,
            "video_dummy_clocks": int(self.video_dummy_clocks),
            "video_pixel_clocks": int(self.video_pixel_clocks),
            "video_output_samples": int(self.video_output_samples),
            "video_valid_start_s": video_valid_start_s,
            "video_valid_end_s": video_valid_end_s,
            "pre_ai_clock_count": int(self.pre_ai_clock_count),
            "pre_ai_sample_clock_count": int(self.pre_ai_sample_clock_count),
            "clk_pulses_per_line": int(self.clk_pulses_per_line),
            "ai_sample_clock_pulses_per_line": int(
                self.ai_sample_clock_pulses_per_line
            ),
            "clk_rate_hz": self.clk_rate,
            "ai_sample_rate_hz": self.ai_sample_rate,
            "ai_sample_clock_divisor": int(self.ai_sample_clock_divisor),
            "ai_sample_clock_source": str(self.ai_sample_clk_src),
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
                "AI sample clock: "
                f"{d['ai_sample_rate_hz'] / 1e6:.3f} MHz "
                f"(divisor={d['ai_sample_clock_divisor']}, "
                f"source={d['ai_sample_clock_source']})"
            ),
            (
                f"AI start edge: {d['ai_start_edge_label']} "
                f"@ {d['ai_start_event_s'] * 1e6:.1f} us"
            ),
            (
                "Capture start (first AI sample clock after AI edge): "
                f"{d['capture_start_s'] * 1e6:.1f} us"
            ),
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
            (
                "CLK pulses per line: "
                f"{d['clk_pulses_per_line']} "
                f"(pre-AI detector clocks: {d['pre_ai_clock_count']})"
            ),
            (
                "AI sample-clock pulses per line: "
                f"{d['ai_sample_clock_pulses_per_line']} "
                f"(pre-AI sample clocks: {d['pre_ai_sample_clock_count']})"
            ),
            (
                "Video dummy clocks before pixels: "
                f"{d['video_dummy_clocks']} "
                f"({(d['video_valid_start_s'] - d['capture_start_s']) * 1e6:.1f} us)"
            ),
            (
                "Video valid window: "
                f"{d['video_valid_start_s'] * 1e6:.1f} -> "
                f"{d['video_valid_end_s'] * 1e6:.1f} us"
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
        f_scan = self.ai_sample_rate
        if self.clk_rate <= 0.0:
            raise ValueError("Detector CLK rate must be > 0.")
        if f_scan <= 0.0:
            raise ValueError("AI sample clock rate must be > 0.")
        channels = max(1, int(num_ai_channels))
        hard_limit_scan_rate = self.ai_max_conversion_rate / channels
        if f_scan > hard_limit_scan_rate:
            raise ValueError(
                f"Requested AI sample clock {f_scan:.3f} Hz exceeds NI-6363 "
                f"limit for {channels} channel(s): {hard_limit_scan_rate:.3f} Hz."
            )
        recommended_max_scan_rate = hard_limit_scan_rate * float(
            self.ai_recommended_utilization
        )
        if f_scan > recommended_max_scan_rate and channels not in self._scan_rate_warned_channels:
            print(
                "Warning: AI sample clock is near NI-6363 conversion ceiling "
                f"for {channels} channel(s): requested={f_scan:.0f} Hz, "
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

        # AI is explicitly clocked from either the detector CLK (legacy) or a
        # divided AI sample clock (decimated modes).
        ai_task.timing.cfg_samp_clk_timing(
            rate=self.ai_sample_rate,
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
            samps_per_chan=self.clk_pulses_per_line,
        )
        return clk_task

    def _build_ai_sample_clk_task(self):
        if not self.uses_separate_ai_sample_clock:
            return None
        period_s = 1.0 / self.ai_sample_rate
        ai_clk_task = nidaqmx.Task("PDA_AI_SAMPLE_CLK")
        ai_clk_task.co_channels.add_co_pulse_chan_time(
            counter=self.ai_sample_clock_counter,
            idle_state=Level.LOW,
            initial_delay=self.ai_sample_clock_initial_delay,
            low_time=0.5 * period_s,
            high_time=0.5 * period_s,
        )
        ai_clk_task.timing.cfg_implicit_timing(
            sample_mode=AcquisitionType.FINITE,
            samps_per_chan=self.ai_sample_clock_pulses_per_line,
        )
        return ai_clk_task

    def _apply_start_trigger_chain(self, ai_task, st_task, clk_task, ai_sample_clk_task=None):
        ai_task.triggers.start_trigger.cfg_dig_edge_start_trig(
            trigger_source=self.st_internal_output,
            trigger_edge=self.ai_start_trigger_edge,
        )
        clk_task.triggers.start_trigger.cfg_dig_edge_start_trig(
            trigger_source=self.st_internal_output,
            trigger_edge=Edge.RISING,
        )
        if ai_sample_clk_task is not None:
            ai_sample_clk_task.triggers.start_trigger.cfg_dig_edge_start_trig(
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
        stop = int(start + self.video_output_samples)
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
             self._build_clk_task() as clk_task, \
             (
                 self._build_ai_sample_clk_task()
                 if self.uses_separate_ai_sample_clock
                 else nullcontext(None)
             ) as ai_sample_clk_task:

            self._apply_start_trigger_chain(
                ai_task,
                st_task,
                clk_task,
                ai_sample_clk_task=ai_sample_clk_task,
            )

            # Arm in downstream-to-upstream order.
            ai_task.start()
            if ai_sample_clk_task is not None:
                ai_sample_clk_task.start()
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

    TODO(2026-04-17): Investigate stale-line behavior in retriggered mode.
    Observed symptom: live view can repeatedly return the same first acquired
    line (blocking the optical source does not change the displayed line),
    while safe per-line mode updates correctly.
    """

    def __init__(
        self,
        pda,
        ai_buffer_lines=1024,
        read_reference=False,
        tdms_log_enable=False,
        tdms_file_path=None,
        tdms_group_name="PDA",
        tdms_logging_mode=LoggingMode.LOG_AND_READ,
        tdms_logging_operation=LoggingOperation.OPEN_OR_CREATE,
        latest_only_read=True,
        overwrite_unread=True,
    ):
        self.pda = pda
        self.ai_buffer_lines = max(64, int(ai_buffer_lines))
        self.read_reference = bool(read_reference)
        self.tdms_log_enable = bool(tdms_log_enable)
        self.tdms_file_path = (
            None if tdms_file_path in (None, "") else str(tdms_file_path)
        )
        self.tdms_group_name = str(tdms_group_name)
        self.tdms_logging_mode = tdms_logging_mode
        self.tdms_logging_operation = tdms_logging_operation
        self.latest_only_read = bool(latest_only_read)
        self.overwrite_unread = bool(overwrite_unread)
        self.ai_task = None
        self.st_task = None
        self.clk_task = None
        self.ai_sample_clk_task = None
        self._buffer_size_warned = False
        self._tdms_warned = False
        self._latest_read_warned = False
        self._latest_cursor_enabled = False
        self.last_lines_consumed = 1

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
        if self.tdms_log_enable and self.tdms_file_path:
            try:
                self.ai_task.in_stream.configure_logging(
                    file_path=self.tdms_file_path,
                    logging_mode=self.tdms_logging_mode,
                    group_name=self.tdms_group_name,
                    operation=self.tdms_logging_operation,
                )
            except Exception as exc:
                if not self._tdms_warned:
                    print(
                        "Warning: failed to enable TDMS logging in retriggered mode. "
                        f"Continuing without TDMS logging. Detail: {exc}"
                    )
                    self._tdms_warned = True
        try:
            self.ai_task.in_stream.overwrite = (
                OverwriteMode.OVERWRITE_UNREAD_SAMPLES
                if self.overwrite_unread
                else OverwriteMode.DO_NOT_OVERWRITE_UNREAD_SAMPLES
            )
        except Exception:
            # Some driver versions expose over_write instead of overwrite.
            try:
                self.ai_task.in_stream.over_write = (
                    OverwriteMode.OVERWRITE_UNREAD_SAMPLES
                    if self.overwrite_unread
                    else OverwriteMode.DO_NOT_OVERWRITE_UNREAD_SAMPLES
                )
            except Exception:
                pass

        if self.latest_only_read:
            line_samples = int(self.pda.ai_samples_per_line)
            try:
                # NI recommendation for overwrite/latest mode:
                # read relative to MOST_RECENT_SAMPLE with a negative offset.
                self.ai_task.in_stream.relative_to = ReadRelativeTo.MOST_RECENT_SAMPLE
                self.ai_task.in_stream.offset = -line_samples
                self._latest_cursor_enabled = True
            except Exception as exc:
                self._latest_cursor_enabled = False
                if not self._latest_read_warned:
                    print(
                        "Warning: could not configure MOST_RECENT_SAMPLE cursor. "
                        "Falling back to queue-drain latest mode. "
                        f"Detail: {exc}"
                    )
                    self._latest_read_warned = True
        self.st_task = self.pda._build_st_task()
        self.clk_task = self.pda._build_clk_task()
        self.ai_sample_clk_task = self.pda._build_ai_sample_clk_task()
        for task_obj, task_label in (
            (self.st_task, "ST"),
            (self.clk_task, "CLK"),
            (self.ai_sample_clk_task, "AI sample clock"),
        ):
            if task_obj is None:
                continue
            try:
                task_obj.co_channels[0].co_enable_initial_delay_on_retrigger = bool(
                    self.pda.retrigger_enable_initial_delay
                )
            except Exception as exc:
                if not self.pda._retrigger_delay_warned:
                    print(
                        "Warning: could not set CO.EnableInitialDelayOnRetrigger "
                        f"for {task_label} task: {exc}"
                    )
                    self.pda._retrigger_delay_warned = True

        self.pda._apply_start_trigger_chain(
            self.ai_task,
            self.st_task,
            self.clk_task,
            ai_sample_clk_task=self.ai_sample_clk_task,
        )
        self.pda._set_retriggerable(self.ai_task, True)
        self.pda._set_retriggerable(self.clk_task, True)
        if self.ai_sample_clk_task is not None:
            self.pda._set_retriggerable(self.ai_sample_clk_task, True)
        self.pda._set_retriggerable(self.st_task, True)

        # Arm downstream first.
        self.ai_task.start()
        if self.ai_sample_clk_task is not None:
            self.ai_sample_clk_task.start()
        self.clk_task.start()
        self.st_task.start()
        return self

    def read_line(self, timeout=10.0):
        line_samples = int(self.pda.ai_samples_per_line)
        self.last_lines_consumed = 1

        if self.latest_only_read and self._latest_cursor_enabled:
            data = self.ai_task.read(
                number_of_samples_per_channel=line_samples,
                timeout=float(timeout),
            )
            return self.pda._format_ai_read_data(data, read_reference=self.read_reference)

        if self.latest_only_read:
            try:
                avail = int(self.ai_task.in_stream.avail_samp_per_chan)
            except Exception:
                avail = 0

            # Drain complete queued lines and keep the newest full line.
            if avail >= line_samples:
                lines_ready = max(1, avail // line_samples)
                samples_to_read = int(lines_ready * line_samples)
                data = self.ai_task.read(
                    number_of_samples_per_channel=samples_to_read,
                    timeout=float(timeout),
                )
                self.last_lines_consumed = lines_ready
                arr = np.asarray(data, dtype=float)
                if self.read_reference:
                    if not (arr.ndim == 2 and arr.shape[0] == 2):
                        # Fall back to existing formatter for safety.
                        return self.pda._format_ai_read_data(
                            data, read_reference=self.read_reference
                        )
                    newest = arr[:, -line_samples:]
                    return self.pda._format_ai_read_data(
                        newest, read_reference=self.read_reference
                    )
                if arr.ndim == 2 and arr.shape[0] == 1:
                    arr = arr[0]
                newest = arr[-line_samples:]
                return self.pda._format_ai_read_data(
                    newest, read_reference=self.read_reference
                )

        data = self.ai_task.read(
            number_of_samples_per_channel=line_samples,
            timeout=float(timeout),
        )
        return self.pda._format_ai_read_data(data, read_reference=self.read_reference)

    def read_available_lines(self, max_lines=16, timeout=10.0):
        """
        Read one or more ordered complete lines from the persistent AI buffer.

        This amortizes NI-DAQmx/Python read overhead in high-throughput live
        modes while preserving line order for phase-sensitive demodulation.
        """
        line_samples = int(self.pda.ai_samples_per_line)
        max_lines = max(1, int(max_lines))
        self.last_lines_consumed = 1

        # Intentionally read a small fixed batch rather than one line at a
        # time. DAQmx will block until this many complete lines are available,
        # which greatly reduces Python/driver call overhead while preserving
        # ordered line-by-line processing downstream.
        lines_to_read = max_lines
        samples_to_read = int(lines_to_read * line_samples)

        data = self.ai_task.read(
            number_of_samples_per_channel=samples_to_read,
            timeout=float(timeout),
        )
        self.last_lines_consumed = int(lines_to_read)

        arr = np.asarray(data, dtype=float)
        lines = []
        if self.read_reference:
            if not (arr.ndim == 2 and arr.shape[0] == 2):
                if lines_to_read == 1:
                    return [
                        self.pda._format_ai_read_data(
                            data,
                            read_reference=self.read_reference,
                        )
                    ]
                raise RuntimeError(
                    "Dual-channel batch read expected shape (2, n_samples), "
                    f"got {tuple(arr.shape)}."
                )
            for idx in range(lines_to_read):
                start = idx * line_samples
                stop = start + line_samples
                lines.append(
                    self.pda._format_ai_read_data(
                        arr[:, start:stop],
                        read_reference=True,
                    )
                )
            return lines

        if arr.ndim == 2 and arr.shape[0] == 1:
            arr = arr[0]
        for idx in range(lines_to_read):
            start = idx * line_samples
            stop = start + line_samples
            lines.append(
                self.pda._format_ai_read_data(
                    arr[start:stop],
                    read_reference=False,
                )
            )
        return lines

    def close(self):
        for task in (self.st_task, self.clk_task, self.ai_sample_clk_task, self.ai_task):
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
        self.ai_sample_clk_task = None

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class _CounterEdgeMonitorSession:
    """
    Monitor digital edge activity on a chosen DAQ terminal using a spare counter.

    Reports edge-rate (Hz). This is not an analog-voltage waveform.
    """

    def __init__(
        self,
        pda,
        *,
        source_terminal,
        counter="ctr2",
        rate_gate_s=0.05,
        task_name="PDA_EDGE_MON",
    ):
        self.pda = pda
        self.source_terminal = str(source_terminal)
        self.counter = f"{self.pda.device}/{str(counter).lstrip('/')}"
        self.task_name = str(task_name)
        # Allow short gates for higher-rate diagnostics.
        self.rate_gate_s = max(0.001, float(rate_gate_s))
        self.task = None
        self.last_count = 0
        self.last_t = 0.0
        self.gate_start_count = 0
        self.gate_start_t = 0.0
        self.last_rate_hz = float("nan")
        self.available = False
        self.error_text = ""

    def __enter__(self):
        try:
            self.task = nidaqmx.Task(self.task_name)
            ch = self.task.ci_channels.add_ci_count_edges_chan(
                counter=self.counter,
                edge=Edge.RISING,
                initial_count=0,
            )
            self.task.ci_channels[0].ci_count_edges_term = self.source_terminal
            # PFI terminals are shared resources: if another task on this terminal
            # enabled a digital filter, NI-DAQmx requires matching filter settings.
            try:
                ch.ci_count_edges_dig_fltr_enable = bool(
                    self.pda.trigger_filter_enable
                )
                if self.pda.trigger_filter_enable:
                    ch.ci_count_edges_dig_fltr_min_pulse_width = float(
                        self.pda.trigger_filter_min_pulse_width_s
                    )
            except Exception:
                pass
            try:
                ch.ci_count_edges_dig_sync_enable = bool(
                    self.pda.trigger_sync_enable
                )
            except Exception:
                pass
            self.task.start()
            self.last_count = int(self.task.read())
            self.last_t = time.perf_counter()
            self.gate_start_count = self.last_count
            self.gate_start_t = self.last_t
            self.last_rate_hz = float("nan")
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
        gate_dt = now - self.gate_start_t
        gate_dc = count - self.gate_start_count
        if gate_dc < 0:
            gate_dc = 0
            self.gate_start_count = count
            self.gate_start_t = now
            gate_dt = 0.0
        if gate_dt >= self.rate_gate_s:
            self.last_rate_hz = float(gate_dc / max(gate_dt, 1e-6))
            self.gate_start_count = count
            self.gate_start_t = now

        self.last_t = now
        self.last_count = count
        return self.last_rate_hz, count

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


class _PFI9EdgeMonitorSession(_CounterEdgeMonitorSession):
    """
    Monitor PFI9 digital activity using a spare counter.
    """

    def __init__(self, pda, counter="ctr2", rate_gate_s=0.05):
        super().__init__(
            pda,
            source_terminal=pda.trig_in,
            counter=counter,
            rate_gate_s=rate_gate_s,
            task_name="PDA_PFI9_MON",
        )


class _ChopperInputEdgeMonitorSession(_CounterEdgeMonitorSession):
    """
    Monitor external chopper FOUT TTL activity on the configured chopper input.
    """

    def __init__(self, pda, counter="ctr3", rate_gate_s=0.05):
        super().__init__(
            pda,
            source_terminal=pda.chopper_input_term,
            counter=counter,
            rate_gate_s=rate_gate_s,
            task_name="PDA_CHOPPER_IN_MON",
        )


class _ChopperInputStateSession:
    """
    Read the instantaneous logic level of the configured chopper input terminal.

    This provides a per-line hardware phase label that can be used instead of
    inferred odd/even or tail-guided phase assignment.
    """

    def __init__(self, pda, task_name="PDA_CHOPPER_IN_STATE"):
        self.pda = pda
        self.task_name = str(task_name)
        self.task = None
        self.available = False
        self.error_text = ""

    def __enter__(self):
        try:
            self.task = nidaqmx.Task(self.task_name)
            self.task.di_channels.add_di_chan(
                lines=self.pda.chopper_input_term,
                line_grouping=LineGrouping.CHAN_PER_LINE,
            )
            self.available = True
        except Exception as exc:
            self.error_text = str(exc)
            self.available = False
            self.close()
        return self

    def read_state(self):
        if self.task is None or not self.available:
            return None
        try:
            value = self.task.read()
        except Exception:
            return None
        if isinstance(value, (list, tuple, np.ndarray)):
            if len(value) <= 0:
                return None
            value = value[0]
        return 1 if bool(value) else 0

    def close(self):
        if self.task is not None:
            try:
                self.task.close()
            except Exception:
                pass
        self.task = None

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class _ChopperSyncOutSession:
    """
    Generate a hardware chopper reference on PFI3 from the PFI9 trigger stream.

    This uses a spare counter in pulse-ticks mode, with the incoming trigger
    used as the tick source. For the default 1 kHz trigger and 1 high tick /
    1 low tick, the output is a phase-derived 500 Hz TTL reference.
    """

    def __init__(self, pda):
        self.pda = pda
        self.task = None
        self.available = False
        self.error_text = ""

    def __enter__(self):
        if not self.pda.chopper_sync_enable:
            return self
        try:
            self.task = nidaqmx.Task("PDA_CHOP_SYNC")
            ch = self.task.co_channels.add_co_pulse_chan_ticks(
                counter=self.pda.chopper_sync_counter,
                source_terminal=self.pda.chopper_sync_source_terminal,
                idle_state=Level.LOW,
                initial_delay=self.pda.chopper_sync_initial_delay_ticks,
                low_ticks=self.pda.chopper_sync_low_ticks,
                high_ticks=self.pda.chopper_sync_high_ticks,
            )
            # For divide-by-2 sync we want a short pulse on each terminal count,
            # not the counter's regular pulse-train output terminal. Exporting
            # the counter output event in pulse mode yields one TTL pulse every
            # two source edges when high/low ticks are both set to 2.
            self.task.export_signals.ctr_out_event_output_term = (
                self.pda.chopper_sync_out_term
            )
            self.task.export_signals.ctr_out_event_output_behavior = (
                ExportAction.PULSE
            )
            self.task.export_signals.ctr_out_event_pulse_polarity = (
                Polarity.ACTIVE_HIGH
            )
            try:
                ch.co_ctr_timebase_dig_sync_enable = bool(
                    self.pda.trigger_sync_enable
                )
            except Exception:
                pass
            try:
                ch.co_ctr_timebase_dig_fltr_enable = bool(
                    self.pda.trigger_filter_enable
                )
                if self.pda.trigger_filter_enable:
                    ch.co_ctr_timebase_dig_fltr_min_pulse_width = float(
                        self.pda.trigger_filter_min_pulse_width_s
                    )
            except Exception:
                pass
            self.task.timing.cfg_implicit_timing(
                sample_mode=AcquisitionType.CONTINUOUS
            )
            self.task.start()
            self.available = True
        except Exception as exc:
            self.error_text = str(exc)
            self.available = False
            self.close()
        return self

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


class _BackgroundLineReader:
    """
    Background DAQ line reader.

    Reads lines continuously in a worker thread and stores them in FIFO order.

    This preserves shot ordering for pump-chop odd/even demod while still
    decoupling acquisition from plotting. When the FIFO fills, the reader
    applies backpressure rather than silently dropping lines.
    """

    def __init__(self, read_fn, max_packets=256):
        self.read_fn = read_fn
        self._cond = threading.Condition()
        self._stop = threading.Event()
        self._thread = None
        self._packets = deque()
        self._error = None
        self._seq = 0
        self._lines_total = 0
        self._acq_total_s = 0.0
        self._max_packets = max(8, int(max_packets))
        self._max_queue_depth = 0

    def start(self):
        if self._thread is not None:
            return self
        self._thread = threading.Thread(
            target=self._run,
            name="PDA_BackgroundReader",
            daemon=True,
        )
        self._thread.start()
        return self

    def _run(self):
        while not self._stop.is_set():
            t0 = time.perf_counter()
            try:
                data, lines_consumed = self.read_fn()
            except Exception as exc:
                with self._cond:
                    self._error = exc
                    self._cond.notify_all()
                return

            elapsed_s = max(0.0, time.perf_counter() - t0)
            consumed = max(1, int(lines_consumed))
            with self._cond:
                while (
                    len(self._packets) >= self._max_packets
                    and not self._stop.is_set()
                ):
                    self._cond.wait(timeout=0.05)
                if self._stop.is_set():
                    return
                self._seq += 1
                self._lines_total += consumed
                self._acq_total_s += elapsed_s
                self._packets.append(
                    {
                        "seq": int(self._seq),
                        "data": data,
                        "lines_total": int(self._lines_total),
                        "acq_total_s": float(self._acq_total_s),
                        "elapsed_s": float(elapsed_s),
                    }
                )
                self._max_queue_depth = max(self._max_queue_depth, len(self._packets))
                self._cond.notify_all()

    def pop_next(self, wait_timeout_s=0.0):
        wait_timeout_s = max(0.0, float(wait_timeout_s))
        with self._cond:
            if (
                wait_timeout_s > 0.0
                and not self._packets
                and self._error is None
                and not self._stop.is_set()
            ):
                self._cond.wait(timeout=wait_timeout_s)
            packet = self._packets.popleft() if self._packets else None
            if packet is not None:
                self._cond.notify_all()
            err = self._error
            depth = len(self._packets)
            max_depth = self._max_queue_depth
        return packet, err, depth, max_depth

    def close(self, join_timeout_s=1.0):
        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=max(0.0, float(join_timeout_s)))
        self._thread = None


class _LiveLineAccumulator:
    """Maintain running raw/integrated/demod state for live plotting."""

    def __init__(
        self,
        initial_size,
        integration_line_count=128,
        pump_chop_demod=False,
        pump_chop_sign=-1.0,
        pump_chop_phase_source="inferred",
        pump_chop_use_adjacent_pairs=False,
        pump_chop_sign_agnostic_preview=False,
        pump_chop_tail_heuristic_enable=False,
        pump_chop_tail_heuristic_start=900,
        pump_chop_tail_heuristic_stop=1000,
        pump_chop_tail_heuristic_expected_sign=1.0,
        pump_chop_tail_heuristic_zero_baseline=True,
        reference_processing_mode="difference",
        reference_ratio_floor=1e-6,
        channel_dark_main_offset=None,
        channel_dark_ref_offset=None,
        pump_chop_dark_offset=None,
        capture_hit_rate_enable=True,
        capture_hit_rate_window_lines=256,
        capture_hit_warmup_lines=64,
        capture_hit_threshold_fraction=0.45,
    ):
        self.integration_line_count = max(1, int(integration_line_count))
        self.pump_chop_demod = bool(pump_chop_demod)
        self.pump_chop_sign = float(pump_chop_sign)
        self.pump_chop_phase_source = str(pump_chop_phase_source).strip().lower()
        if self.pump_chop_phase_source not in ("inferred", "chopper_input"):
            raise ValueError(
                "pump_chop_phase_source must be 'inferred' or 'chopper_input'."
            )
        self.pump_chop_use_adjacent_pairs = bool(pump_chop_use_adjacent_pairs)
        self.pump_chop_sign_agnostic_preview = bool(
            pump_chop_sign_agnostic_preview
        )
        self.pump_chop_tail_heuristic_enable = bool(
            pump_chop_tail_heuristic_enable
        )
        self.pump_chop_tail_heuristic_start = int(pump_chop_tail_heuristic_start)
        self.pump_chop_tail_heuristic_stop = int(pump_chop_tail_heuristic_stop)
        self.pump_chop_tail_heuristic_expected_sign = float(
            pump_chop_tail_heuristic_expected_sign
        )
        self.pump_chop_tail_heuristic_zero_baseline = bool(
            pump_chop_tail_heuristic_zero_baseline
        )
        self.reference_processing_mode = str(reference_processing_mode).strip().lower()
        if self.reference_processing_mode not in ("difference", "ratio"):
            raise ValueError(
                "reference_processing_mode must be 'difference' or 'ratio'."
            )
        self.reference_ratio_floor = max(1e-12, float(reference_ratio_floor))
        self.channel_dark_main_offset = None
        self.channel_dark_ref_offset = None
        self.pump_chop_dark_offset = None
        self.capture_hit_rate_enable = bool(capture_hit_rate_enable)
        self.capture_hit_rate_window_lines = max(
            16, int(capture_hit_rate_window_lines)
        )
        self.capture_hit_warmup_lines = max(8, int(capture_hit_warmup_lines))
        self.capture_hit_threshold_fraction = min(
            1.0, max(0.0, float(capture_hit_threshold_fraction))
        )
        self.capture_score_recent = deque(
            maxlen=max(
                self.capture_hit_rate_window_lines,
                self.capture_hit_warmup_lines,
                128,
            )
        )
        self.capture_hit_flags = deque(maxlen=self.capture_hit_rate_window_lines)
        self.line_counter = 0
        self.demod_accept_count = 0
        self.demod_reject_count = 0
        self.demod_reject_missing_count = 0
        self.demod_multi_edge_count = 0
        self.demod_reject_no_prev_count = 0
        self.demod_last_line_phase = None
        self.chop_phase = 0
        self.chop_prev_line = None
        self.chop_prev_phase = None
        self.chop_pair_counter = 0
        self.chop_preview_flip_counter = 0
        self.chop_parity_reset_counter = 0
        self.chop_preview_template = None
        self.chop_parity_warned = False
        self.tail_guided_flip_counter = 0
        self.tail_guided_last_mean = float("nan")
        self._pending_parity_reset = False
        self._pending_parity_reset_reason = ""
        self.last_warning_message = None
        self._reset_size(int(initial_size))
        self.set_channel_dark_offsets(
            channel_dark_main_offset,
            channel_dark_ref_offset,
        )
        self.set_pump_chop_dark_offset(pump_chop_dark_offset)

    def _reset_size(self, size):
        size = max(1, int(size))
        self.size = size
        self.line_buffer = deque(maxlen=self.integration_line_count)
        self.integration_sum = np.zeros(size, dtype=float)
        self.ref_line_buffer = deque(maxlen=self.integration_line_count)
        self.ref_integration_sum = np.zeros(size, dtype=float)
        self.chop_phase0_buffer = deque(maxlen=self.integration_line_count)
        self.chop_phase1_buffer = deque(maxlen=self.integration_line_count)
        self.chop_phase0_sum = np.zeros(size, dtype=float)
        self.chop_phase1_sum = np.zeros(size, dtype=float)
        self.chop_ref_phase0_buffer = deque(maxlen=self.integration_line_count)
        self.chop_ref_phase1_buffer = deque(maxlen=self.integration_line_count)
        self.chop_ref_phase0_sum = np.zeros(size, dtype=float)
        self.chop_ref_phase1_sum = np.zeros(size, dtype=float)
        self.chop_pair_buffer = deque(maxlen=self.integration_line_count)
        self.chop_pair_sum = np.zeros(size, dtype=float)
        self.chop_pair_dark_corrected_buffer = deque(maxlen=self.integration_line_count)
        self.chop_pair_dark_corrected_sum = np.zeros(size, dtype=float)
        self.latest_line = np.zeros(size, dtype=float)
        self.integrated_line = np.zeros(size, dtype=float)
        self.ref_line = None
        self.ref_integrated_line = None
        self.diff_line = None
        self.diff_integrated_line = None
        self.latest_chop_pair = None
        self.latest_chop_integrated = None
        self.latest_chop_integrated_pairwise_dark_corrected = None
        self.latest_chop_main_diagnostic = None
        self.latest_chop_ref_diagnostic = None
        self.integrated_auc_main = float("nan")
        self.integrated_auc_ref = float("nan")
        self.integrated_auc_diff = float("nan")
        self.capture_hit_rate_pct = float("nan")
        self.capture_last_score_vpp = float("nan")
        self.capture_last_threshold_vpp = float("nan")
        self.capture_score_recent.clear()
        self.capture_hit_flags.clear()
        self.chop_phase = 0
        self.chop_prev_line = None
        self.chop_prev_phase = None
        self.chop_pair_counter = 0
        self.chop_preview_flip_counter = 0
        self.chop_parity_reset_counter = 0
        self.chop_preview_template = None
        self.demod_last_line_phase = None
        self.tail_guided_flip_counter = 0
        self.tail_guided_last_mean = float("nan")
        if self.channel_dark_main_offset is not None and self.channel_dark_main_offset.size != size:
            self.channel_dark_main_offset = None
        if self.channel_dark_ref_offset is not None and self.channel_dark_ref_offset.size != size:
            self.channel_dark_ref_offset = None
        if self.pump_chop_dark_offset is not None and self.pump_chop_dark_offset.size != size:
            self.pump_chop_dark_offset = None

    def _reference_ratio(self, numerator, denominator):
        numerator = np.asarray(numerator, dtype=float)
        denominator = np.asarray(denominator, dtype=float)
        safe_den = denominator.copy()
        small_mask = np.abs(safe_den) < self.reference_ratio_floor
        if np.any(small_mask):
            safe_den[small_mask] = np.where(
                safe_den[small_mask] < 0.0,
                -self.reference_ratio_floor,
                self.reference_ratio_floor,
            )
        ratio = np.divide(numerator, safe_den, out=np.zeros_like(numerator), where=np.isfinite(safe_den))
        ratio[~np.isfinite(ratio)] = 0.0
        return ratio

    def _safe_positive_ratio(self, numerator, denominator):
        ratio = self._reference_ratio(numerator, denominator)
        return np.maximum(ratio, self.reference_ratio_floor)

    def _compute_delta_od(self, pumped_ratio, unpumped_ratio):
        pumped_safe = np.maximum(np.asarray(pumped_ratio, dtype=float), self.reference_ratio_floor)
        unpumped_safe = np.maximum(np.asarray(unpumped_ratio, dtype=float), self.reference_ratio_floor)
        return -np.log10(np.divide(pumped_safe, unpumped_safe))

    def _compute_single_channel_delta_od(self, pumped_signal, unpumped_signal, dark_offset):
        pumped = self._apply_channel_dark(pumped_signal, dark_offset)
        unpumped = self._apply_channel_dark(unpumped_signal, dark_offset)
        if pumped is None or unpumped is None:
            return None
        pumped_safe = np.asarray(pumped, dtype=float)
        unpumped_safe = np.asarray(unpumped, dtype=float)
        valid = (
            np.isfinite(pumped_safe)
            & np.isfinite(unpumped_safe)
            & (pumped_safe > self.reference_ratio_floor)
            & (unpumped_safe > self.reference_ratio_floor)
        )
        ratio = np.divide(
            pumped_safe,
            unpumped_safe,
            out=np.ones_like(pumped_safe),
            where=valid,
        )
        valid &= np.isfinite(ratio) & (ratio > 0.0)
        out = np.full(pumped_safe.shape, np.nan, dtype=float)
        out[valid] = -np.log10(ratio[valid])
        return out

    def _referenced_phase_diagnostics(self):
        self.latest_chop_main_diagnostic = None
        self.latest_chop_ref_diagnostic = None
        if not (
            self.reference_processing_mode == "ratio"
            and self.ref_line is not None
            and len(self.chop_phase0_buffer) > 0
            and len(self.chop_phase1_buffer) > 0
            and len(self.chop_ref_phase0_buffer) > 0
            and len(self.chop_ref_phase1_buffer) > 0
        ):
            return None

        phase0_main_mean = self.chop_phase0_sum / float(len(self.chop_phase0_buffer))
        phase1_main_mean = self.chop_phase1_sum / float(len(self.chop_phase1_buffer))
        phase0_ref_mean = self.chop_ref_phase0_sum / float(len(self.chop_ref_phase0_buffer))
        phase1_ref_mean = self.chop_ref_phase1_sum / float(len(self.chop_ref_phase1_buffer))

        if self.pump_chop_sign > 0:
            pumped_main = phase1_main_mean
            unpumped_main = phase0_main_mean
            pumped_ref = phase1_ref_mean
            unpumped_ref = phase0_ref_mean
        else:
            pumped_main = phase0_main_mean
            unpumped_main = phase1_main_mean
            pumped_ref = phase0_ref_mean
            unpumped_ref = phase1_ref_mean

        self.latest_chop_main_diagnostic = self._compute_single_channel_delta_od(
            pumped_main,
            unpumped_main,
            self.channel_dark_main_offset,
        )
        self.latest_chop_ref_diagnostic = self._compute_single_channel_delta_od(
            pumped_ref,
            unpumped_ref,
            self.channel_dark_ref_offset,
        )

        pumped_ratio = self._safe_positive_ratio(
            self._apply_channel_dark(pumped_main, self.channel_dark_main_offset),
            self._apply_channel_dark(pumped_ref, self.channel_dark_ref_offset),
        )
        unpumped_ratio = self._safe_positive_ratio(
            self._apply_channel_dark(unpumped_main, self.channel_dark_main_offset),
            self._apply_channel_dark(unpumped_ref, self.channel_dark_ref_offset),
        )
        return self._compute_delta_od(pumped_ratio, unpumped_ratio)

    def set_channel_dark_offsets(self, main_offset=None, ref_offset=None):
        if main_offset is None:
            self.channel_dark_main_offset = None
        else:
            arr = np.asarray(main_offset, dtype=float).copy()
            if arr.size != self.size:
                raise ValueError(
                    f"main channel dark size mismatch ({arr.size} != {self.size})"
                )
            self.channel_dark_main_offset = arr
        if ref_offset is None:
            self.channel_dark_ref_offset = None
        else:
            arr = np.asarray(ref_offset, dtype=float).copy()
            if arr.size != self.size:
                raise ValueError(
                    f"reference channel dark size mismatch ({arr.size} != {self.size})"
                )
            self.channel_dark_ref_offset = arr

    def set_pump_chop_dark_offset(self, dark_offset=None):
        if dark_offset is None:
            self.pump_chop_dark_offset = None
        else:
            arr = np.asarray(dark_offset, dtype=float).copy()
            if arr.size != self.size:
                raise ValueError(
                    f"pump-chop dark size mismatch ({arr.size} != {self.size})"
                )
            self.pump_chop_dark_offset = arr
        self.chop_pair_dark_corrected_buffer.clear()
        self.chop_pair_dark_corrected_sum = np.zeros(self.size, dtype=float)
        self.latest_chop_integrated_pairwise_dark_corrected = None

    def _apply_channel_dark(self, line, dark_offset):
        if line is None:
            return None
        arr = np.asarray(line, dtype=float)
        if dark_offset is None:
            return arr.copy()
        return arr - dark_offset

    def _corrected_main_line(self, line=None):
        src = self.latest_line if line is None else line
        return self._apply_channel_dark(src, self.channel_dark_main_offset)

    def _corrected_ref_line(self, line=None):
        if self.ref_line is None and line is None:
            return None
        src = self.ref_line if line is None else line
        return self._apply_channel_dark(src, self.channel_dark_ref_offset)

    def _demod_source_line(self):
        if self.reference_processing_mode == "ratio" and self.diff_line is not None:
            return self.diff_line
        return self.latest_line

    def request_parity_reset(self, reason):
        self._pending_parity_reset = True
        self._pending_parity_reset_reason = str(reason)

    def set_pump_chop_sign(self, sign):
        """Update demod sign live without throwing away accumulated state."""
        sign = float(sign)
        if sign == 0.0:
            raise ValueError("pump_chop_sign must be non-zero.")
        old_sign = float(self.pump_chop_sign)
        if sign == old_sign:
            return
        ratio = sign / old_sign
        self.pump_chop_sign = sign
        if self.latest_chop_pair is not None:
            self.latest_chop_pair = self.latest_chop_pair * ratio
        if self.latest_chop_integrated is not None:
            self.latest_chop_integrated = self.latest_chop_integrated * ratio
        if self.latest_chop_integrated_pairwise_dark_corrected is not None:
            self.latest_chop_integrated_pairwise_dark_corrected = (
                self.latest_chop_integrated_pairwise_dark_corrected * ratio
            )
        if self.latest_chop_main_diagnostic is not None:
            self.latest_chop_main_diagnostic = self.latest_chop_main_diagnostic * ratio
        if self.latest_chop_ref_diagnostic is not None:
            self.latest_chop_ref_diagnostic = self.latest_chop_ref_diagnostic * ratio
        if self.pump_chop_dark_offset is not None:
            self.pump_chop_dark_offset = self.pump_chop_dark_offset * ratio
        if self.pump_chop_use_adjacent_pairs or self.pump_chop_tail_heuristic_enable:
            self.chop_pair_buffer = deque(
                (pair * ratio for pair in self.chop_pair_buffer),
                maxlen=self.chop_pair_buffer.maxlen,
            )
            self.chop_pair_sum = self.chop_pair_sum * ratio
            if self.chop_preview_template is not None:
                self.chop_preview_template = self.chop_preview_template * ratio
        if len(self.chop_pair_dark_corrected_buffer) > 0:
            self.chop_pair_dark_corrected_buffer = deque(
                (pair * ratio for pair in self.chop_pair_dark_corrected_buffer),
                maxlen=self.chop_pair_dark_corrected_buffer.maxlen,
            )
            self.chop_pair_dark_corrected_sum = (
                self.chop_pair_dark_corrected_sum * ratio
            )

    def _update_pairwise_dark_corrected_referenced(self, chop_pair):
        if not (
            self.reference_processing_mode == "ratio"
            and self.pump_chop_dark_offset is not None
        ):
            return
        pair = np.asarray(chop_pair, dtype=float)
        if self.pump_chop_dark_offset.size != pair.size:
            return
        corrected = pair - self.pump_chop_dark_offset
        if len(self.chop_pair_dark_corrected_buffer) == self.chop_pair_dark_corrected_buffer.maxlen:
            self.chop_pair_dark_corrected_sum -= self.chop_pair_dark_corrected_buffer.popleft()
        self.chop_pair_dark_corrected_buffer.append(corrected.copy())
        self.chop_pair_dark_corrected_sum += corrected
        self.latest_chop_integrated_pairwise_dark_corrected = (
            self.chop_pair_dark_corrected_sum
            / float(len(self.chop_pair_dark_corrected_buffer))
        )

    def _tail_guided_pair(self, raw_pair):
        pair = np.asarray(raw_pair, dtype=float).copy()
        if not self.pump_chop_tail_heuristic_enable:
            return self.pump_chop_sign * pair

        start = max(0, min(self.size - 1, int(self.pump_chop_tail_heuristic_start)))
        stop = max(start + 1, min(self.size, int(self.pump_chop_tail_heuristic_stop)))
        tail = pair[start:stop]
        if tail.size <= 0:
            return self.pump_chop_sign * pair

        tail_mean = float(np.mean(tail))
        if np.isfinite(tail_mean):
            if (
                self.pump_chop_tail_heuristic_expected_sign != 0.0
                and tail_mean * self.pump_chop_tail_heuristic_expected_sign < 0.0
            ):
                pair = -pair
                tail_mean = -tail_mean
                self.tail_guided_flip_counter += 1
            self.tail_guided_last_mean = tail_mean
            if self.pump_chop_tail_heuristic_zero_baseline:
                pair = pair - tail_mean

        return self.pump_chop_sign * pair

    def _tail_guided_reference_delta_od(self, current_ratio, previous_ratio):
        current = np.asarray(current_ratio, dtype=float)
        previous = np.asarray(previous_ratio, dtype=float)
        ratio_pair = current - previous

        start = max(0, min(ratio_pair.size - 1, int(self.pump_chop_tail_heuristic_start)))
        stop = max(start + 1, min(ratio_pair.size, int(self.pump_chop_tail_heuristic_stop)))
        flip_pair = False
        tail_mean = float("nan")

        if self.pump_chop_tail_heuristic_enable:
            tail = ratio_pair[start:stop]
            if tail.size > 0:
                tail_mean = float(np.mean(tail))
                if np.isfinite(tail_mean):
                    if (
                        self.pump_chop_tail_heuristic_expected_sign != 0.0
                        and tail_mean * self.pump_chop_tail_heuristic_expected_sign < 0.0
                    ):
                        flip_pair = True
                        tail_mean = -tail_mean
                        self.tail_guided_flip_counter += 1
                    self.tail_guided_last_mean = tail_mean

        delta_od = self._compute_delta_od(current, previous)
        if flip_pair:
            delta_od = -delta_od
        delta_od = self.pump_chop_sign * delta_od

        if self.pump_chop_tail_heuristic_enable and self.pump_chop_tail_heuristic_zero_baseline:
            tail = delta_od[start:stop]
            if tail.size > 0:
                finite_tail = tail[np.isfinite(tail)]
                if finite_tail.size > 0:
                    delta_od = delta_od - float(np.mean(finite_tail))

        return delta_od

    def _apply_parity_reset(self, reason):
        self.chop_phase = 0
        self.chop_prev_line = None
        self.chop_prev_phase = None
        self.demod_last_line_phase = None
        self.chop_phase0_buffer.clear()
        self.chop_phase1_buffer.clear()
        self.chop_phase0_sum = np.zeros(self.size, dtype=float)
        self.chop_phase1_sum = np.zeros(self.size, dtype=float)
        self.chop_ref_phase0_buffer.clear()
        self.chop_ref_phase1_buffer.clear()
        self.chop_ref_phase0_sum = np.zeros(self.size, dtype=float)
        self.chop_ref_phase1_sum = np.zeros(self.size, dtype=float)
        self.chop_pair_buffer.clear()
        self.chop_pair_sum = np.zeros(self.size, dtype=float)
        self.chop_pair_dark_corrected_buffer.clear()
        self.chop_pair_dark_corrected_sum = np.zeros(self.size, dtype=float)
        self.latest_chop_pair = None
        self.latest_chop_integrated = None
        self.latest_chop_integrated_pairwise_dark_corrected = None
        self.latest_chop_main_diagnostic = None
        self.latest_chop_ref_diagnostic = None
        self.chop_preview_template = None
        self.chop_parity_reset_counter += 1
        if not self.chop_parity_warned:
            self.last_warning_message = (
                "Warning: reset chop demod parity due to trigger/line discontinuity "
                f"({reason})."
            )
            self.chop_parity_warned = True

    def process_line(
        self,
        line,
        ref_line=None,
        lines_consumed=1,
        demod_qual_active=False,
        edge_delta_since_last_line=None,
        external_phase_state=None,
    ):
        self.last_warning_message = None
        line = np.asarray(line, dtype=float)
        ref_arr = None if ref_line is None else np.asarray(ref_line, dtype=float)
        if line.size != self.size:
            self._reset_size(line.size)
            self.request_parity_reset("line size changed")

        self.line_counter += max(1, int(lines_consumed))
        self.latest_line = line.copy()
        self.ref_line = None if ref_arr is None else ref_arr.copy()

        if len(self.line_buffer) == self.line_buffer.maxlen:
            self.integration_sum -= self.line_buffer.popleft()
        self.line_buffer.append(line.copy())
        self.integration_sum += line
        self.integrated_line = self.integration_sum / float(len(self.line_buffer))

        if ref_arr is not None:
            if len(self.ref_line_buffer) == self.ref_line_buffer.maxlen:
                self.ref_integration_sum -= self.ref_line_buffer.popleft()
            self.ref_line_buffer.append(ref_arr.copy())
            self.ref_integration_sum += ref_arr
            self.ref_integrated_line = (
                self.ref_integration_sum / float(len(self.ref_line_buffer))
            )
            if self.reference_processing_mode == "ratio":
                corrected_main = self._corrected_main_line(self.latest_line)
                corrected_ref = self._corrected_ref_line(self.ref_line)
                corrected_integrated_main = self._corrected_main_line(
                    self.integrated_line
                )
                corrected_integrated_ref = self._corrected_ref_line(
                    self.ref_integrated_line
                )
                self.diff_line = self._reference_ratio(corrected_main, corrected_ref)
                self.diff_integrated_line = self._reference_ratio(
                    corrected_integrated_main,
                    corrected_integrated_ref,
                )
            else:
                self.diff_line = self.latest_line - self.ref_line
                self.diff_integrated_line = (
                    self.integrated_line - self.ref_integrated_line
                )
        else:
            self.ref_integrated_line = None
            self.diff_line = None
            self.diff_integrated_line = None

        self.integrated_auc_main = float(np.trapz(self.integrated_line))
        if self.ref_integrated_line is not None:
            self.integrated_auc_ref = float(np.trapz(self.ref_integrated_line))
            self.integrated_auc_diff = float(np.trapz(self.diff_integrated_line))
        else:
            self.integrated_auc_ref = float("nan")
            self.integrated_auc_diff = float("nan")

        if self.capture_hit_rate_enable:
            capture_score = float(np.ptp(self.latest_line))
            self.capture_last_score_vpp = capture_score
            self.capture_score_recent.append(capture_score)
            if len(self.capture_score_recent) >= self.capture_hit_warmup_lines:
                score_arr = np.asarray(self.capture_score_recent, dtype=float)
                score_lo = float(np.quantile(score_arr, 0.10))
                score_hi = float(np.quantile(score_arr, 0.90))
                score_span = max(1e-12, score_hi - score_lo)
                capture_threshold = (
                    score_lo + self.capture_hit_threshold_fraction * score_span
                )
                self.capture_last_threshold_vpp = capture_threshold
                self.capture_hit_flags.append(capture_score >= capture_threshold)
                self.capture_hit_rate_pct = (
                    100.0 * float(np.mean(self.capture_hit_flags))
                    if self.capture_hit_flags
                    else float("nan")
                )

        if not self.pump_chop_demod:
            return

        demod_line = self._demod_source_line()

        parity_reset_needed = False
        local_reset_reason = ""
        if self._pending_parity_reset:
            parity_reset_needed = True
            local_reset_reason = self._pending_parity_reset_reason
            self._pending_parity_reset = False
            self._pending_parity_reset_reason = ""

        demod_line_accepted = True
        demod_line_phase = None
        if self.pump_chop_phase_source == "chopper_input":
            if external_phase_state is None:
                demod_line_accepted = False
                self.demod_reject_count += 1
                self.demod_reject_missing_count += 1
                parity_reset_needed = True
                local_reset_reason = "missing chopper-input phase label"
            else:
                demod_line_phase = int(bool(external_phase_state))
                self.demod_accept_count += 1
                self.demod_last_line_phase = demod_line_phase
        elif demod_qual_active:
            if edge_delta_since_last_line is None:
                if self.demod_last_line_phase is None:
                    demod_line_phase = 0
                else:
                    demod_line_accepted = False
                    self.demod_reject_no_prev_count += 1
                    local_reset_reason = (
                        "trigger-qualified reject (no previous edge sample)"
                    )
            elif edge_delta_since_last_line <= 0:
                demod_line_accepted = False
                self.demod_reject_missing_count += 1
                local_reset_reason = "trigger-qualified reject (missing edge)"
            else:
                step_edges = int(edge_delta_since_last_line)
                if step_edges > 1:
                    self.demod_multi_edge_count += 1
                if self.demod_last_line_phase is None:
                    demod_line_phase = 0
                else:
                    demod_line_phase = int(
                        self.demod_last_line_phase ^ (step_edges & 1)
                    )

            if demod_line_accepted:
                self.demod_accept_count += 1
                self.demod_last_line_phase = int(
                    0 if demod_line_phase is None else demod_line_phase
                )
            else:
                self.demod_reject_count += 1
                parity_reset_needed = True
                self.latest_chop_pair = None
                self.latest_chop_integrated = None

        if parity_reset_needed:
            self._apply_parity_reset(local_reset_reason)

        if not demod_line_accepted:
            return

        if self.pump_chop_use_adjacent_pairs:
            if self.pump_chop_sign_agnostic_preview:
                if self.chop_prev_line is None:
                    self.chop_prev_line = demod_line.copy()
                    self.latest_chop_pair = None
                    self.latest_chop_integrated = None
                else:
                    if self.reference_processing_mode == "ratio" and self.ref_line is not None:
                        chop_pair = self._tail_guided_reference_delta_od(
                            demod_line,
                            self.chop_prev_line,
                        )
                    else:
                        chop_pair = self._tail_guided_pair(
                            demod_line - self.chop_prev_line
                        )
                    if (
                        self.chop_preview_template is not None
                        and np.any(np.isfinite(self.chop_preview_template))
                        and not self.pump_chop_tail_heuristic_enable
                    ):
                        template_dot = float(
                            np.dot(chop_pair, self.chop_preview_template)
                        )
                        if template_dot < 0.0:
                            chop_pair = -chop_pair
                            self.chop_preview_flip_counter += 1
                    self.chop_pair_counter += 1
                    self.latest_chop_pair = chop_pair
                    self._update_pairwise_dark_corrected_referenced(chop_pair)
                    if len(self.chop_pair_buffer) == self.chop_pair_buffer.maxlen:
                        self.chop_pair_sum -= self.chop_pair_buffer.popleft()
                    self.chop_pair_buffer.append(chop_pair.copy())
                    self.chop_pair_sum += chop_pair
                    self.latest_chop_integrated = self.chop_pair_sum / float(
                        len(self.chop_pair_buffer)
                    )
                    self.chop_preview_template = self.latest_chop_integrated.copy()
                    self.chop_prev_line = demod_line.copy()
            else:
                if demod_qual_active:
                    current_phase = int(
                        0 if demod_line_phase is None else demod_line_phase
                    )
                else:
                    current_phase = int(self.chop_phase ^ ((lines_consumed - 1) & 1))
                    self.chop_phase = int(self.chop_phase ^ (lines_consumed & 1))
                if self.chop_prev_line is None:
                    self.chop_prev_line = demod_line.copy()
                    self.chop_prev_phase = current_phase
                    self.latest_chop_pair = None
                    self.latest_chop_integrated = None
                else:
                    make_pair = (
                        (self.chop_prev_phase is None)
                        or (current_phase != self.chop_prev_phase)
                    )
                    if make_pair:
                        if self.reference_processing_mode == "ratio" and self.ref_line is not None:
                            chop_pair = self._tail_guided_reference_delta_od(
                                demod_line,
                                self.chop_prev_line,
                            )
                        else:
                            chop_pair = self._tail_guided_pair(
                                demod_line - self.chop_prev_line
                            )
                        self.chop_pair_counter += 1
                        self.latest_chop_pair = chop_pair
                        self._update_pairwise_dark_corrected_referenced(chop_pair)
                        if len(self.chop_pair_buffer) == self.chop_pair_buffer.maxlen:
                            self.chop_pair_sum -= self.chop_pair_buffer.popleft()
                        self.chop_pair_buffer.append(chop_pair.copy())
                        self.chop_pair_sum += chop_pair
                        self.latest_chop_integrated = self.chop_pair_sum / float(
                            len(self.chop_pair_buffer)
                        )
                    else:
                        self.latest_chop_pair = None
                        self.chop_prev_line = demod_line.copy()
                        self.chop_prev_phase = current_phase
            return

        if demod_qual_active:
            current_phase = int(0 if demod_line_phase is None else demod_line_phase)
        else:
            current_phase = int(self.chop_phase ^ ((lines_consumed - 1) & 1))
            self.chop_phase = int(self.chop_phase ^ (lines_consumed & 1))

        if current_phase == 0:
            if len(self.chop_phase0_buffer) == self.chop_phase0_buffer.maxlen:
                self.chop_phase0_sum -= self.chop_phase0_buffer.popleft()
            self.chop_phase0_buffer.append(self.latest_line.copy())
            self.chop_phase0_sum += self.latest_line
            if self.reference_processing_mode == "ratio" and self.ref_line is not None:
                if len(self.chop_ref_phase0_buffer) == self.chop_ref_phase0_buffer.maxlen:
                    self.chop_ref_phase0_sum -= self.chop_ref_phase0_buffer.popleft()
                self.chop_ref_phase0_buffer.append(self.ref_line.copy())
                self.chop_ref_phase0_sum += self.ref_line
        else:
            if len(self.chop_phase1_buffer) == self.chop_phase1_buffer.maxlen:
                self.chop_phase1_sum -= self.chop_phase1_buffer.popleft()
            self.chop_phase1_buffer.append(self.latest_line.copy())
            self.chop_phase1_sum += self.latest_line
            if self.reference_processing_mode == "ratio" and self.ref_line is not None:
                if len(self.chop_ref_phase1_buffer) == self.chop_ref_phase1_buffer.maxlen:
                    self.chop_ref_phase1_sum -= self.chop_ref_phase1_buffer.popleft()
                self.chop_ref_phase1_buffer.append(self.ref_line.copy())
                self.chop_ref_phase1_sum += self.ref_line

        if self.chop_prev_line is None:
            self.chop_prev_line = demod_line.copy()
            self.chop_prev_phase = current_phase
            self.latest_chop_pair = None
        else:
            make_pair = (
                (self.chop_prev_phase is None)
                or (current_phase != self.chop_prev_phase)
            )
            if make_pair:
                if (
                    self.reference_processing_mode == "ratio"
                    and self.ref_line is not None
                    and self.pump_chop_tail_heuristic_enable
                ):
                    chop_pair = self._tail_guided_reference_delta_od(
                        demod_line,
                        self.chop_prev_line,
                    )
                elif self.reference_processing_mode == "ratio" and self.ref_line is not None:
                    pumped_phase = 1 if self.pump_chop_sign > 0 else 0
                    if current_phase == pumped_phase:
                        pumped_ratio = demod_line
                        unpumped_ratio = self.chop_prev_line
                    else:
                        pumped_ratio = self.chop_prev_line
                        unpumped_ratio = demod_line
                    chop_pair = self._compute_delta_od(
                        pumped_ratio,
                        unpumped_ratio,
                    )
                else:
                    chop_pair = self._tail_guided_pair(
                        demod_line - self.chop_prev_line
                    )
                self.chop_pair_counter += 1
                self.latest_chop_pair = chop_pair
                self._update_pairwise_dark_corrected_referenced(chop_pair)
                if self.pump_chop_tail_heuristic_enable:
                    if len(self.chop_pair_buffer) == self.chop_pair_buffer.maxlen:
                        self.chop_pair_sum -= self.chop_pair_buffer.popleft()
                    self.chop_pair_buffer.append(chop_pair.copy())
                    self.chop_pair_sum += chop_pair
            else:
                self.latest_chop_pair = None
            self.chop_prev_line = demod_line.copy()
            self.chop_prev_phase = current_phase

        referenced_phase_delta_od = self._referenced_phase_diagnostics()

        if self.pump_chop_tail_heuristic_enable:
            if len(self.chop_pair_buffer) > 0:
                self.latest_chop_integrated = self.chop_pair_sum / float(
                    len(self.chop_pair_buffer)
                )
            else:
                self.latest_chop_integrated = None
        elif referenced_phase_delta_od is not None:
            self.latest_chop_integrated = referenced_phase_delta_od
        elif len(self.chop_phase0_buffer) > 0 and len(self.chop_phase1_buffer) > 0:
            phase0_mean = self.chop_phase0_sum / float(len(self.chop_phase0_buffer))
            phase1_mean = self.chop_phase1_sum / float(len(self.chop_phase1_buffer))
            self.latest_chop_integrated = self.pump_chop_sign * (
                phase1_mean - phase0_mean
            )
        else:
            self.latest_chop_integrated = None

    def snapshot(self):
        return {
            "line_counter": int(self.line_counter),
            "line": self.latest_line.copy(),
            "integrated_line": self.integrated_line.copy(),
            "ref_line": None if self.ref_line is None else self.ref_line.copy(),
            "ref_integrated_line": (
                None
                if self.ref_integrated_line is None
                else self.ref_integrated_line.copy()
            ),
            "diff_line": None if self.diff_line is None else self.diff_line.copy(),
            "diff_integrated_line": (
                None
                if self.diff_integrated_line is None
                else self.diff_integrated_line.copy()
            ),
            "latest_chop_pair": (
                None if self.latest_chop_pair is None else self.latest_chop_pair.copy()
            ),
            "latest_chop_integrated": (
                None
                if self.latest_chop_integrated is None
                else self.latest_chop_integrated.copy()
            ),
            "latest_chop_integrated_pairwise_dark_corrected": (
                None
                if self.latest_chop_integrated_pairwise_dark_corrected is None
                else self.latest_chop_integrated_pairwise_dark_corrected.copy()
            ),
            "latest_chop_main_diagnostic": (
                None
                if self.latest_chop_main_diagnostic is None
                else self.latest_chop_main_diagnostic.copy()
            ),
            "latest_chop_ref_diagnostic": (
                None
                if self.latest_chop_ref_diagnostic is None
                else self.latest_chop_ref_diagnostic.copy()
            ),
            "integrated_auc_main": float(self.integrated_auc_main),
            "integrated_auc_ref": float(self.integrated_auc_ref),
            "integrated_auc_diff": float(self.integrated_auc_diff),
            "capture_hit_rate_pct": float(self.capture_hit_rate_pct),
            "capture_last_score_vpp": float(self.capture_last_score_vpp),
            "capture_last_threshold_vpp": float(self.capture_last_threshold_vpp),
            "capture_warmup_count": int(len(self.capture_score_recent)),
            "line_buffer_len": int(len(self.line_buffer)),
            "phase0_buffer_len": int(len(self.chop_phase0_buffer)),
            "phase1_buffer_len": int(len(self.chop_phase1_buffer)),
            "pair_buffer_len": int(len(self.chop_pair_buffer)),
            "chop_pair_counter": int(self.chop_pair_counter),
            "chop_preview_flip_counter": int(self.chop_preview_flip_counter),
            "tail_guided_flip_counter": int(self.tail_guided_flip_counter),
            "tail_guided_last_mean": float(self.tail_guided_last_mean),
            "chop_parity_reset_counter": int(self.chop_parity_reset_counter),
            "demod_accept_count": int(self.demod_accept_count),
            "demod_reject_count": int(self.demod_reject_count),
            "demod_reject_missing_count": int(self.demod_reject_missing_count),
            "demod_multi_edge_count": int(self.demod_multi_edge_count),
            "demod_reject_no_prev_count": int(self.demod_reject_no_prev_count),
            "last_warning_message": self.last_warning_message,
        }


class _BackgroundAccumulatorProcessor:
    """Read ordered lines continuously and publish reduced live snapshots."""

    def __init__(
        self,
        read_fn,
        accumulator,
        read_reference=False,
        snapshot_interval_s=0.02,
    ):
        self.read_fn = read_fn
        self.accumulator = accumulator
        self.read_reference = bool(read_reference)
        self.snapshot_interval_s = max(0.005, float(snapshot_interval_s))
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._latest = None
        self._error = None
        self._snapshot_seq = 0
        self._line_rate_buffer = deque(maxlen=32)
        self._wall_start_t = None
        self._wall_total_lines = 0
        self._next_snapshot_t = 0.0

    def start(self):
        if self._thread is not None:
            return self
        self._thread = threading.Thread(
            target=self._run,
            name="PDA_BackgroundAccumulator",
            daemon=True,
        )
        self._thread.start()
        return self

    def _publish_snapshot(self, now):
        service_rate_hz = (
            float(np.median(self._line_rate_buffer)) if self._line_rate_buffer else 0.0
        )
        wall_elapsed_s = (
            max(1e-6, now - self._wall_start_t)
            if self._wall_start_t is not None
            else 1e-6
        )
        wall_line_rate_hz = self._wall_total_lines / wall_elapsed_s
        snap = self.accumulator.snapshot()
        snap["service_rate_hz"] = float(service_rate_hz)
        snap["wall_line_rate_hz"] = float(wall_line_rate_hz)
        self._snapshot_seq += 1
        snap["snapshot_seq"] = int(self._snapshot_seq)
        with self._lock:
            self._latest = snap

    def _run(self):
        while not self._stop.is_set():
            t0 = time.perf_counter()
            try:
                data, lines_consumed = self.read_fn()
            except Exception as exc:
                with self._lock:
                    self._error = exc
                return
            elapsed_s = max(1e-9, time.perf_counter() - t0)
            data_items = data if isinstance(data, list) else [data]
            consumed = max(1, len(data_items))
            for item in data_items:
                external_phase_state = None
                if self.read_reference:
                    if not (
                        isinstance(item, dict)
                        and "main" in item
                        and "reference" in item
                    ):
                        with self._lock:
                            self._error = RuntimeError(
                                "Expected {'main','reference'} data in background processor."
                            )
                        return
                    line = np.asarray(item["main"], dtype=float)
                    ref_line = np.asarray(item["reference"], dtype=float)
                    external_phase_state = item.get("_phase_state")
                else:
                    if isinstance(item, dict) and "main" in item:
                        line = np.asarray(item["main"], dtype=float)
                        ref_line = (
                            None
                            if item.get("reference") is None
                            else np.asarray(item["reference"], dtype=float)
                        )
                        external_phase_state = item.get("_phase_state")
                    else:
                        line = np.asarray(item, dtype=float)
                        ref_line = None
                self.accumulator.process_line(
                    line,
                    ref_line=ref_line,
                    lines_consumed=1,
                    demod_qual_active=False,
                    edge_delta_since_last_line=None,
                    external_phase_state=external_phase_state,
                )
            self._line_rate_buffer.append(consumed / elapsed_s)
            now = time.perf_counter()
            if self._wall_start_t is None:
                self._wall_start_t = now
            self._wall_total_lines += consumed
            if (self._latest is None) or (now >= self._next_snapshot_t):
                self._publish_snapshot(now)
                self._next_snapshot_t = now + self.snapshot_interval_s

    def snapshot(self):
        with self._lock:
            latest = None if self._latest is None else dict(self._latest)
            err = self._error
        return latest, err

    def close(self, join_timeout_s=1.0):
        self._stop.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=max(0.0, float(join_timeout_s)))
        self._thread = None


def _is_daq_buffer_overwrite_error(exc):
    """Return True if an exception corresponds to NI-DAQmx buffer overwrite (-200222)."""
    code = getattr(exc, "error_code", None)
    if code == -200222:
        return True
    text = str(exc).lower()
    return ("-200222" in text) or ("input buffer overwrite" in text)


def _is_daq_timeout_error(exc):
    """Return True if an exception corresponds to an NI-DAQmx read timeout (-200284)."""
    code = getattr(exc, "error_code", None)
    if code == -200284:
        return True
    text = str(exc).lower()
    return ("-200284" in text) or (
        "some or all of the samples requested have not yet been acquired" in text
    )


def _metric_score(auc_median, auc_std, trigger_eff, valid):
    """
    Score function for sweep ranking.

    Higher is better:
    - rewards higher median signal area,
    - penalizes variation,
    - rewards trigger efficiency.
    """
    if not valid:
        return float("-inf")
    med = float(auc_median) if np.isfinite(auc_median) else float("-inf")
    std = float(auc_std) if np.isfinite(auc_std) else 0.0
    eff = float(trigger_eff) if np.isfinite(trigger_eff) else 0.0
    return med - (0.25 * std) + (100.0 * eff)


def evaluate_persistent_candidate(
    pda,
    expected_trigger_hz=1000.0,
    evaluation_seconds=6.0,
    acquisition_timeout_s=10.0,
    monitor_pfi9=True,
    pfi9_monitor_counter="ctr2",
    pfi9_rate_gate_s=0.05,
    ai_buffer_lines=2048,
    queue_abort_fraction=0.80,
):
    """
    Evaluate one persistent-retrigger timing candidate without plotting.

    Returns a metrics dict with validity flag and score.
    """
    if not pda.use_external_trigger:
        raise ValueError(
            "Persistent candidate evaluation requires external trigger enabled."
        )

    eval_s = max(0.5, float(evaluation_seconds))
    ai_buffer_lines = max(256, int(ai_buffer_lines))
    queue_abort_fraction = min(0.98, max(0.05, float(queue_abort_fraction)))

    auc_values = []
    line_rate_hz_buffer = deque(maxlen=32)
    pending_lines_buffer = deque(maxlen=128)
    pfi9_rate_hz_buffer = deque(maxlen=128)
    line_count = 0
    lines_consumed_total = 0
    max_pending_lines = 0.0
    invalid_reason = None

    t_wall_start = time.perf_counter()
    monitor_context = (
        _PFI9EdgeMonitorSession(
            pda,
            counter=pfi9_monitor_counter,
            rate_gate_s=pfi9_rate_gate_s,
        )
        if monitor_pfi9
        else nullcontext(None)
    )

    with _RetriggerLineSession(
        pda,
        ai_buffer_lines=ai_buffer_lines,
        read_reference=False,
        tdms_log_enable=False,
        latest_only_read=False,
        overwrite_unread=False,
    ) as session, monitor_context as pfi9_monitor:
        while (time.perf_counter() - t_wall_start) < eval_s:
            pending_lines = float("nan")
            try:
                avail_samples = float(session.ai_task.in_stream.avail_samp_per_chan)
                pending_lines = avail_samples / float(max(1, pda.ai_samples_per_line))
                pending_lines_buffer.append(pending_lines)
                max_pending_lines = max(max_pending_lines, pending_lines)
            except Exception:
                pass

            if (
                np.isfinite(pending_lines)
                and pending_lines > (queue_abort_fraction * ai_buffer_lines)
            ):
                invalid_reason = (
                    "queue_guard_abort:"
                    f" pending={pending_lines:.1f} lines, "
                    f"limit={queue_abort_fraction * ai_buffer_lines:.1f}"
                )
                break

            t0 = time.perf_counter()
            try:
                data = session.read_line(timeout=float(acquisition_timeout_s))
            except Exception as exc:
                if _is_daq_buffer_overwrite_error(exc):
                    invalid_reason = "buffer_overwrite_-200222"
                    break
                invalid_reason = f"read_error:{exc}"
                break
            t_read = max(1e-9, time.perf_counter() - t0)

            lines_consumed = max(1, int(getattr(session, "last_lines_consumed", 1)))
            lines_consumed_total += lines_consumed
            line_count += 1
            line_rate_hz_buffer.append(lines_consumed / t_read)

            arr = np.asarray(data, dtype=float)
            auc_values.append(float(np.trapz(arr, dx=1.0)))

            if pfi9_monitor is not None and getattr(pfi9_monitor, "available", False):
                pfi9_rate_hz, _ = pfi9_monitor.read_rate()
                if np.isfinite(pfi9_rate_hz):
                    pfi9_rate_hz_buffer.append(float(pfi9_rate_hz))

    wall_elapsed_s = max(1e-9, time.perf_counter() - t_wall_start)
    wall_line_rate_hz = lines_consumed_total / wall_elapsed_s
    read_service_rate_hz = (
        float(np.median(line_rate_hz_buffer))
        if line_rate_hz_buffer
        else float("nan")
    )
    pfi9_med_hz = (
        float(np.median(pfi9_rate_hz_buffer))
        if pfi9_rate_hz_buffer
        else float("nan")
    )
    auc_arr = np.asarray(auc_values, dtype=float)
    auc_median = float(np.median(auc_arr)) if auc_arr.size else float("nan")
    auc_mean = float(np.mean(auc_arr)) if auc_arr.size else float("nan")
    auc_std = float(np.std(auc_arr)) if auc_arr.size else float("nan")
    auc_p10 = float(np.percentile(auc_arr, 10)) if auc_arr.size else float("nan")
    auc_p90 = float(np.percentile(auc_arr, 90)) if auc_arr.size else float("nan")
    queue_med = (
        float(np.median(pending_lines_buffer))
        if pending_lines_buffer
        else float("nan")
    )

    if expected_trigger_hz > 0:
        denom = (
            pfi9_med_hz
            if np.isfinite(pfi9_med_hz) and pfi9_med_hz > 0
            else float(expected_trigger_hz)
        )
        trigger_eff = min(1.0, wall_line_rate_hz / max(1e-9, denom))
    else:
        trigger_eff = float("nan")

    valid = (
        invalid_reason is None
        and auc_arr.size >= 8
        and np.isfinite(auc_median)
        and np.isfinite(wall_line_rate_hz)
    )

    score = _metric_score(
        auc_median=auc_median,
        auc_std=auc_std,
        trigger_eff=trigger_eff,
        valid=valid,
    )

    return {
        "valid": bool(valid),
        "invalid_reason": invalid_reason,
        "score": float(score),
        "line_count": int(line_count),
        "lines_consumed_total": int(lines_consumed_total),
        "wall_elapsed_s": float(wall_elapsed_s),
        "wall_line_rate_hz": float(wall_line_rate_hz),
        "read_service_rate_hz": float(read_service_rate_hz),
        "pfi9_rate_median_hz": float(pfi9_med_hz),
        "trigger_eff": float(trigger_eff),
        "auc_median": float(auc_median),
        "auc_mean": float(auc_mean),
        "auc_std": float(auc_std),
        "auc_p10": float(auc_p10),
        "auc_p90": float(auc_p90),
        "queue_median_lines": float(queue_med),
        "queue_max_lines": float(max_pending_lines),
        "ai_buffer_lines": int(ai_buffer_lines),
        "queue_abort_fraction": float(queue_abort_fraction),
    }


def run_persistent_timing_sweep(
    pda,
    expected_trigger_hz=1000.0,
    evaluation_seconds=6.0,
    acquisition_timeout_s=10.0,
    monitor_pfi9=True,
    pfi9_monitor_counter="ctr2",
    pfi9_rate_gate_s=0.05,
    phase_coarse_start_us=0.0,
    phase_coarse_stop_us=1000.0,
    phase_coarse_step_us=25.0,
    phase_fine_half_width_us=50.0,
    phase_fine_step_us=5.0,
    st_delay_half_width_us=100.0,
    st_delay_step_us=10.0,
    ai_buffer_lines=2048,
    queue_abort_fraction=0.80,
):
    """
    Automated persistent-mode timing sweep:
    1) Baseline diagnostic at current settings
    2) Trigger->ST phase coarse sweep + fine sweep
    3) ST initial delay fine sweep
    """
    if not pda.use_external_trigger:
        raise ValueError("Sweep mode requires external trigger enabled.")

    phase_coarse_step_us = max(1e-3, float(phase_coarse_step_us))
    phase_fine_step_us = max(1e-3, float(phase_fine_step_us))
    st_delay_step_us = max(1e-3, float(st_delay_step_us))
    rows = []

    orig = {
        "st_high": float(pda.st_high_time),
        "st_low": float(pda.st_low_time),
        "base_st_delay": float(pda.base_st_initial_delay),
        "trigger_shift": float(pda.trigger_phase_shift_s),
    }

    def _evaluate_and_record(stage, phase_us, st_delay_us):
        m = evaluate_persistent_candidate(
            pda=pda,
            expected_trigger_hz=expected_trigger_hz,
            evaluation_seconds=evaluation_seconds,
            acquisition_timeout_s=acquisition_timeout_s,
            monitor_pfi9=monitor_pfi9,
            pfi9_monitor_counter=pfi9_monitor_counter,
            pfi9_rate_gate_s=pfi9_rate_gate_s,
            ai_buffer_lines=ai_buffer_lines,
            queue_abort_fraction=queue_abort_fraction,
        )
        row = {
            "stage": str(stage),
            "phase_shift_us": float(phase_us),
            "st_initial_delay_us": float(st_delay_us),
            **m,
        }
        rows.append(row)
        validity = "OK" if m["valid"] else f"INVALID ({m['invalid_reason']})"
        print(
            f"[{stage}] phase={phase_us:.1f} us, st_delay={st_delay_us:.1f} us "
            f"| AUCmed={m['auc_median']:.3f} | eff={100.0 * m['trigger_eff']:.1f}% "
            f"| wall={m['wall_line_rate_hz']:.1f} Hz | score={m['score']:.3f} "
            f"| {validity}"
        )
        return row

    try:
        print("Running persistent-mode baseline diagnostic...")
        baseline = _evaluate_and_record(
            stage="baseline",
            phase_us=(pda.trigger_phase_shift_s * 1e6),
            st_delay_us=(pda.base_st_initial_delay * 1e6),
        )
        if not baseline["valid"]:
            print(
                "Baseline is invalid before sweep; likely software/throughput path issue. "
                "Sweep will continue, but results may be limited."
            )
        elif baseline["trigger_eff"] < 0.70:
            print(
                "Baseline trigger efficiency is low; software path may still be throughput-limited."
            )
        else:
            print(
                "Baseline path looks software-stable; proceeding to timing sweep."
            )

        coarse_values = np.arange(
            float(phase_coarse_start_us),
            float(phase_coarse_stop_us) + 0.5 * phase_coarse_step_us,
            phase_coarse_step_us,
        )
        print(
            "Coarse phase sweep: "
            f"{coarse_values[0]:.1f} -> {coarse_values[-1]:.1f} us "
            f"(step {phase_coarse_step_us:.1f} us)"
        )
        for phase_us in coarse_values:
            pda.set_trigger_phase_shift(float(phase_us) * 1e-6)
            _evaluate_and_record(
                stage="phase_coarse",
                phase_us=float(phase_us),
                st_delay_us=(pda.base_st_initial_delay * 1e6),
            )

        valid_phase_rows = [
            r for r in rows if r["stage"] in ("phase_coarse",) and r["valid"]
        ]
        if not valid_phase_rows:
            raise RuntimeError(
                "No valid points in phase coarse sweep. "
                "Check trigger path/queue limits and rerun."
            )
        best_phase_coarse = max(valid_phase_rows, key=lambda r: r["score"])

        phase_center_us = float(best_phase_coarse["phase_shift_us"])
        phase_fine_values = np.arange(
            phase_center_us - float(phase_fine_half_width_us),
            phase_center_us + float(phase_fine_half_width_us) + 0.5 * phase_fine_step_us,
            phase_fine_step_us,
        )
        # Counter initial delay cannot be negative on NI-DAQmx.
        phase_fine_values = np.unique(np.clip(phase_fine_values, 0.0, None))
        print(
            "Fine phase sweep around best coarse: "
            f"{phase_fine_values[0]:.1f} -> {phase_fine_values[-1]:.1f} us "
            f"(step {phase_fine_step_us:.1f} us)"
        )
        for phase_us in phase_fine_values:
            pda.set_trigger_phase_shift(float(phase_us) * 1e-6)
            _evaluate_and_record(
                stage="phase_fine",
                phase_us=float(phase_us),
                st_delay_us=(pda.base_st_initial_delay * 1e6),
            )

        valid_phase_all = [
            r for r in rows if r["stage"] in ("phase_coarse", "phase_fine") and r["valid"]
        ]
        best_phase = max(valid_phase_all, key=lambda r: r["score"])
        best_phase_us = float(best_phase["phase_shift_us"])
        pda.set_trigger_phase_shift(best_phase_us * 1e-6)
        print(f"Best phase shift so far: {best_phase_us:.1f} us")

        st_center_us = float(pda.base_st_initial_delay * 1e6)
        st_values = np.arange(
            st_center_us - float(st_delay_half_width_us),
            st_center_us + float(st_delay_half_width_us) + 0.5 * st_delay_step_us,
            st_delay_step_us,
        )
        st_values = np.clip(st_values, 0.0, None)
        print(
            "ST initial-delay sweep: "
            f"{st_values[0]:.1f} -> {st_values[-1]:.1f} us "
            f"(step {st_delay_step_us:.1f} us)"
        )
        for st_us in st_values:
            pda.set_st_timing(
                high_time_s=pda.st_high_time,
                low_time_s=pda.st_low_time,
                initial_delay_s=float(st_us) * 1e-6,
            )
            _evaluate_and_record(
                stage="st_delay_fine",
                phase_us=best_phase_us,
                st_delay_us=float(st_us),
            )

        valid_final = [
            r for r in rows if r["stage"] in ("phase_coarse", "phase_fine", "st_delay_fine") and r["valid"]
        ]
        if not valid_final:
            raise RuntimeError("Sweep produced no valid final candidates.")
        best = max(valid_final, key=lambda r: r["score"])

        best_phase_us = float(best["phase_shift_us"])
        best_st_delay_us = float(best["st_initial_delay_us"])
        pda.set_st_timing(
            high_time_s=pda.st_high_time,
            low_time_s=pda.st_low_time,
            initial_delay_s=best_st_delay_us * 1e-6,
        )
        pda.set_trigger_phase_shift(best_phase_us * 1e-6)

        out_dir = Path(__file__).resolve().parent / "optimization_results"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        csv_path = out_dir / f"persistent_timing_sweep_{ts}.csv"
        json_path = out_dir / f"persistent_timing_sweep_{ts}.json"

        csv_fields = [
            "stage",
            "phase_shift_us",
            "st_initial_delay_us",
            "valid",
            "invalid_reason",
            "score",
            "line_count",
            "lines_consumed_total",
            "wall_elapsed_s",
            "wall_line_rate_hz",
            "read_service_rate_hz",
            "pfi9_rate_median_hz",
            "trigger_eff",
            "auc_median",
            "auc_mean",
            "auc_std",
            "auc_p10",
            "auc_p90",
            "queue_median_lines",
            "queue_max_lines",
            "ai_buffer_lines",
            "queue_abort_fraction",
        ]
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        summary = {
            "timestamp": ts,
            "best": best,
            "best_phase_shift_us": best_phase_us,
            "best_st_initial_delay_us": best_st_delay_us,
            "timing_profile_name": getattr(pda, "timing_profile_name", ""),
            "rows_count": len(rows),
            "csv_path": str(csv_path),
            "json_path": str(json_path),
            "settings": {
                "expected_trigger_hz": float(expected_trigger_hz),
                "evaluation_seconds": float(evaluation_seconds),
                "phase_coarse_start_us": float(phase_coarse_start_us),
                "phase_coarse_stop_us": float(phase_coarse_stop_us),
                "phase_coarse_step_us": float(phase_coarse_step_us),
                "phase_fine_half_width_us": float(phase_fine_half_width_us),
                "phase_fine_step_us": float(phase_fine_step_us),
                "st_delay_half_width_us": float(st_delay_half_width_us),
                "st_delay_step_us": float(st_delay_step_us),
                "ai_buffer_lines": int(ai_buffer_lines),
                "queue_abort_fraction": float(queue_abort_fraction),
            },
        }
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        print("Sweep complete.")
        print(
            "Best persistent candidate: "
            f"phase={best_phase_us:.1f} us, st_delay={best_st_delay_us:.1f} us "
            f"| AUCmed={best['auc_median']:.3f} | eff={100.0 * best['trigger_eff']:.1f}%"
        )
        print(f"Saved sweep CSV:  {csv_path}")
        print(f"Saved sweep JSON: {json_path}")
        return summary, rows
    except Exception:
        # Keep the most recent profile timings if sweep fails, but restore the
        # original trigger shift and base ST delay for safety.
        pda.set_st_timing(
            high_time_s=orig["st_high"],
            low_time_s=orig["st_low"],
            initial_delay_s=orig["base_st_delay"],
        )
        pda.set_trigger_phase_shift(orig["trigger_shift"])
        raise


def run_live_plot(
    pda,
    integration_line_count=128,
    plot_raw_line=True,
    live_video_mode="main",
    reference_processing_mode="difference",
    reference_ratio_floor=1e-6,
    channel_dark_subtract=False,
    channel_dark_file=None,
    pump_chop_demod=False,
    pump_chop_sign=-1.0,
    pump_chop_phase_source="inferred",
    pump_chop_use_adjacent_pairs=False,
    pump_chop_sign_agnostic_preview=False,
    pump_chop_tail_heuristic_enable=False,
    pump_chop_tail_heuristic_start=900,
    pump_chop_tail_heuristic_stop=1000,
    pump_chop_tail_heuristic_expected_sign=1.0,
    pump_chop_tail_heuristic_zero_baseline=True,
    pump_chop_dark_subtract=True,
    pump_chop_dark_file=None,
    pump_chop_display_mode="native",
    pump_chop_mod_min_light_v=0.025,
    demod_trigger_qualified_acceptance=True,
    expected_trigger_hz=1000.0,
    acquisition_timeout_s=10.0,
    use_persistent_session=False,
    plot_update_every_n_lines=4,
    timing_text_update_every_n_lines=12,
    autoscale_every_n_plot_updates=3,
    monitor_pfi9=True,
    pfi9_monitor_counter="ctr2",
    pfi9_rate_gate_s=0.05,
    monitor_chopper_input=False,
    chopper_input_monitor_counter="ctr2",
    chopper_input_rate_gate_s=0.05,
    trigger_plot_history=240,
    plot_target_fps=15.0,
    retrigger_latest_only_read=True,
    retrigger_overwrite_unread=True,
    tdms_log_enable=False,
    tdms_file_path=None,
    tdms_group_name="PDA",
    tdms_logging_mode=LoggingMode.LOG_AND_READ,
    tdms_logging_operation=LoggingOperation.OPEN_OR_CREATE,
    decouple_acquisition_from_plot=True,
    persistent_ai_buffer_lines=256,
    reader_fifo_max_packets=256,
    ordered_read_batch_lines=16,
    capture_hit_rate_enable=True,
    capture_hit_rate_window_lines=256,
    capture_hit_threshold_fraction=0.45,
    capture_hit_warmup_lines=64,
):
    """
    Acquire and plot live lines from the CMOS/PDA chain.

    Supports:
    - safe per-line task build/start mode,
    - persistent retriggered session mode,
    - optional background-reader decoupling,
    - optional pump-chop demod diagnostics,
    - optional saved pump-dark baseline subtraction for integrated demod traces,
    - optional sign-agnostic adjacent-pair preview demod,
    - optional digital edge-rate monitoring for trigger or chopper input.
    """
    def _is_buffer_overwrite_error(exc):
        code = getattr(exc, "error_code", None)
        if code == -200222:
            return True
        text = str(exc).lower()
        return ("-200222" in text) or ("input buffer overwrite" in text)

    mode_key = str(live_video_mode).strip().lower()
    if mode_key not in ("main", "ref", "both"):
        raise ValueError("live_video_mode must be 'main', 'ref', or 'both'.")
    reference_processing_mode = str(reference_processing_mode).strip().lower()
    if reference_processing_mode not in ("difference", "ratio"):
        raise ValueError(
            "reference_processing_mode must be 'difference' or 'ratio'."
        )
    reference_ratio_floor = max(1e-12, float(reference_ratio_floor))
    channel_dark_subtract = bool(channel_dark_subtract)
    pump_chop_demod = bool(pump_chop_demod)
    pump_chop_sign = float(pump_chop_sign)
    pump_chop_phase_source = str(pump_chop_phase_source).strip().lower()
    if pump_chop_phase_source not in ("inferred", "chopper_input"):
        raise ValueError(
            "pump_chop_phase_source must be 'inferred' or 'chopper_input'."
        )
    pump_chop_use_adjacent_pairs = bool(pump_chop_use_adjacent_pairs)
    pump_chop_sign_agnostic_preview = bool(pump_chop_sign_agnostic_preview)
    pump_chop_tail_heuristic_enable = bool(pump_chop_tail_heuristic_enable)
    pump_chop_tail_heuristic_start = int(pump_chop_tail_heuristic_start)
    pump_chop_tail_heuristic_stop = int(pump_chop_tail_heuristic_stop)
    pump_chop_tail_heuristic_expected_sign = float(
        pump_chop_tail_heuristic_expected_sign
    )
    pump_chop_tail_heuristic_zero_baseline = bool(
        pump_chop_tail_heuristic_zero_baseline
    )
    tail_window_mapping_note = ""
    pump_chop_dark_subtract = bool(pump_chop_dark_subtract)
    pump_chop_display_mode = str(pump_chop_display_mode).strip().lower()
    if pump_chop_display_mode in ("mod", "milliod", "milli_od", "milli-od"):
        pump_chop_display_mode = "milli_od"
    elif pump_chop_display_mode in ("native", "raw"):
        pump_chop_display_mode = "native"
    else:
        raise ValueError(
            "pump_chop_display_mode must be 'native' or 'mod'/'milliod'."
        )
    pump_chop_mod_min_light_v = max(
        reference_ratio_floor,
        float(pump_chop_mod_min_light_v),
    )
    demod_trigger_qualified_acceptance = bool(demod_trigger_qualified_acceptance)
    retrigger_latest_only_read = bool(retrigger_latest_only_read)
    retrigger_overwrite_unread = bool(retrigger_overwrite_unread)
    tdms_log_enable = bool(tdms_log_enable)
    decouple_acquisition_from_plot = bool(decouple_acquisition_from_plot)
    persistent_ai_buffer_lines = max(64, int(persistent_ai_buffer_lines))
    reader_fifo_max_packets = max(8, int(reader_fifo_max_packets))
    ordered_read_batch_lines = max(1, int(ordered_read_batch_lines))
    plot_target_fps = max(1.0, float(plot_target_fps))
    gui_poll_timeout_s = max(
        0.02,
        min(float(acquisition_timeout_s), 0.25),
    )
    background_read_timeout_s = max(
        0.10,
        min(float(acquisition_timeout_s), 0.50),
    )
    capture_hit_rate_enable = bool(capture_hit_rate_enable)
    capture_hit_rate_window_lines = max(16, int(capture_hit_rate_window_lines))
    capture_hit_warmup_lines = max(8, int(capture_hit_warmup_lines))
    capture_hit_threshold_fraction = float(capture_hit_threshold_fraction)
    capture_hit_threshold_fraction = min(
        1.0, max(0.0, capture_hit_threshold_fraction)
    )
    if pump_chop_phase_source == "chopper_input":
        demod_mode_label = (
            "chopper-input adjacent pairs"
            if pump_chop_use_adjacent_pairs
            else "chopper-input buckets"
        )
    else:
        demod_mode_label = (
            "tail-guided pairs"
            if pump_chop_tail_heuristic_enable
            else (
                "adjacent preview"
                if (pump_chop_use_adjacent_pairs and pump_chop_sign_agnostic_preview)
                else (
                    "adjacent pairs"
                    if pump_chop_use_adjacent_pairs
                    else "phase1-phase0 buckets"
                )
            )
        )
    if pump_chop_sign == 0:
        raise ValueError("pump_chop_sign must be non-zero.")
    read_reference = mode_key == "both"
    ref_only = mode_key == "ref"
    if reference_processing_mode == "ratio" and not read_reference:
        raise ValueError(
            "reference_processing_mode='ratio' requires live_video_mode='both'."
        )
    pda._validate_scan_rate(num_ai_channels=(2 if read_reference else 1))

    def _counter_name(counter_value):
        return str(counter_value).replace("\\", "/").split("/")[-1].lower()

    requested_pfi9_monitor = bool(monitor_pfi9)
    requested_chopper_input_monitor = bool(monitor_chopper_input)
    pfi9_monitor_counter_name = _counter_name(pfi9_monitor_counter)
    chopper_input_monitor_counter_name = _counter_name(
        chopper_input_monitor_counter
    )
    ai_clock_counter_name = _counter_name(pda.ai_sample_clock_counter)
    chopper_sync_counter_name = _counter_name(pda.chopper_sync_counter)
    if requested_chopper_input_monitor and (
        pda.uses_separate_ai_sample_clock
        and chopper_input_monitor_counter_name == ai_clock_counter_name
    ):
        print(
            "Chopper-input monitor disabled: decimated acquisition uses "
            f"{pda.ai_sample_clock_counter} for the AI sample clock."
        )
        requested_chopper_input_monitor = False
    if requested_chopper_input_monitor and (
        pda.chopper_sync_enable
        and chopper_input_monitor_counter_name == chopper_sync_counter_name
    ):
        print(
            "Chopper-input monitor disabled: chopper sync output already uses "
            f"{pda.chopper_sync_counter}."
        )
        requested_chopper_input_monitor = False
    if (
        requested_pfi9_monitor
        and requested_chopper_input_monitor
        and pfi9_monitor_counter_name == chopper_input_monitor_counter_name
    ):
        print(
            "PFI9 monitor disabled: chopper-input monitor is using "
            f"{chopper_input_monitor_counter}."
        )
        requested_pfi9_monitor = False

    rate_plot_enable = bool(
        requested_pfi9_monitor or requested_chopper_input_monitor
    )
    rate_plot_is_pfi9 = bool(requested_pfi9_monitor)
    rate_plot_is_chopper_input = bool(requested_chopper_input_monitor)
    rate_plot_label = "PFI9 Edge Rate (Hz)"
    rate_plot_ylabel = "PFI9 Hz"
    rate_med_label = "PFI9 med"
    rate_title_prefix = "pfi9"
    if requested_pfi9_monitor and pda.uses_separate_ai_sample_clock:
        if pfi9_monitor_counter_name == ai_clock_counter_name:
            print(
                "PFI9 monitor disabled: decimated acquisition uses "
                f"{pda.ai_sample_clock_counter} for the AI sample clock. "
                "Chopper sync on ctr3/PFI3 is unaffected. "
                "The lower rate panel will show DAQ line rate instead."
            )
            requested_pfi9_monitor = False
            rate_plot_enable = True
            rate_plot_is_pfi9 = False
            rate_plot_is_chopper_input = False
            rate_plot_label = "DAQ Line Rate (Hz; PFI9 counter unavailable)"
            rate_plot_ylabel = "Line Hz"
            rate_med_label = "Line-rate med"
            rate_title_prefix = "line"
    if requested_chopper_input_monitor:
        rate_plot_enable = True
        rate_plot_is_pfi9 = False
        rate_plot_is_chopper_input = True
        rate_plot_label = "Chopper In Rate (Hz)"
        rate_plot_ylabel = "Chopper Hz"
        rate_med_label = "Chopper med"
        rate_title_prefix = "chop_in"
    if (
        pump_chop_tail_heuristic_enable
        and int(pda.ai_sample_clock_divisor) > 1
    ):
        divisor = float(pda.ai_sample_clock_divisor)
        original_start = int(pump_chop_tail_heuristic_start)
        original_stop = int(pump_chop_tail_heuristic_stop)
        mapped_start = int(np.floor(original_start / divisor))
        mapped_stop = int(np.ceil(original_stop / divisor))
        mapped_start = max(0, min(int(pda.output_samples_per_line) - 1, mapped_start))
        mapped_stop = max(
            mapped_start + 1,
            min(int(pda.output_samples_per_line), mapped_stop),
        )
        pump_chop_tail_heuristic_start = mapped_start
        pump_chop_tail_heuristic_stop = mapped_stop
        tail_window_mapping_note = (
            f"detector-equiv [{original_start}:{original_stop}) -> "
            f"acquired [{mapped_start}:{mapped_stop})"
        )
        print(
            "Tail-guided window remapped for decimated sampling: "
            f"{tail_window_mapping_note}."
        )
    derived_signal_label = (
        "Main/Ref" if reference_processing_mode == "ratio" else "Main-Ref"
    )
    derived_signal_units = (
        "arb." if reference_processing_mode == "ratio" else "V"
    )
    if pump_chop_display_mode == "milli_od":
        chop_signal_units = "mOD"
        chop_axis_label = "Delta OD (mOD)"
    elif read_reference and reference_processing_mode == "ratio":
        chop_signal_units = "OD"
        chop_axis_label = "Delta OD (OD)"
    else:
        chop_signal_units = "V"
        chop_axis_label = "Delta V (V)"

    x = pda.output_sample_axis()
    if channel_dark_file in (None, ""):
        channel_dark_path = (
            Path(__file__).resolve().parent
            / "acquisition_results"
            / "channel_dark_offset_latest.npz"
        )
    else:
        channel_dark_path = Path(channel_dark_file).expanduser()
    channel_dark_path.parent.mkdir(parents=True, exist_ok=True)
    channel_dark_main_offset = None
    channel_dark_ref_offset = None
    channel_dark_label = (
        "ChanDark"
        if (read_reference and reference_processing_mode == "ratio")
        else "IntensityDark"
    )
    channel_dark_status = (
        "disabled"
        if not channel_dark_subtract
        else f"ready ({channel_dark_path.name})"
    )

    def _set_channel_dark_status(text):
        nonlocal channel_dark_status
        channel_dark_status = str(text)

    def _save_channel_dark_offsets(main_trace, ref_trace=None):
        main_trace = np.asarray(main_trace, dtype=float)
        payload = {
            "main_integrated": main_trace,
            "sample_index": pda.output_sample_axis(main_trace.size),
            "reference_processing_mode": np.asarray([reference_processing_mode]),
            "dark_kind": np.asarray(
                [
                    "main_ref_channel_dark"
                    if ref_trace is not None
                    else "main_intensity_dark"
                ]
            ),
            "timestamp": np.asarray([time.strftime("%Y-%m-%d %H:%M:%S")]),
        }
        if ref_trace is not None:
            payload["reference_integrated"] = np.asarray(ref_trace, dtype=float)
        np.savez(channel_dark_path, **payload)

    def _load_channel_dark_offsets(print_message=True):
        nonlocal channel_dark_main_offset, channel_dark_ref_offset
        if not channel_dark_subtract:
            channel_dark_main_offset = None
            channel_dark_ref_offset = None
            _set_channel_dark_status("disabled")
            if print_message:
                print("Intensity/channel dark subtraction is disabled; not loading dark.")
            return False
        if not channel_dark_path.exists():
            channel_dark_main_offset = None
            channel_dark_ref_offset = None
            _set_channel_dark_status("missing")
            if print_message:
                print(
                    "Channel-dark load skipped: file not found at "
                    f"{channel_dark_path}"
                )
            return False
        with np.load(channel_dark_path, allow_pickle=False) as npz_file:
            need_reference = read_reference and reference_processing_mode == "ratio"
            if "main_integrated" not in npz_file:
                raise KeyError("main_integrated not present in dark file.")
            if need_reference and "reference_integrated" not in npz_file:
                raise KeyError(
                    "reference_integrated not present in dark file; "
                    "referenced detection needs a two-channel dark."
                )
            main_loaded = np.asarray(npz_file["main_integrated"], dtype=float)
            ref_loaded = (
                np.asarray(npz_file["reference_integrated"], dtype=float)
                if need_reference
                else None
            )
        ref_size_bad = ref_loaded is not None and ref_loaded.size != x.size
        if main_loaded.size != x.size or ref_size_bad:
            channel_dark_main_offset = None
            channel_dark_ref_offset = None
            ref_size = "none" if ref_loaded is None else str(ref_loaded.size)
            _set_channel_dark_status(
                f"size mismatch ({main_loaded.size}/{ref_size}!={x.size})"
            )
            if print_message:
                print(
                    "Intensity/channel dark load skipped: sample-count mismatch "
                    f"({main_loaded.size}/{ref_size} vs current {x.size})."
                )
            return False
        channel_dark_main_offset = main_loaded.copy()
        channel_dark_ref_offset = None if ref_loaded is None else ref_loaded.copy()
        _set_channel_dark_status(f"active ({channel_dark_path.name})")
        if print_message:
            if ref_loaded is None:
                print(
                    "Intensity dark loaded from "
                    f"{channel_dark_path} and will be subtracted from Vavg "
                    "only for main-only mOD display normalization."
                )
            else:
                print(
                    "Channel-dark offsets loaded from "
                    f"{channel_dark_path} and will be subtracted from main/ref "
                    "before referenced detection."
                )
        return True
    if pump_chop_dark_file in (None, ""):
        pump_chop_dark_path = (
            Path(__file__).resolve().parent
            / "acquisition_results"
            / "pump_chop_dark_offset_latest.npz"
        )
    else:
        pump_chop_dark_path = Path(pump_chop_dark_file).expanduser()
    pump_chop_dark_path.parent.mkdir(parents=True, exist_ok=True)
    pump_chop_dark_offset = None
    pump_chop_dark_main_diagnostic_offset = None
    pump_chop_dark_ref_diagnostic_offset = None
    pump_chop_dark_status = (
        "disabled"
        if not pump_chop_dark_subtract
        else f"ready ({pump_chop_dark_path.name})"
    )

    def _set_pump_dark_status(text):
        nonlocal pump_chop_dark_status
        pump_chop_dark_status = str(text)

    def _save_pump_dark_offset(trace, main_diagnostic_trace=None, ref_diagnostic_trace=None):
        trace = np.asarray(trace, dtype=float)
        payload = {
            "demod_integrated": trace,
            "sample_index": pda.output_sample_axis(trace.size),
            "pump_chop_sign": np.asarray([pump_chop_sign], dtype=float),
            "demod_mode": np.asarray([demod_mode_label]),
            "reference_processing_mode": np.asarray([reference_processing_mode]),
            "timestamp": np.asarray([time.strftime("%Y-%m-%d %H:%M:%S")]),
        }
        if main_diagnostic_trace is not None:
            payload["main_diagnostic_integrated"] = np.asarray(
                main_diagnostic_trace,
                dtype=float,
            )
        if ref_diagnostic_trace is not None:
            payload["ref_diagnostic_integrated"] = np.asarray(
                ref_diagnostic_trace,
                dtype=float,
            )
        np.savez(pump_chop_dark_path, **payload)

    def _load_pump_dark_offset(print_message=True):
        nonlocal pump_chop_dark_offset
        nonlocal pump_chop_dark_main_diagnostic_offset
        nonlocal pump_chop_dark_ref_diagnostic_offset
        if not pump_chop_dark_subtract:
            pump_chop_dark_offset = None
            pump_chop_dark_main_diagnostic_offset = None
            pump_chop_dark_ref_diagnostic_offset = None
            _set_pump_dark_status("disabled")
            if print_message:
                print("Pump-dark subtraction is disabled; not loading dark offset.")
            return False
        if not pump_chop_dark_path.exists():
            pump_chop_dark_offset = None
            pump_chop_dark_main_diagnostic_offset = None
            pump_chop_dark_ref_diagnostic_offset = None
            _set_pump_dark_status("missing")
            if print_message:
                print(
                    "Pump-dark load skipped: file not found at "
                    f"{pump_chop_dark_path}"
                )
            return False
        with np.load(pump_chop_dark_path, allow_pickle=False) as npz_file:
            if "demod_integrated" not in npz_file:
                raise KeyError("demod_integrated not present in pump-dark file.")
            loaded = np.asarray(npz_file["demod_integrated"], dtype=float)
            loaded_main_diag = (
                np.asarray(npz_file["main_diagnostic_integrated"], dtype=float)
                if "main_diagnostic_integrated" in npz_file
                else None
            )
            loaded_ref_diag = (
                np.asarray(npz_file["ref_diagnostic_integrated"], dtype=float)
                if "ref_diagnostic_integrated" in npz_file
                else None
            )
        if loaded.size != x.size:
            pump_chop_dark_offset = None
            pump_chop_dark_main_diagnostic_offset = None
            pump_chop_dark_ref_diagnostic_offset = None
            _set_pump_dark_status(f"size mismatch ({loaded.size}!={x.size})")
            if print_message:
                print(
                    "Pump-dark load skipped: sample-count mismatch "
                    f"({loaded.size} vs current {x.size})."
                )
            return False
        if loaded_main_diag is not None and loaded_main_diag.size != x.size:
            loaded_main_diag = None
        if loaded_ref_diag is not None and loaded_ref_diag.size != x.size:
            loaded_ref_diag = None
        pump_chop_dark_offset = loaded.copy()
        pump_chop_dark_main_diagnostic_offset = (
            None if loaded_main_diag is None else loaded_main_diag.copy()
        )
        pump_chop_dark_ref_diagnostic_offset = (
            None if loaded_ref_diag is None else loaded_ref_diag.copy()
        )
        _set_pump_dark_status(f"active ({pump_chop_dark_path.name})")
        if print_message:
            if (
                loaded_main_diag is not None
                or loaded_ref_diag is not None
            ):
                print(
                    "Referenced demod baseline loaded from "
                    f"{pump_chop_dark_path} and will be subtracted from the "
                    "integrated S/R, main-only, ref-only, and pairwise-corrected "
                    "comparison DeltaOD traces when available."
                )
            else:
                print(
                    "Pump-dark offset loaded from "
                    f"{pump_chop_dark_path} and will be subtracted from the "
                    "integrated pump-chop trace."
                )
        return True

    def _apply_pump_dark_offset(trace, role="demod"):
        if trace is None:
            return None
        if not pump_chop_dark_subtract:
            return np.asarray(trace, dtype=float)
        trace_arr = np.asarray(trace, dtype=float)
        if role == "main_diagnostic":
            offset = pump_chop_dark_main_diagnostic_offset
        elif role == "ref_diagnostic":
            offset = pump_chop_dark_ref_diagnostic_offset
        else:
            offset = pump_chop_dark_offset
        if offset is None or offset.size != trace_arr.size:
            return trace_arr
        return trace_arr - offset

    def _voltage_delta_to_milliod(delta_trace, baseline_trace):
        if delta_trace is None or baseline_trace is None:
            return None
        delta_arr = np.asarray(delta_trace, dtype=float)
        baseline_arr = np.asarray(baseline_trace, dtype=float)
        if delta_arr.size != baseline_arr.size:
            return np.full(delta_arr.shape, np.nan, dtype=float)

        pumped = baseline_arr + 0.5 * delta_arr
        unpumped = baseline_arr - 0.5 * delta_arr
        ratio = np.full(delta_arr.shape, np.nan, dtype=float)
        valid = (
            np.isfinite(pumped)
            & np.isfinite(unpumped)
            & (np.abs(baseline_arr) >= pump_chop_mod_min_light_v)
            & (np.abs(pumped) > reference_ratio_floor)
            & (np.abs(unpumped) > reference_ratio_floor)
            & ((pumped * unpumped) > 0.0)
        )
        np.divide(pumped, unpumped, out=ratio, where=valid)
        valid &= np.isfinite(ratio) & (ratio > 0.0)

        out = np.full(delta_arr.shape, np.nan, dtype=float)
        out[valid] = -1000.0 * np.log10(ratio[valid])
        return out

    def _prepare_chop_display(trace, baseline_trace=None, apply_dark=False, dark_role="demod"):
        if trace is None:
            return None
        trace_arr = (
            _apply_pump_dark_offset(trace, role=dark_role)
            if apply_dark
            else np.asarray(trace, dtype=float)
        )
        if pump_chop_display_mode != "milli_od":
            return trace_arr
        if read_reference and reference_processing_mode == "ratio":
            return 1000.0 * trace_arr
        baseline_arr = (
            None if baseline_trace is None else np.asarray(baseline_trace, dtype=float)
        )
        if (
            baseline_arr is not None
            and channel_dark_subtract
            and channel_dark_main_offset is not None
            and channel_dark_main_offset.size == baseline_arr.size
        ):
            baseline_arr = baseline_arr - channel_dark_main_offset
        return _voltage_delta_to_milliod(trace_arr, baseline_arr)

    def _trace_rms_pp(trace):
        if trace is None:
            return float("nan"), float("nan")
        arr = np.asarray(trace, dtype=float)
        finite = arr[np.isfinite(arr)]
        if finite.size <= 0:
            return float("nan"), float("nan")
        return float(np.std(finite)), float(np.ptp(finite))

    def _format_demod_title():
        dark_label = ""
        if pump_chop_dark_subtract:
            dark_label = (
                ", dark-subtracted"
                if pump_chop_dark_offset is not None
                else ", dark-ready"
            )
        if pump_chop_display_mode == "milli_od":
            leading = (
                "Referenced Pump-Chop Delta OD (mOD)"
                if (read_reference and reference_processing_mode == "ratio")
                else "Pump-Chop Delta OD Estimate (mOD)"
            )
        else:
            leading = (
                "Referenced Pump-Chop Delta OD"
                if (read_reference and reference_processing_mode == "ratio")
                else "Pump-Chop Difference"
            )
        return (
            f"{leading} ({demod_mode_label}, "
            f"sign {pump_chop_sign:+.0f}{dark_label}"
            + (
                ", baseline-subtracted"
                if (
                    read_reference
                    and reference_processing_mode == "ratio"
                    and pump_chop_dark_subtract
                    and pump_chop_dark_offset is not None
                )
                else ""
            )
            + ")"
        )

    tail_guided_flip_counter = 0
    tail_guided_last_mean = float("nan")

    def _tail_guided_pair(raw_pair):
        nonlocal tail_guided_flip_counter, tail_guided_last_mean
        pair = np.asarray(raw_pair, dtype=float).copy()
        if not pump_chop_tail_heuristic_enable:
            return pump_chop_sign * pair

        start = max(0, min(pair.size - 1, int(pump_chop_tail_heuristic_start)))
        stop = max(start + 1, min(pair.size, int(pump_chop_tail_heuristic_stop)))
        tail = pair[start:stop]
        if tail.size <= 0:
            return pump_chop_sign * pair

        tail_mean = float(np.mean(tail))
        if np.isfinite(tail_mean):
            if (
                pump_chop_tail_heuristic_expected_sign != 0.0
                and tail_mean * pump_chop_tail_heuristic_expected_sign < 0.0
            ):
                pair = -pair
                tail_mean = -tail_mean
                tail_guided_flip_counter += 1
            tail_guided_last_mean = tail_mean
            if pump_chop_tail_heuristic_zero_baseline:
                pair = pair - tail_mean
        return pump_chop_sign * pair

    def _tail_guided_reference_delta_od(current_ratio, previous_ratio):
        nonlocal tail_guided_flip_counter, tail_guided_last_mean
        current = np.asarray(current_ratio, dtype=float)
        previous = np.asarray(previous_ratio, dtype=float)
        ratio_pair = current - previous

        start = max(0, min(ratio_pair.size - 1, int(pump_chop_tail_heuristic_start)))
        stop = max(start + 1, min(ratio_pair.size, int(pump_chop_tail_heuristic_stop)))
        flip_pair = False

        if pump_chop_tail_heuristic_enable:
            tail = ratio_pair[start:stop]
            if tail.size > 0:
                tail_mean = float(np.mean(tail))
                if np.isfinite(tail_mean):
                    if (
                        pump_chop_tail_heuristic_expected_sign != 0.0
                        and tail_mean * pump_chop_tail_heuristic_expected_sign < 0.0
                    ):
                        flip_pair = True
                        tail_mean = -tail_mean
                        tail_guided_flip_counter += 1
                    tail_guided_last_mean = tail_mean

        delta_od = _compute_delta_od_local(current, previous)
        if flip_pair:
            delta_od = -delta_od
        delta_od = pump_chop_sign * delta_od

        if pump_chop_tail_heuristic_enable and pump_chop_tail_heuristic_zero_baseline:
            tail = delta_od[start:stop]
            if tail.size > 0:
                finite_tail = tail[np.isfinite(tail)]
                if finite_tail.size > 0:
                    delta_od = delta_od - float(np.mean(finite_tail))

        return delta_od

    def _referenced_phase_diagnostics_local():
        nonlocal latest_chop_main_diagnostic, latest_chop_ref_diagnostic
        latest_chop_main_diagnostic = None
        latest_chop_ref_diagnostic = None
        if not (
            reference_processing_mode == "ratio"
            and ref_line is not None
            and len(chop_phase0_buffer) > 0
            and len(chop_phase1_buffer) > 0
            and len(chop_ref_phase0_buffer) > 0
            and len(chop_ref_phase1_buffer) > 0
        ):
            return None

        phase0_main_mean = chop_phase0_sum / float(len(chop_phase0_buffer))
        phase1_main_mean = chop_phase1_sum / float(len(chop_phase1_buffer))
        phase0_ref_mean = chop_ref_phase0_sum / float(len(chop_ref_phase0_buffer))
        phase1_ref_mean = chop_ref_phase1_sum / float(len(chop_ref_phase1_buffer))

        if pump_chop_sign > 0:
            pumped_main = phase1_main_mean
            unpumped_main = phase0_main_mean
            pumped_ref = phase1_ref_mean
            unpumped_ref = phase0_ref_mean
        else:
            pumped_main = phase0_main_mean
            unpumped_main = phase1_main_mean
            pumped_ref = phase0_ref_mean
            unpumped_ref = phase1_ref_mean

        latest_chop_main_diagnostic = _compute_single_channel_delta_od_local(
            pumped_main,
            unpumped_main,
            channel_dark_main_offset,
        )
        latest_chop_ref_diagnostic = _compute_single_channel_delta_od_local(
            pumped_ref,
            unpumped_ref,
            channel_dark_ref_offset,
        )

        pumped_ratio = _safe_positive_ratio_local(
            _apply_channel_dark_local(pumped_main, channel_dark_main_offset),
            _apply_channel_dark_local(pumped_ref, channel_dark_ref_offset),
        )
        unpumped_ratio = _safe_positive_ratio_local(
            _apply_channel_dark_local(unpumped_main, channel_dark_main_offset),
            _apply_channel_dark_local(unpumped_ref, channel_dark_ref_offset),
        )
        return _compute_delta_od_local(pumped_ratio, unpumped_ratio)

    def _set_runtime_pump_chop_sign(new_sign):
        nonlocal pump_chop_sign
        nonlocal latest_chop_pair, latest_chop_integrated
        nonlocal latest_chop_main_diagnostic, latest_chop_ref_diagnostic
        nonlocal latest_chop_pair_display, latest_chop_integrated_display
        nonlocal chop_pair_buffer, chop_pair_sum, chop_preview_template
        nonlocal pump_chop_dark_offset
        nonlocal pump_chop_dark_main_diagnostic_offset
        nonlocal pump_chop_dark_ref_diagnostic_offset
        new_sign = float(new_sign)
        if new_sign == 0.0:
            raise ValueError("pump_chop_sign must be non-zero.")
        old_sign = float(pump_chop_sign)
        if new_sign == old_sign:
            return
        ratio = new_sign / old_sign
        pump_chop_sign = new_sign
        if latest_chop_pair is not None:
            latest_chop_pair = latest_chop_pair * ratio
        if latest_chop_integrated is not None:
            latest_chop_integrated = latest_chop_integrated * ratio
        if latest_chop_main_diagnostic is not None:
            latest_chop_main_diagnostic = latest_chop_main_diagnostic * ratio
        if latest_chop_ref_diagnostic is not None:
            latest_chop_ref_diagnostic = latest_chop_ref_diagnostic * ratio
        if processor is not None and getattr(processor, "accumulator", None) is not None:
            processor.accumulator.set_pump_chop_sign(new_sign)
            processor.accumulator.set_pump_chop_dark_offset(pump_chop_dark_offset)
        else:
            if pump_chop_use_adjacent_pairs or pump_chop_tail_heuristic_enable:
                chop_pair_buffer = deque(
                    (pair * ratio for pair in chop_pair_buffer),
                    maxlen=chop_pair_buffer.maxlen,
                )
                chop_pair_sum = chop_pair_sum * ratio
                if chop_preview_template is not None:
                    chop_preview_template = chop_preview_template * ratio
        if pump_chop_dark_offset is not None:
            pump_chop_dark_offset = pump_chop_dark_offset * ratio
        if pump_chop_dark_main_diagnostic_offset is not None:
            pump_chop_dark_main_diagnostic_offset = (
                pump_chop_dark_main_diagnostic_offset * ratio
            )
        if pump_chop_dark_ref_diagnostic_offset is not None:
            pump_chop_dark_ref_diagnostic_offset = (
                pump_chop_dark_ref_diagnostic_offset * ratio
            )
        latest_chop_pair_display = _prepare_chop_display(
            latest_chop_pair,
            integrated_line,
            apply_dark=False,
        )
        _reset_pairwise_dark_corrected_local()
        latest_chop_integrated_display = _prepare_chop_display(
            latest_chop_integrated,
            integrated_line,
            apply_dark=True,
        )
        if chop_raw_line_plot is not None and latest_chop_pair_display is not None:
            chop_raw_line_plot.set_ydata(latest_chop_pair_display)
        if (
            chop_integrated_line_plot is not None
            and latest_chop_integrated_display is not None
        ):
            chop_integrated_line_plot.set_ydata(latest_chop_integrated_display)
        if ax_demod is not None:
            ax_demod.set_title(_format_demod_title())
        fig.canvas.draw_idle()

    plt.ion()
    if pump_chop_demod and rate_plot_enable:
        fig, (ax, ax_demod, ax_trig) = plt.subplots(
            3,
            1,
            figsize=(10, 7.2),
            gridspec_kw={"height_ratios": [3.2, 2.0, 1.3]},
            sharex=False,
        )
    elif pump_chop_demod:
        fig, (ax, ax_demod) = plt.subplots(
            2,
            1,
            figsize=(10, 6.2),
            gridspec_kw={"height_ratios": [3.0, 2.0]},
            sharex=True,
        )
        ax_trig = None
    elif rate_plot_enable:
        fig, (ax, ax_trig) = plt.subplots(
            2,
            1,
            figsize=(10, 5.7),
            gridspec_kw={"height_ratios": [3.2, 1.3]},
            sharex=False,
        )
        ax_demod = None
    else:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax_demod = None
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
    derived_axis = None
    if read_reference:
        derived_axis = (
            ax.twinx() if reference_processing_mode == "ratio" else ax
        )
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
        diff_raw_line_plot, = derived_axis.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.0,
            alpha=0.40,
            color="tab:gray",
            label=f"{derived_signal_label} raw",
        )
        diff_integrated_line_plot, = derived_axis.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.9,
            color="black",
            label=f"{derived_signal_label} integrated ({integration_line_count} lines)",
        )
        if derived_axis is not ax:
            derived_axis.set_ylabel(f"{derived_signal_label} ({derived_signal_units})")
            derived_axis.set_xlim(float(x[0]), float(x[-1]) if x.size else 1.0)
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
    ax.set_xlabel("Detector-equivalent Sample Index")
    ax.set_ylabel("Voltage (V)")
    ax.set_xlim(float(x[0]), float(x[-1]) if x.size else 1.0)
    ax.grid(True, alpha=0.3)
    top_handles, top_labels = ax.get_legend_handles_labels()
    if derived_axis is not None and derived_axis is not ax:
        derived_handles, derived_labels = derived_axis.get_legend_handles_labels()
        top_handles += derived_handles
        top_labels += derived_labels
    ax.legend(top_handles, top_labels, loc="upper right")
    timing_text = ax.text(
        0.015,
        0.98,
        "Diagnostics: warming up...",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=7,
        linespacing=1.15,
        family="monospace",
        bbox={"facecolor": "white", "alpha": 0.78, "edgecolor": "none"},
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
    alert_text = ax.text(
        0.985,
        0.90,
        "",
        transform=ax.transAxes,
        va="top",
        ha="right",
        fontsize=8,
        family="monospace",
        color="#8a1f1f",
        bbox={"facecolor": "#fff3cd", "alpha": 0.88, "edgecolor": "none"},
    )
    if ax_demod is not None:
        chop_raw_line_plot, = ax_demod.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.0,
            alpha=0.45,
            color="tab:purple",
            label=(
                "Chop pair raw (tail-guided)"
                if pump_chop_tail_heuristic_enable
                else (
                    "Chop pair raw (sign-aligned preview)"
                    if (pump_chop_use_adjacent_pairs and pump_chop_sign_agnostic_preview)
                    else "Chop pair raw (adjacent)"
                )
            ),
        )
        chop_integrated_line_plot, = ax_demod.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.9,
            color="tab:purple",
            label=(
                f"Chop integrated ({integration_line_count} pairs, tail-guided)"
                if pump_chop_tail_heuristic_enable
                else (
                    f"Chop integrated ({integration_line_count} pairs, sign-aligned)"
                    if (pump_chop_use_adjacent_pairs and pump_chop_sign_agnostic_preview)
                    else f"Chop integrated ({integration_line_count} lines/phase)"
                )
            ),
        )
        if read_reference and reference_processing_mode == "ratio":
            chop_integrated_pairwise_dark_corrected_plot, = ax_demod.plot(
                x,
                np.zeros_like(x, dtype=float),
                linewidth=1.7,
                linestyle="-.",
                color="tab:red",
                label="Chop integrated (pairwise baseline-corrected)",
            )
            chop_main_diag_line_plot, = ax_demod.plot(
                x,
                np.zeros_like(x, dtype=float),
                linewidth=1.3,
                linestyle="--",
                color="tab:orange",
                label="Main-only demod integrated (S channel)",
            )
            chop_ref_diag_line_plot, = ax_demod.plot(
                x,
                np.zeros_like(x, dtype=float),
                linewidth=1.3,
                linestyle="--",
                color="tab:green",
                label="Ref-only demod integrated (R channel)",
            )
        else:
            chop_integrated_pairwise_dark_corrected_plot = None
            chop_main_diag_line_plot = None
            chop_ref_diag_line_plot = None
        ax_demod.axhline(
            0.0,
            color="black",
            linewidth=0.8,
            alpha=0.6,
        )
        if not plot_raw_line:
            chop_raw_line_plot.set_visible(False)
        ax_demod.set_title(_format_demod_title())
        ax_demod.set_ylabel(chop_axis_label)
        ax_demod.set_xlabel("Detector-equivalent Sample Index")
        ax_demod.set_xlim(float(x[0]), float(x[-1]) if x.size else 1.0)
        ax_demod.grid(True, alpha=0.3)
        ax_demod.legend(loc="upper right")
    else:
        chop_raw_line_plot = None
        chop_integrated_line_plot = None
        chop_integrated_pairwise_dark_corrected_plot = None
        chop_main_diag_line_plot = None
        chop_ref_diag_line_plot = None
    if ax_trig is not None:
        trigger_plot_history = max(32, int(trigger_plot_history))
        trig_rate_plot, = ax_trig.plot(
            np.arange(trigger_plot_history),
            np.zeros(trigger_plot_history, dtype=float),
            linewidth=1.2,
            label=rate_plot_label,
        )
        ax_trig.axhline(
            expected_trigger_hz,
            color="tab:red",
            linestyle="--",
            linewidth=1.0,
        )
        ax_trig.set_xlim(0, max(1, trigger_plot_history - 1))
        ax_trig.set_ylabel(rate_plot_ylabel)
        ax_trig.set_xlabel("Recent Updates")
        ax_trig.grid(True, alpha=0.3)
        ax_trig.legend(loc="upper right")
    else:
        trig_rate_plot = None
    fig.tight_layout()
    plt.show(block=False)

    stop_requested = False
    stop_reason = ""
    no_data_warn_threshold_s = 1.0
    last_data_t = time.perf_counter()
    no_data_warning_active = False
    no_data_warning_printed = False

    use_fast_session = bool(use_persistent_session and pda.use_external_trigger)
    fast_rebuild_delay_s = 2.0
    fast_rebuild_max_attempts = 3
    fast_rebuild_attempt_count = 0
    fast_rebuild_deadline_t = None
    fast_rebuild_exhausted_announced = False

    def _fast_line_mode_text():
        if decouple_acquisition_from_plot and not retrigger_latest_only_read:
            return "persistent retriggered session (background ordered processor)"
        if decouple_acquisition_from_plot:
            return "persistent retriggered session (latest-view reader)"
        return "persistent retriggered session"

    def _set_mode_badge(label, color):
        nonlocal mode_badge_label, mode_badge_color
        mode_badge_label = str(label)
        mode_badge_color = str(color)
        _restore_mode_badge()

    def _restore_mode_badge():
        mode_text.set_text(mode_badge_label)
        mode_text.set_bbox(
            {"facecolor": mode_badge_color, "alpha": 0.85, "edgecolor": "none"}
        )

    def _show_no_data_indicator(detail):
        alert_text.set_text("NO-DATA")
        timing_text.set_text(
            "No new lines acquired.\n"
            f"{detail}\n"
            "Check trigger/camera/DAQ cabling and external sync."
        )

    def _mark_data_activity():
        nonlocal last_data_t, no_data_warning_active, no_data_warning_printed
        last_data_t = time.perf_counter()
        if no_data_warning_active:
            _restore_mode_badge()
            alert_text.set_text("")
        no_data_warning_active = False
        no_data_warning_printed = False

    def _maybe_show_no_data_warning(detail):
        nonlocal no_data_warning_active, no_data_warning_printed
        if stop_requested:
            return
        if fast_rebuild_deadline_t is not None:
            return
        idle_s = time.perf_counter() - last_data_t
        if idle_s < no_data_warn_threshold_s:
            return
        if not no_data_warning_printed:
            print(
                "Warning: no new lines acquired for "
                f"{idle_s:.1f} s. {detail}"
            )
            no_data_warning_printed = True
        _show_no_data_indicator(detail)
        no_data_warning_active = True

    def _request_stop(reason):
        nonlocal stop_requested, stop_reason
        stop_requested = True
        stop_reason = str(reason)

    if use_fast_session:
        line_mode = _fast_line_mode_text()
    else:
        line_mode = "per-line task build/start"
    mode_badge_label = "FAST"
    mode_badge_color = "#d9f2d9"
    if not use_fast_session:
        mode_badge_label = "SAFE"
        mode_badge_color = "#f6f2d9"
    _set_mode_badge(mode_badge_label, mode_badge_color)

    print("Starting continuous acquisition. Press Ctrl+C to stop.")
    print(f"External trigger on {pda.trig_in}: {pda.use_external_trigger}")
    print(f"Live video mode: {mode_key}")
    print(
        "Pump chop demod: "
        f"{pump_chop_demod} "
        + (
            (
                " (chopper-input grouping: per-line phase is read from "
                f"{pda.chopper_input_term}; software odd/even inference is bypassed)"
            )
            if pump_chop_phase_source == "chopper_input"
            else
            (
                " (tail-guided pair mode: raw pair=line[n]-line[n-1], "
                f"tail mean over [{pump_chop_tail_heuristic_start}:"
                f"{pump_chop_tail_heuristic_stop}) is forced toward "
                f"{pump_chop_tail_heuristic_expected_sign:+.0f}, "
                f"tail baseline {'subtracted' if pump_chop_tail_heuristic_zero_baseline else 'kept'}, "
                "then display sign applied)"
                + (
                    f" | {tail_window_mapping_note}"
                    if tail_window_mapping_note
                    else ""
                )
            )
            if pump_chop_tail_heuristic_enable
            else
            (
                " (adjacent preview: consecutive accepted-line pairs, "
                "sign-aligned to running template; absolute sign arbitrary)"
            )
            if (pump_chop_use_adjacent_pairs and pump_chop_sign_agnostic_preview)
            else (
                f"(adjacent mode: {pump_chop_sign:+.0f} * (line[n]-line[n-1]))"
                if pump_chop_use_adjacent_pairs
                else (
                    f"(phase-bucket mode: {pump_chop_sign:+.0f} * "
                    "(mean(phase1 accepted lines)-mean(phase0 accepted lines)))"
                )
            )
        )
    )
    print(
        "Demod trigger-qualified acceptance: "
        f"{demod_trigger_qualified_acceptance}"
    )
    if pump_chop_demod:
        print(
            "Pump-dark baseline: "
            f"{pump_chop_dark_status} | hotkeys: d=save/apply, l=load, "
            "x=clear, s=flip sign, +=set +1, -=set -1"
        )
        print(f"Pump-chop display units: {chop_signal_units}")
        if read_reference and reference_processing_mode == "ratio":
            print(
                "Referenced demod diagnostics: demod panel overlays final S/R "
                "DeltaOD plus dashed integrated main-only (S) and ref-only (R) "
                "demod DeltaOD traces."
            )
        if channel_dark_subtract:
            print(
                f"{channel_dark_label}: {channel_dark_status} | "
                "hotkeys: c=save, v=load, n=clear"
            )
        if pump_chop_display_mode == "milli_od" and not (
            read_reference and reference_processing_mode == "ratio"
        ):
            print(
                "Main-only mOD normalization: uses Vavg minus active "
                f"intensity dark; masks |Vlight| < {pump_chop_mod_min_light_v:.4g} V."
            )
    if demod_trigger_qualified_acceptance and use_fast_session:
        print(
            "Note: trigger-qualified acceptance is only exact in safe mode. "
            "In persistent mode, per-line edge qualification is ambiguous."
        )
    if pda.use_external_trigger:
        print(
            "Trigger filter: "
            f"enabled={pda.trigger_filter_enable}, "
            f"min_pulse={pda.trigger_filter_min_pulse_width_s * 1e6:.2f} us"
        )
    print(f"Acquisition mode: {line_mode}")
    print(
        "Acq/plot decoupling: "
        f"{(use_fast_session and decouple_acquisition_from_plot)}"
        + (
            " (background ordered processor)"
            if (
                use_fast_session
                and decouple_acquisition_from_plot
                and not retrigger_latest_only_read
            )
            else (
                f" (FIFO, max {reader_fifo_max_packets} packets)"
                if (use_fast_session and decouple_acquisition_from_plot)
                else ""
            )
        )
    )
    print(
        f"Live plot target FPS: {plot_target_fps:.1f} "
        f"(line-gate every {max(1, int(plot_update_every_n_lines))} lines)"
    )
    if pda.chopper_sync_enable:
        chopper_divisor = (
            0.5 * (pda.chopper_sync_high_ticks + pda.chopper_sync_low_ticks)
        )
        chopper_sync_hz = (
            expected_trigger_hz / float(chopper_divisor)
            if expected_trigger_hz > 0
            else float("nan")
        )
        print(
            "Chopper sync out: "
            f"enabled on {pda.chopper_sync_out_term} "
            f"from {pda.chopper_sync_source_terminal} "
            f"(ticks high/low={pda.chopper_sync_high_ticks}/"
            f"{pda.chopper_sync_low_ticks}, "
            f"expected~{chopper_sync_hz:.1f} Hz)"
        )
    if use_fast_session:
        print(
            "Retriggered read mode: "
            f"latest_only={retrigger_latest_only_read}, "
            f"overwrite_unread={retrigger_overwrite_unread}"
        )
        if tdms_log_enable and tdms_file_path:
            print(
                "TDMS logging: enabled "
                f"(file='{tdms_file_path}', group='{tdms_group_name}')"
            )
        else:
            print("TDMS logging: disabled")
    print(
        f"Timing: ST high-period={pda.st_high_time * 1e6:.1f} us, "
        f"ST low-period={pda.st_low_time * 1e6:.1f} us, "
        f"ST delay={pda.st_initial_delay * 1e6:.1f} us, "
        f"CLK high-period={pda.clk_high_time * 1e9:.1f} ns, "
        f"CLK low-period={pda.clk_low_time * 1e9:.1f} ns, "
        f"CLK delay={pda.clk_initial_delay * 1e6:.1f} us"
    )
    print(
        "AI sample clock: "
        f"rate={pda.ai_sample_rate / 1e6:.3f} MHz, "
        f"divisor={pda.ai_sample_clock_divisor}, "
        f"source={pda.ai_sample_clk_src}"
    )
    print(
        "Video timing: "
        f"AI edge={('ST falling' if pda.ai_start_trigger_edge == Edge.FALLING else 'ST rising')}, "
        f"dummy_clocks={pda.video_dummy_clocks}, "
        f"pixel_clocks={pda.video_pixel_clocks}, "
        f"output_samples={pda.video_output_samples}"
    )
    print(
        f"Samples/line read={pda.ai_samples_per_line}, "
        f"plotted={pda.output_samples_per_line}, "
        f"CLK pulses/line={pda.clk_pulses_per_line}"
    )
    print("Timing diagnostics:")
    print(pda.format_timing_diagnostics(trigger_frequency_hz=expected_trigger_hz))
    trigger_budget_margin_s = None
    trigger_budget_limited_s = None
    if pda.use_external_trigger and expected_trigger_hz > 0:
        trigger_period_s = 1.0 / float(expected_trigger_hz)
        capture_end_s = pda.get_timing_diagnostics(
            trigger_frequency_hz=expected_trigger_hz
        )["capture_end_s"]
        st_period_s = pda.st_period_s
        trigger_budget_limited_s = max(capture_end_s, st_period_s)
        trigger_budget_margin_s = trigger_period_s - trigger_budget_limited_s
        print(
            "Trigger budget check: "
            f"period={trigger_period_s * 1e6:.1f} us, "
            f"ST period={st_period_s * 1e6:.1f} us, "
            f"capture end={capture_end_s * 1e6:.1f} us, "
            f"limiting={trigger_budget_limited_s * 1e6:.1f} us, "
            f"margin={trigger_budget_margin_s * 1e6:.1f} us"
        )
        if st_period_s > trigger_period_s:
            print(
                "Warning: ST period exceeds trigger period. "
                "Retriggered operation can slip phase or miss intended on/off pairing."
            )
        if capture_end_s > trigger_period_s:
            print(
                "Warning: capture end exceeds trigger period. "
                "Backlog and parity inversions are likely in persistent mode."
            )

    line_buffer = deque(maxlen=max(1, int(integration_line_count)))
    integration_sum = np.zeros_like(x, dtype=float)
    ref_line_buffer = deque(maxlen=max(1, int(integration_line_count)))
    ref_integration_sum = np.zeros_like(x, dtype=float)
    diff_line_buffer = deque(maxlen=max(1, int(integration_line_count)))
    diff_integration_sum = np.zeros_like(x, dtype=float)
    chop_prev_line = None
    chop_phase = 0
    chop_phase0_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_phase1_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_phase0_sum = np.zeros_like(x, dtype=float)
    chop_phase1_sum = np.zeros_like(x, dtype=float)
    chop_ref_phase0_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_ref_phase1_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_ref_phase0_sum = np.zeros_like(x, dtype=float)
    chop_ref_phase1_sum = np.zeros_like(x, dtype=float)
    chop_pair_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_pair_sum = np.zeros_like(x, dtype=float)
    chop_pair_counter = 0
    chop_preview_flip_counter = 0
    chop_parity_reset_counter = 0
    latest_chop_pair = None
    latest_chop_integrated = None
    latest_chop_main_diagnostic = None
    latest_chop_ref_diagnostic = None
    chop_preview_template = None
    chop_prev_phase = None
    line_rate_hz_buffer = deque(maxlen=32)
    wall_start_t = None
    wall_total_lines = 0
    pfi9_rate_hz_buffer = deque(maxlen=max(32, int(trigger_plot_history)))
    pfi9_count_prev = None
    edge_delta_since_last_line = None
    demod_accept_count = 0
    demod_reject_count = 0
    demod_reject_missing_count = 0
    demod_multi_edge_count = 0
    demod_reject_no_prev_count = 0
    demod_last_line_phase = None
    demod_qual_requested = bool(
        demod_trigger_qualified_acceptance and pda.use_external_trigger
    )
    demod_qual_active = False
    pending_lines_buffer = deque(maxlen=128)
    line_counter = 0
    plot_update_counter = 0
    read_service_rate_hz = 0.0
    wall_line_rate_hz = 0.0
    trigger_eff = 1.0
    max_pending_lines = 0.0
    plot_update_every_n_lines = max(1, int(plot_update_every_n_lines))
    timing_text_update_every_n_lines = max(1, int(timing_text_update_every_n_lines))
    autoscale_every_n_plot_updates = max(1, int(autoscale_every_n_plot_updates))
    plot_update_interval_s = 1.0 / plot_target_fps
    ui_idle_pause_s = max(0.005, min(0.02, 0.5 * plot_update_interval_s))
    next_plot_update_t = 0.0
    queue_probe_warned = False
    fallback_close_warned = False
    pfi9_rate_warned = False
    latest_skip_warned = False
    chop_parity_warned = False
    parity_reset_requested = False
    parity_reset_reason = ""
    demod_qual_warned_persistent = False
    demod_qual_warned_no_monitor = False
    latest_multiline_read_events = 0
    latest_skipped_lines_total = 0
    capture_score_recent = deque(
        maxlen=max(capture_hit_rate_window_lines, capture_hit_warmup_lines, 128)
    )
    capture_hit_flags = deque(maxlen=capture_hit_rate_window_lines)
    capture_hit_rate_pct = float("nan")
    capture_last_score_vpp = float("nan")
    capture_last_threshold_vpp = float("nan")
    integrated_auc_main = float("nan")
    integrated_auc_ref = float("nan")
    integrated_auc_diff = float("nan")
    latest_chop_pair_display = None
    latest_chop_integrated_display = None
    latest_chop_integrated_pairwise_dark_corrected = None
    line = np.zeros_like(x, dtype=float)
    integrated_line = np.zeros_like(x, dtype=float)
    ref_line = None
    ref_integrated_line = None
    diff_line = None
    diff_integrated_line = None
    chop_pair_dark_corrected_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_pair_dark_corrected_sum = np.zeros_like(x, dtype=float)

    original_video_main = pda.video_main
    reader = None
    processor = None
    processor_snapshot_seq_last = 0
    processor_last_warning = None
    plot_rate_poll_interval_s = 0.01
    last_plot_rate_read_t = float("-inf")
    cached_plot_rate_hz = float("nan")
    cached_plot_rate_count = None
    derived_chopper_prev_state = None
    derived_chopper_last_rate_hz = float("nan")
    derived_chopper_gate_start_t = None
    derived_chopper_rising_edges = 0
    reader_queue_depth = float("nan")
    reader_queue_max_depth = 0.0
    if ref_only:
        # Repoint single-channel reads to the configured reference input.
        pda.video_main = pda.video_ref

    def _sync_channel_dark_to_runtime():
        if processor is not None and getattr(processor, "accumulator", None) is not None:
            processor.accumulator.set_channel_dark_offsets(
                channel_dark_main_offset,
                channel_dark_ref_offset,
            )

    def _sync_pump_dark_to_runtime():
        if processor is not None and getattr(processor, "accumulator", None) is not None:
            processor.accumulator.set_pump_chop_dark_offset(
                pump_chop_dark_offset,
            )

    def _reference_ratio_local(numerator, denominator):
        numerator = np.asarray(numerator, dtype=float)
        denominator = np.asarray(denominator, dtype=float)
        safe_den = denominator.copy()
        small_mask = np.abs(safe_den) < reference_ratio_floor
        if np.any(small_mask):
            safe_den[small_mask] = np.where(
                safe_den[small_mask] < 0.0,
                -reference_ratio_floor,
                reference_ratio_floor,
            )
        ratio = np.divide(
            numerator,
            safe_den,
            out=np.zeros_like(numerator),
            where=np.isfinite(safe_den),
        )
        ratio[~np.isfinite(ratio)] = 0.0
        return ratio

    def _safe_positive_ratio_local(numerator, denominator):
        ratio = _reference_ratio_local(numerator, denominator)
        return np.maximum(ratio, reference_ratio_floor)

    def _compute_delta_od_local(pumped_ratio, unpumped_ratio):
        pumped_safe = np.maximum(
            np.asarray(pumped_ratio, dtype=float), reference_ratio_floor
        )
        unpumped_safe = np.maximum(
            np.asarray(unpumped_ratio, dtype=float), reference_ratio_floor
        )
        return -np.log10(np.divide(pumped_safe, unpumped_safe))

    def _compute_single_channel_delta_od_local(pumped_signal, unpumped_signal, dark_offset):
        pumped = _apply_channel_dark_local(pumped_signal, dark_offset)
        unpumped = _apply_channel_dark_local(unpumped_signal, dark_offset)
        if pumped is None or unpumped is None:
            return None
        pumped_safe = np.asarray(pumped, dtype=float)
        unpumped_safe = np.asarray(unpumped, dtype=float)
        valid = (
            np.isfinite(pumped_safe)
            & np.isfinite(unpumped_safe)
            & (pumped_safe > reference_ratio_floor)
            & (unpumped_safe > reference_ratio_floor)
        )
        ratio = np.divide(
            pumped_safe,
            unpumped_safe,
            out=np.ones_like(pumped_safe),
            where=valid,
        )
        valid &= np.isfinite(ratio) & (ratio > 0.0)
        out = np.full(pumped_safe.shape, np.nan, dtype=float)
        out[valid] = -np.log10(ratio[valid])
        return out

    def _apply_channel_dark_local(line_data, dark_offset):
        if line_data is None:
            return None
        arr = np.asarray(line_data, dtype=float)
        if dark_offset is None:
            return arr.copy()
        return arr - dark_offset

    def _demod_source_line_local(main_line, derived_line):
        if reference_processing_mode == "ratio" and derived_line is not None:
            return derived_line
        return main_line

    def _reset_pairwise_dark_corrected_local():
        nonlocal chop_pair_dark_corrected_buffer
        nonlocal chop_pair_dark_corrected_sum
        nonlocal latest_chop_integrated_pairwise_dark_corrected
        chop_pair_dark_corrected_buffer = deque(
            maxlen=max(1, int(integration_line_count))
        )
        chop_pair_dark_corrected_sum = np.zeros_like(x, dtype=float)
        latest_chop_integrated_pairwise_dark_corrected = None

    def _update_pairwise_dark_corrected_referenced_local(chop_pair):
        nonlocal latest_chop_integrated_pairwise_dark_corrected
        nonlocal chop_pair_dark_corrected_sum
        if not (
            reference_processing_mode == "ratio"
            and pump_chop_dark_offset is not None
        ):
            return
        pair = np.asarray(chop_pair, dtype=float)
        if pump_chop_dark_offset.size != pair.size:
            return
        corrected = pair - pump_chop_dark_offset
        if len(chop_pair_dark_corrected_buffer) == chop_pair_dark_corrected_buffer.maxlen:
            chop_pair_dark_corrected_sum -= chop_pair_dark_corrected_buffer.popleft()
        chop_pair_dark_corrected_buffer.append(corrected.copy())
        chop_pair_dark_corrected_sum += corrected
        latest_chop_integrated_pairwise_dark_corrected = (
            chop_pair_dark_corrected_sum / float(len(chop_pair_dark_corrected_buffer))
        )

    def _on_key_press(event):
        nonlocal pump_chop_dark_offset
        nonlocal pump_chop_dark_main_diagnostic_offset
        nonlocal pump_chop_dark_ref_diagnostic_offset
        nonlocal channel_dark_main_offset, channel_dark_ref_offset
        key = str(getattr(event, "key", "") or "").lower()
        if key in {"q", "escape"}:
            _request_stop("user requested stop from plot window")
            return
        if not pump_chop_demod:
            return
        if key == "d":
            if latest_chop_integrated is None:
                print(
                    "Pump-dark capture skipped: no integrated pump-chop trace "
                    "is available yet."
                )
                return
            trace = np.asarray(latest_chop_integrated, dtype=float)
            main_diag_trace = None
            ref_diag_trace = None
            if read_reference and reference_processing_mode == "ratio":
                if latest_chop_main_diagnostic is not None:
                    main_diag_trace = np.asarray(
                        latest_chop_main_diagnostic,
                        dtype=float,
                    )
                if latest_chop_ref_diagnostic is not None:
                    ref_diag_trace = np.asarray(
                        latest_chop_ref_diagnostic,
                        dtype=float,
                    )
            _save_pump_dark_offset(
                trace,
                main_diagnostic_trace=main_diag_trace,
                ref_diagnostic_trace=ref_diag_trace,
            )
            pump_chop_dark_offset = trace.copy()
            pump_chop_dark_main_diagnostic_offset = (
                None if main_diag_trace is None else main_diag_trace.copy()
            )
            pump_chop_dark_ref_diagnostic_offset = (
                None if ref_diag_trace is None else ref_diag_trace.copy()
            )
            _sync_pump_dark_to_runtime()
            _reset_pairwise_dark_corrected_local()
            _set_pump_dark_status(f"active ({pump_chop_dark_path.name})")
            if read_reference and reference_processing_mode == "ratio":
                print(
                    "Saved current integrated referenced DeltaOD baselines to "
                    f"{pump_chop_dark_path} and enabled subtraction for the "
                    "final S/R, main-only, ref-only, and pairwise-corrected "
                    "comparison demod traces."
                )
            else:
                print(
                    "Saved current integrated pump-chop trace as dark offset to "
                    f"{pump_chop_dark_path} and enabled subtraction."
                )
        elif key == "l":
            try:
                loaded = _load_pump_dark_offset(print_message=True)
                if loaded:
                    _sync_pump_dark_to_runtime()
                    _reset_pairwise_dark_corrected_local()
            except Exception as exc:
                print(f"Pump-dark load failed: {exc}")
        elif key == "x":
            pump_chop_dark_offset = None
            pump_chop_dark_main_diagnostic_offset = None
            pump_chop_dark_ref_diagnostic_offset = None
            _sync_pump_dark_to_runtime()
            _reset_pairwise_dark_corrected_local()
            _set_pump_dark_status(
                "ready ({})".format(pump_chop_dark_path.name)
                if pump_chop_dark_subtract
                else "disabled"
            )
            print("Cleared in-memory pump-dark subtraction.")
        elif key == "s":
            _set_runtime_pump_chop_sign(-pump_chop_sign)
            print(f"Pump-chop sign flipped live. New sign={pump_chop_sign:+.0f}.")
        elif key in {"+", "="}:
            _set_runtime_pump_chop_sign(+1.0)
            print("Pump-chop sign set live to +1.")
        elif key in {"-", "_"}:
            _set_runtime_pump_chop_sign(-1.0)
            print("Pump-chop sign set live to -1.")
        elif key == "c":
            if not channel_dark_subtract:
                print(
                    "Intensity/channel dark capture skipped: dark subtraction is disabled."
                )
                return
            if read_reference and reference_processing_mode == "ratio":
                if integrated_line is None or ref_integrated_line is None:
                    print(
                        "Channel-dark capture skipped: integrated main/ref traces are not available yet."
                    )
                    return
                _save_channel_dark_offsets(integrated_line, ref_integrated_line)
                channel_dark_main_offset = np.asarray(
                    integrated_line, dtype=float
                ).copy()
                channel_dark_ref_offset = np.asarray(
                    ref_integrated_line, dtype=float
                ).copy()
                _sync_channel_dark_to_runtime()
                _set_channel_dark_status(f"active ({channel_dark_path.name})")
                print(
                    "Saved current integrated main/ref traces as channel-dark offsets to "
                    f"{channel_dark_path} and enabled referenced channel subtraction."
                )
            else:
                if integrated_line is None:
                    print(
                        "Intensity-dark capture skipped: integrated main trace is not available yet."
                    )
                    return
                _save_channel_dark_offsets(integrated_line, None)
                channel_dark_main_offset = np.asarray(
                    integrated_line, dtype=float
                ).copy()
                channel_dark_ref_offset = None
                _sync_channel_dark_to_runtime()
                _set_channel_dark_status(f"active ({channel_dark_path.name})")
                print(
                    "Saved current integrated main trace as intensity dark to "
                    f"{channel_dark_path}; it will only correct the main-only "
                    "mOD display denominator."
                )
        elif key == "v":
            try:
                loaded = _load_channel_dark_offsets(print_message=True)
                if loaded:
                    _sync_channel_dark_to_runtime()
            except Exception as exc:
                print(f"Channel-dark load failed: {exc}")
        elif key == "n":
            channel_dark_main_offset = None
            channel_dark_ref_offset = None
            _sync_channel_dark_to_runtime()
            _set_channel_dark_status(
                "ready ({})".format(channel_dark_path.name)
                if channel_dark_subtract
                else "disabled"
            )
            print("Cleared in-memory intensity/channel dark subtraction.")

    fig.canvas.mpl_connect("key_press_event", _on_key_press)
    fig.canvas.mpl_connect(
        "close_event",
        lambda _event: _request_stop("plot window closed"),
    )

    try:
        session_context = (
            _RetriggerLineSession(
                pda,
                ai_buffer_lines=persistent_ai_buffer_lines,
                read_reference=read_reference,
                tdms_log_enable=tdms_log_enable,
                tdms_file_path=tdms_file_path,
                tdms_group_name=tdms_group_name,
                tdms_logging_mode=tdms_logging_mode,
                tdms_logging_operation=tdms_logging_operation,
                latest_only_read=retrigger_latest_only_read,
                overwrite_unread=retrigger_overwrite_unread,
            )
            if use_fast_session
            else nullcontext(None)
        )
        chopper_sync_context = (
            _ChopperSyncOutSession(pda)
            if pda.chopper_sync_enable
            else nullcontext(None)
        )
        monitor_context = (
            _PFI9EdgeMonitorSession(
                pda,
                counter=pfi9_monitor_counter,
                rate_gate_s=pfi9_rate_gate_s,
            )
            if requested_pfi9_monitor
            else nullcontext(None)
        )
        chopper_input_monitor_context = (
            _ChopperInputEdgeMonitorSession(
                pda,
                counter=chopper_input_monitor_counter,
                rate_gate_s=chopper_input_rate_gate_s,
            )
            if (
                requested_chopper_input_monitor
                and pump_chop_phase_source != "chopper_input"
            )
            else nullcontext(None)
        )
        chopper_input_state_context = (
            _ChopperInputStateSession(pda)
            if pump_chop_phase_source == "chopper_input"
            else nullcontext(None)
        )
        with (
            session_context as session,
            chopper_sync_context as chopper_sync,
            monitor_context as pfi9_monitor,
            chopper_input_monitor_context as chopper_input_monitor,
            chopper_input_state_context as chopper_input_state,
        ):
            if pda.chopper_sync_enable:
                if (
                    chopper_sync is not None
                    and getattr(chopper_sync, "available", False)
                ):
                    print(
                        "Chopper sync output active on "
                        f"{pda.chopper_sync_out_term} "
                        f"(source: {pda.chopper_sync_source_terminal})."
                    )
                elif (
                    chopper_sync is not None
                    and getattr(chopper_sync, "error_text", "")
                ):
                    print(
                            "Warning: chopper sync output unavailable; continuing without it: "
                            f"{str(getattr(chopper_sync, 'error_text', ''))}"
                    )
            actual_phase_source = pump_chop_phase_source
            if actual_phase_source == "chopper_input":
                if (
                    chopper_input_state is not None
                    and getattr(chopper_input_state, "available", False)
                ):
                    print(
                        "Chopper-input phase grouping enabled on "
                        f"{pda.chopper_input_term}."
                    )
                else:
                    actual_phase_source = "inferred"
                    print(
                        "Warning: chopper-input phase grouping unavailable; "
                        "falling back to inferred grouping. "
                        f"Detail: {str(getattr(chopper_input_state, 'error_text', ''))}"
                    )
            if pump_chop_demod:
                if actual_phase_source == "chopper_input":
                    print(
                        "Pump-chop phase source: "
                        f"external chopper input on {pda.chopper_input_term}"
                    )
                else:
                    print("Pump-chop phase source: inferred from acquisition order")

            accumulator = _LiveLineAccumulator(
                initial_size=int(pda.output_samples_per_line),
                integration_line_count=integration_line_count,
                pump_chop_demod=pump_chop_demod,
                pump_chop_sign=pump_chop_sign,
                pump_chop_phase_source=actual_phase_source,
                pump_chop_use_adjacent_pairs=pump_chop_use_adjacent_pairs,
                pump_chop_sign_agnostic_preview=pump_chop_sign_agnostic_preview,
                pump_chop_tail_heuristic_enable=pump_chop_tail_heuristic_enable,
                pump_chop_tail_heuristic_start=pump_chop_tail_heuristic_start,
                pump_chop_tail_heuristic_stop=pump_chop_tail_heuristic_stop,
                pump_chop_tail_heuristic_expected_sign=pump_chop_tail_heuristic_expected_sign,
                pump_chop_tail_heuristic_zero_baseline=pump_chop_tail_heuristic_zero_baseline,
                reference_processing_mode=reference_processing_mode,
                reference_ratio_floor=reference_ratio_floor,
                channel_dark_main_offset=channel_dark_main_offset,
                channel_dark_ref_offset=channel_dark_ref_offset,
                pump_chop_dark_offset=pump_chop_dark_offset,
                capture_hit_rate_enable=capture_hit_rate_enable,
                capture_hit_rate_window_lines=capture_hit_rate_window_lines,
                capture_hit_warmup_lines=capture_hit_warmup_lines,
                capture_hit_threshold_fraction=capture_hit_threshold_fraction,
            )
            if pfi9_monitor is not None:
                if getattr(pfi9_monitor, "available", False):
                    print(
                        f"PFI9 monitor enabled on {pfi9_monitor.counter} "
                        f"(source: {pda.trig_in}, gate: {pfi9_monitor.rate_gate_s * 1e3:.0f} ms)."
                    )
                elif getattr(pfi9_monitor, "error_text", ""):
                    pfi9_err = str(getattr(pfi9_monitor, "error_text", ""))
                    if "minimum pulse width" in pfi9_err.lower():
                        pfi9_err += (
                            " | Hint: PFI9 is shared; make trigger and monitor filter "
                            "settings match, or disable one user of this terminal."
                        )
                    print(
                        "PFI9 monitor unavailable; continuing without it: "
                        f"{pfi9_err}"
                    )
            if chopper_input_monitor is not None:
                if getattr(chopper_input_monitor, "available", False):
                    print(
                        "Chopper-input monitor enabled on "
                        f"{chopper_input_monitor.counter} "
                        f"(source: {pda.chopper_input_term}, gate: "
                        f"{chopper_input_monitor.rate_gate_s * 1e3:.0f} ms)."
                    )
                elif getattr(chopper_input_monitor, "error_text", ""):
                    print(
                        "Chopper-input monitor unavailable; continuing without it: "
                        f"{str(getattr(chopper_input_monitor, 'error_text', ''))}"
                    )
            elif requested_chopper_input_monitor and actual_phase_source == "chopper_input":
                print(
                    "Chopper-input monitor: deriving displayed rate from "
                    f"{pda.chopper_input_term} phase labels."
                )
            active_rate_gate_s = None
            if (
                rate_plot_is_chopper_input
                and chopper_input_monitor is not None
                and getattr(chopper_input_monitor, "available", False)
            ):
                active_rate_gate_s = float(chopper_input_monitor.rate_gate_s)
            elif rate_plot_is_chopper_input and actual_phase_source == "chopper_input":
                active_rate_gate_s = float(chopper_input_rate_gate_s)
            elif (
                pfi9_monitor is not None
                and getattr(pfi9_monitor, "available", False)
            ):
                active_rate_gate_s = float(pfi9_monitor.rate_gate_s)
            if active_rate_gate_s is not None:
                plot_rate_poll_interval_s = max(
                    0.01,
                    min(0.05, 0.5 * active_rate_gate_s),
                )

            def _read_plot_rate():
                nonlocal last_plot_rate_read_t, cached_plot_rate_hz
                nonlocal cached_plot_rate_count
                now = time.perf_counter()
                if (
                    np.isfinite(cached_plot_rate_hz)
                    and (now - last_plot_rate_read_t) < plot_rate_poll_interval_s
                ):
                    return cached_plot_rate_hz, cached_plot_rate_count
                if (
                    rate_plot_is_chopper_input
                    and actual_phase_source == "chopper_input"
                ):
                    cached_plot_rate_hz = float(derived_chopper_last_rate_hz)
                    cached_plot_rate_count = None
                    last_plot_rate_read_t = now
                    return cached_plot_rate_hz, cached_plot_rate_count
                if (
                    rate_plot_is_chopper_input
                    and chopper_input_monitor is not None
                    and getattr(chopper_input_monitor, "available", False)
                ):
                    rate_hz, rate_count = chopper_input_monitor.read_rate()
                    cached_plot_rate_hz = float(rate_hz)
                    cached_plot_rate_count = rate_count
                    last_plot_rate_read_t = now
                    return cached_plot_rate_hz, cached_plot_rate_count
                if (
                    pfi9_monitor is not None
                    and getattr(pfi9_monitor, "available", False)
                ):
                    rate_hz, rate_count = pfi9_monitor.read_rate()
                    cached_plot_rate_hz = float(rate_hz)
                    cached_plot_rate_count = rate_count
                    last_plot_rate_read_t = now
                    return cached_plot_rate_hz, cached_plot_rate_count
                cached_plot_rate_hz = float("nan")
                cached_plot_rate_count = None
                last_plot_rate_read_t = now
                return cached_plot_rate_hz, cached_plot_rate_count

            def _rate_status_line():
                if rate_plot_is_chopper_input:
                    if actual_phase_source == "chopper_input":
                        return (
                            "Chopper monitor: derived from phase labels "
                            f"({pda.chopper_input_term}, gate={chopper_input_rate_gate_s * 1e3:.1f} ms)"
                        )
                    if chopper_input_monitor is None:
                        return "Chopper monitor: disabled"
                    if getattr(chopper_input_monitor, "available", False):
                        return (
                            "Chopper monitor: active "
                            f"({chopper_input_monitor.counter}, "
                            f"src={pda.chopper_input_term}, "
                            f"gate={chopper_input_monitor.rate_gate_s * 1e3:.1f} ms)"
                        )
                    chop_err = str(
                        getattr(chopper_input_monitor, "error_text", "")
                    ).strip()
                    if len(chop_err) > 90:
                        chop_err = chop_err[:87] + "..."
                    return "Chopper monitor: unavailable" + (
                        f" ({chop_err})" if chop_err else ""
                    )
                if pfi9_monitor is None:
                    return (
                        "PFI9 monitor: disabled; plotting DAQ line rate"
                        if (rate_plot_enable and not rate_plot_is_pfi9)
                        else "PFI9 monitor: disabled"
                    )
                if getattr(pfi9_monitor, "available", False):
                    return (
                        "PFI9 monitor: active "
                        f"({pfi9_monitor.counter}, gate={pfi9_monitor.rate_gate_s * 1e3:.1f} ms)"
                    )
                pfi9_err = str(getattr(pfi9_monitor, "error_text", "")).strip()
                if len(pfi9_err) > 90:
                    pfi9_err = pfi9_err[:87] + "..."
                return "PFI9 monitor: unavailable" + (
                    f" ({pfi9_err})" if pfi9_err else ""
                )

            def _read_chopper_phase_state():
                if actual_phase_source != "chopper_input":
                    return None
                if (
                    chopper_input_state is None
                    or not getattr(chopper_input_state, "available", False)
                ):
                    return None
                return chopper_input_state.read_state()

            def _attach_phase_state(raw_data):
                nonlocal derived_chopper_prev_state
                nonlocal derived_chopper_last_rate_hz
                nonlocal derived_chopper_gate_start_t
                nonlocal derived_chopper_rising_edges
                if actual_phase_source != "chopper_input":
                    return raw_data
                phase_state = _read_chopper_phase_state()
                if phase_state is None:
                    return raw_data

                def _update_derived_rate(state_sequence):
                    nonlocal derived_chopper_prev_state
                    nonlocal derived_chopper_last_rate_hz
                    nonlocal derived_chopper_gate_start_t
                    nonlocal derived_chopper_rising_edges
                    now = time.perf_counter()
                    if derived_chopper_gate_start_t is None:
                        derived_chopper_gate_start_t = now
                    for state_value in state_sequence:
                        state_value = int(bool(state_value))
                        if (
                            derived_chopper_prev_state is not None
                            and derived_chopper_prev_state == 0
                            and state_value == 1
                        ):
                            derived_chopper_rising_edges += 1
                        derived_chopper_prev_state = state_value
                    gate_dt = now - derived_chopper_gate_start_t
                    if gate_dt >= chopper_input_rate_gate_s:
                        derived_chopper_last_rate_hz = float(
                            derived_chopper_rising_edges / max(gate_dt, 1e-6)
                        )
                        derived_chopper_gate_start_t = now
                        derived_chopper_rising_edges = 0

                def _wrap_one(item, state_value):
                    if isinstance(item, dict):
                        wrapped = dict(item)
                        wrapped["_phase_state"] = int(bool(state_value))
                        return wrapped
                    return {
                        "main": item,
                        "_phase_state": int(bool(state_value)),
                    }

                if isinstance(raw_data, list):
                    latest_state = int(bool(phase_state))
                    wrapped_items = []
                    state_sequence = []
                    item_count = len(raw_data)
                    for idx, item in enumerate(raw_data):
                        item_state = latest_state ^ ((item_count - 1 - idx) & 1)
                        state_sequence.append(item_state)
                        wrapped_items.append(_wrap_one(item, item_state))
                    _update_derived_rate(state_sequence)
                    return wrapped_items
                _update_derived_rate([phase_state])
                return _wrap_one(raw_data, phase_state)
            reader = None
            processor = None

            def _schedule_fast_rebuild():
                nonlocal fast_rebuild_deadline_t, fast_rebuild_exhausted_announced
                if not use_fast_session:
                    return
                if fast_rebuild_attempt_count >= fast_rebuild_max_attempts:
                    fast_rebuild_deadline_t = None
                    if not fast_rebuild_exhausted_announced:
                        print(
                            "Fast-session rebuild limit reached; remaining in safe mode."
                        )
                        fast_rebuild_exhausted_announced = True
                    return
                fast_rebuild_deadline_t = (
                    time.perf_counter() + fast_rebuild_delay_s
                )
                _mark_data_activity()
                print(
                    "Scheduling retriggered-session rebuild in "
                    f"{fast_rebuild_delay_s:.1f} s "
                    f"({fast_rebuild_attempt_count + 1}/{fast_rebuild_max_attempts})."
                )

            def _try_fast_rebuild():
                nonlocal session, reader, processor, line_mode
                nonlocal fast_rebuild_deadline_t, fast_rebuild_attempt_count
                nonlocal fast_rebuild_exhausted_announced
                nonlocal parity_reset_requested, parity_reset_reason
                if not use_fast_session or session is not None:
                    return False
                if fast_rebuild_deadline_t is None:
                    return False
                if time.perf_counter() < fast_rebuild_deadline_t:
                    return False
                if fast_rebuild_attempt_count >= fast_rebuild_max_attempts:
                    fast_rebuild_deadline_t = None
                    if not fast_rebuild_exhausted_announced:
                        print(
                            "Fast-session rebuild limit reached; remaining in safe mode."
                        )
                        fast_rebuild_exhausted_announced = True
                    return False

                fast_rebuild_attempt_count += 1
                fast_rebuild_deadline_t = None
                rebuild_idx = fast_rebuild_attempt_count
                print(
                    "Attempting retriggered-session rebuild "
                    f"({rebuild_idx}/{fast_rebuild_max_attempts})..."
                )
                rebuilt_session = None
                rebuilt_reader = None
                rebuilt_processor = None
                try:
                    rebuilt_session = _RetriggerLineSession(
                        pda,
                        ai_buffer_lines=persistent_ai_buffer_lines,
                        read_reference=read_reference,
                        tdms_log_enable=tdms_log_enable,
                        tdms_file_path=tdms_file_path,
                        tdms_group_name=tdms_group_name,
                        tdms_logging_mode=tdms_logging_mode,
                        tdms_logging_operation=tdms_logging_operation,
                        latest_only_read=retrigger_latest_only_read,
                        overwrite_unread=retrigger_overwrite_unread,
                    ).__enter__()
                    if (
                        decouple_acquisition_from_plot
                        and not retrigger_latest_only_read
                    ):

                        def _processor_pull():
                            fast_data = rebuilt_session.read_available_lines(
                                max_lines=ordered_read_batch_lines,
                                timeout=background_read_timeout_s,
                            )
                            fast_data = _attach_phase_state(fast_data)
                            fast_lines = (
                                len(fast_data)
                                if isinstance(fast_data, list)
                                else 1
                            )
                            return fast_data, max(1, fast_lines)

                        rebuilt_processor = _BackgroundAccumulatorProcessor(
                            _processor_pull,
                            accumulator=accumulator,
                            read_reference=read_reference,
                            snapshot_interval_s=max(
                                0.005, 0.5 * plot_update_interval_s
                            ),
                        ).start()
                        print(
                            "Background processor re-enabled after rebuild "
                            f"(batch up to {ordered_read_batch_lines} lines/read)."
                        )
                    elif decouple_acquisition_from_plot:

                        def _reader_pull():
                            fast_data = _attach_phase_state(
                                rebuilt_session.read_line(
                                    timeout=background_read_timeout_s
                                )
                            )
                            fast_lines = int(
                                getattr(rebuilt_session, "last_lines_consumed", 1)
                            )
                            return fast_data, max(1, fast_lines)

                        rebuilt_reader = _BackgroundLineReader(
                            _reader_pull,
                            max_packets=reader_fifo_max_packets,
                        ).start()
                        print(
                            "Background reader re-enabled after rebuild with "
                            "strict FIFO ordering."
                        )

                    session = rebuilt_session
                    reader = rebuilt_reader
                    processor = rebuilt_processor
                    line_mode = _fast_line_mode_text()
                    print(f"Acquisition mode: {line_mode}")
                    _set_mode_badge("FAST", "#d9f2d9")
                    _mark_data_activity()
                    accumulator.request_parity_reset("fast-session rebuild")
                    parity_reset_requested = True
                    parity_reset_reason = "fast-session rebuild"
                    print(
                        "Retriggered-session rebuild succeeded; returned to fast mode."
                    )
                    return True
                except Exception as rebuild_exc:
                    if rebuilt_processor is not None:
                        try:
                            rebuilt_processor.close()
                        except Exception:
                            pass
                    if rebuilt_reader is not None:
                        try:
                            rebuilt_reader.close()
                        except Exception:
                            pass
                    if rebuilt_session is not None:
                        try:
                            rebuilt_session.close()
                        except Exception:
                            pass
                    print(
                        "Warning: retriggered-session rebuild attempt "
                        f"{rebuild_idx}/{fast_rebuild_max_attempts} failed. "
                        f"Continuing in safe mode. Detail: {rebuild_exc}"
                    )
                    _schedule_fast_rebuild()
                    return False

            if (
                session is not None
                and decouple_acquisition_from_plot
                and not retrigger_latest_only_read
            ):
                def _processor_pull():
                    fast_data = session.read_available_lines(
                        max_lines=ordered_read_batch_lines,
                        timeout=background_read_timeout_s,
                    )
                    fast_data = _attach_phase_state(fast_data)
                    fast_lines = len(fast_data) if isinstance(fast_data, list) else 1
                    return fast_data, max(1, fast_lines)

                processor = _BackgroundAccumulatorProcessor(
                    _processor_pull,
                    accumulator=accumulator,
                    read_reference=read_reference,
                    snapshot_interval_s=max(0.005, 0.5 * plot_update_interval_s),
                ).start()
                print(
                    "Background processor enabled: ordered acquisition plus "
                    "phase-bucket accumulation now runs off the GUI thread "
                    f"(batch up to {ordered_read_batch_lines} lines/read)."
                )
            elif session is not None and decouple_acquisition_from_plot:
                def _reader_pull():
                    fast_data = _attach_phase_state(
                        session.read_line(timeout=background_read_timeout_s)
                    )
                    fast_lines = int(getattr(session, "last_lines_consumed", 1))
                    return fast_data, max(1, fast_lines)

                reader = _BackgroundLineReader(
                    _reader_pull,
                    max_packets=reader_fifo_max_packets,
                ).start()
                print(
                    "Background reader enabled: DAQ acquisition decoupled "
                    "from plotting loop with strict FIFO ordering."
                )
            while True:
                if stop_requested:
                    break
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
                if _try_fast_rebuild():
                    continue

                if processor is not None:
                    try:
                        snap, processor_exc = processor.snapshot()
                        if processor_exc is not None:
                            raise processor_exc
                        if (
                            snap is None
                            or int(snap.get("snapshot_seq", 0))
                            <= int(processor_snapshot_seq_last)
                        ):
                            plot_rate_idle, _ = _read_plot_rate()
                            if np.isfinite(plot_rate_idle):
                                pfi9_rate_hz_buffer.append(float(plot_rate_idle))
                            _maybe_show_no_data_warning(
                                "No accepted acquisition snapshots yet."
                            )
                            plt.pause(ui_idle_pause_s)
                            continue
                        processor_snapshot_seq_last = int(snap["snapshot_seq"])
                        _mark_data_activity()
                        read_service_rate_hz = float(snap["service_rate_hz"])
                        wall_line_rate_hz = float(snap["wall_line_rate_hz"])
                        line_counter = int(snap["line_counter"])
                        line = np.asarray(snap["line"], dtype=float)
                        integrated_line = np.asarray(
                            snap["integrated_line"], dtype=float
                        )
                        ref_line = (
                            None
                            if snap["ref_line"] is None
                            else np.asarray(snap["ref_line"], dtype=float)
                        )
                        ref_integrated_line = (
                            None
                            if snap["ref_integrated_line"] is None
                            else np.asarray(snap["ref_integrated_line"], dtype=float)
                        )
                        diff_line = (
                            None
                            if snap["diff_line"] is None
                            else np.asarray(snap["diff_line"], dtype=float)
                        )
                        diff_integrated_line = (
                            None
                            if snap["diff_integrated_line"] is None
                            else np.asarray(snap["diff_integrated_line"], dtype=float)
                        )
                        latest_chop_pair = (
                            None
                            if snap["latest_chop_pair"] is None
                            else np.asarray(snap["latest_chop_pair"], dtype=float)
                        )
                        latest_chop_integrated = (
                            None
                            if snap["latest_chop_integrated"] is None
                            else np.asarray(
                                snap["latest_chop_integrated"], dtype=float
                            )
                        )
                        latest_chop_integrated_pairwise_dark_corrected = (
                            None
                            if snap.get("latest_chop_integrated_pairwise_dark_corrected") is None
                            else np.asarray(
                                snap["latest_chop_integrated_pairwise_dark_corrected"],
                                dtype=float,
                            )
                        )
                        latest_chop_main_diagnostic = (
                            None
                            if snap.get("latest_chop_main_diagnostic") is None
                            else np.asarray(
                                snap["latest_chop_main_diagnostic"], dtype=float
                            )
                        )
                        latest_chop_ref_diagnostic = (
                            None
                            if snap.get("latest_chop_ref_diagnostic") is None
                            else np.asarray(
                                snap["latest_chop_ref_diagnostic"], dtype=float
                            )
                        )
                        integrated_auc_main = float(snap["integrated_auc_main"])
                        integrated_auc_ref = float(snap["integrated_auc_ref"])
                        integrated_auc_diff = float(snap["integrated_auc_diff"])
                        capture_hit_rate_pct = float(snap["capture_hit_rate_pct"])
                        capture_last_score_vpp = float(snap["capture_last_score_vpp"])
                        capture_last_threshold_vpp = float(
                            snap["capture_last_threshold_vpp"]
                        )
                        capture_warmup_count = int(snap["capture_warmup_count"])
                        chop_pair_counter = int(snap["chop_pair_counter"])
                        chop_preview_flip_counter = int(
                            snap["chop_preview_flip_counter"]
                        )
                        tail_guided_flip_counter = int(
                            snap.get("tail_guided_flip_counter", 0)
                        )
                        tail_guided_last_mean = float(
                            snap.get("tail_guided_last_mean", float("nan"))
                        )
                        chop_parity_reset_counter = int(
                            snap["chop_parity_reset_counter"]
                        )
                        line_buffer_len = int(snap["line_buffer_len"])
                        phase0_buffer_len = int(snap["phase0_buffer_len"])
                        phase1_buffer_len = int(snap["phase1_buffer_len"])
                        pair_buffer_len = int(snap["pair_buffer_len"])
                        warning_text = snap.get("last_warning_message")
                        if (
                            warning_text
                            and warning_text != processor_last_warning
                        ):
                            print(warning_text)
                            processor_last_warning = warning_text
                    except Exception as exc:
                        if session is not None and _is_buffer_overwrite_error(exc):
                            median_pending = (
                                float(np.median(pending_lines_buffer))
                                if pending_lines_buffer
                                else float("nan")
                            )
                            expected = (
                                float(expected_trigger_hz)
                                if expected_trigger_hz > 0
                                else float("nan")
                            )
                            print(
                                "\nFast-mode diagnostics before fallback: "
                                f"service_rate~{read_service_rate_hz:.1f} Hz, "
                                f"wall_rate~{wall_line_rate_hz:.1f} Hz, "
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
                                processor.close()
                            except Exception:
                                pass
                            processor = None
                            try:
                                session.close()
                            except Exception as close_exc:
                                if not fallback_close_warned:
                                    print(
                                        "Warning: fast-session close during fallback "
                                        f"reported an error. Detail: {close_exc}"
                                    )
                                    fallback_close_warned = True
                            session = None
                            line_mode = "per-line task build/start (fallback)"
                            print(f"Acquisition mode: {line_mode}")
                            _set_mode_badge("SAFE-FALLBACK", "#f7d9d9")
                            accumulator.request_parity_reset(
                                "fast-session fallback/possible line loss"
                            )
                            _schedule_fast_rebuild()
                            continue
                        raise

                    pfi9_rate_hz, _ = _read_plot_rate()
                    pfi9_count = None
                    if np.isfinite(pfi9_rate_hz):
                        pfi9_rate_hz_buffer.append(float(pfi9_rate_hz))
                    if (
                        pfi9_monitor is not None
                        and getattr(pfi9_monitor, "available", False)
                    ):
                        _, pfi9_count = pfi9_monitor.read_rate()
                    if pda.use_external_trigger:
                        if np.isfinite(pfi9_rate_hz) and pfi9_rate_hz > 0:
                            trigger_eff = min(1.0, wall_line_rate_hz / pfi9_rate_hz)
                        elif expected_trigger_hz > 0:
                            trigger_eff = min(
                                1.0, wall_line_rate_hz / expected_trigger_hz
                            )
                        else:
                            trigger_eff = 1.0
                    else:
                        trigger_eff = 1.0
                    if (
                        rate_plot_enable
                        and not (rate_plot_is_pfi9 or rate_plot_is_chopper_input)
                        and np.isfinite(wall_line_rate_hz)
                    ):
                        pfi9_rate_hz_buffer.append(float(wall_line_rate_hz))

                    if (
                        expected_trigger_hz > 0
                        and np.isfinite(pfi9_rate_hz)
                        and pfi9_rate_hz > (1.5 * expected_trigger_hz)
                        and not pfi9_rate_warned
                    ):
                        print(
                            "Warning: PFI9 edge rate is much higher than expected "
                            f"({pfi9_rate_hz:.1f} vs {expected_trigger_hz:.1f} Hz). "
                            "This can cause parity flips and pump-chop cancellation in persistent mode."
                        )
                        pfi9_rate_warned = True

                    size_changed = False
                    if line.size != x.size:
                        x = pda.output_sample_axis(line.size)
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
                        if derived_axis is not None and derived_axis is not ax:
                            derived_axis.set_xlim(float(x[0]), float(x[-1]))
                        if chop_raw_line_plot is not None:
                            chop_raw_line_plot.set_xdata(x)
                        if chop_integrated_line_plot is not None:
                            chop_integrated_line_plot.set_xdata(x)
                        if chop_integrated_pairwise_dark_corrected_plot is not None:
                            chop_integrated_pairwise_dark_corrected_plot.set_xdata(x)
                        if chop_main_diag_line_plot is not None:
                            chop_main_diag_line_plot.set_xdata(x)
                        if chop_ref_diag_line_plot is not None:
                            chop_ref_diag_line_plot.set_xdata(x)
                        ax.set_xlim(float(x[0]), float(x[-1]))
                        if ax_demod is not None:
                            ax_demod.set_xlim(float(x[0]), float(x[-1]))
                        if (
                            pump_chop_dark_offset is not None
                            and pump_chop_dark_offset.size != line.size
                        ):
                            pump_chop_dark_offset = None
                            _set_pump_dark_status(
                                f"size mismatch after resize ({line.size})"
                            )
                            print(
                                "Cleared pump-dark offset after sample-count change."
                            )
                        size_changed = True

                    reader_queue_depth = float("nan")
                    reader_queue_max_depth = float("nan")
                    now_for_plot = time.perf_counter()
                    should_update_plot = size_changed or (
                        now_for_plot >= next_plot_update_t
                    )
                    if not should_update_plot:
                        continue
                    next_plot_update_t = now_for_plot + plot_update_interval_s

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
                    latest_chop_pair_display = _prepare_chop_display(
                        latest_chop_pair,
                        integrated_line,
                        apply_dark=False,
                    )
                    if (
                        chop_raw_line_plot is not None
                        and latest_chop_pair_display is not None
                    ):
                        chop_raw_line_plot.set_ydata(latest_chop_pair_display)
                    latest_chop_integrated_display = _prepare_chop_display(
                        latest_chop_integrated,
                        integrated_line,
                        apply_dark=True,
                        dark_role="demod",
                    )
                    if (
                        chop_integrated_line_plot is not None
                        and latest_chop_integrated_display is not None
                    ):
                        chop_integrated_line_plot.set_ydata(
                            latest_chop_integrated_display
                        )
                    latest_chop_integrated_pairwise_dark_corrected_display = _prepare_chop_display(
                        latest_chop_integrated_pairwise_dark_corrected,
                        integrated_line,
                        apply_dark=False,
                    )
                    if (
                        chop_integrated_pairwise_dark_corrected_plot is not None
                        and latest_chop_integrated_pairwise_dark_corrected_display is not None
                    ):
                        chop_integrated_pairwise_dark_corrected_plot.set_ydata(
                            latest_chop_integrated_pairwise_dark_corrected_display
                        )
                    latest_chop_main_diag_display = _prepare_chop_display(
                        latest_chop_main_diagnostic,
                        integrated_line,
                        apply_dark=True,
                        dark_role="main_diagnostic",
                    )
                    latest_chop_ref_diag_display = _prepare_chop_display(
                        latest_chop_ref_diagnostic,
                        ref_integrated_line,
                        apply_dark=True,
                        dark_role="ref_diagnostic",
                    )
                    if (
                        chop_main_diag_line_plot is not None
                        and latest_chop_main_diag_display is not None
                    ):
                        chop_main_diag_line_plot.set_ydata(
                            latest_chop_main_diag_display
                        )
                    if (
                        chop_ref_diag_line_plot is not None
                        and latest_chop_ref_diag_display is not None
                    ):
                        chop_ref_diag_line_plot.set_ydata(
                            latest_chop_ref_diag_display
                        )

                    ax.set_title(
                        "CMOS Video (Simple Mode) "
                        + f"| mode={mode_key} "
                        + (
                            f"| derived={derived_signal_label} "
                            if read_reference
                            else ""
                        )
                        + f"| st_delay={pda.st_initial_delay * 1e6:.1f} us "
                        + f"| clk_delay={pda.clk_initial_delay * 1e6:.1f} us "
                        + f"| integrated N={line_buffer_len} "
                        + f"| svc={read_service_rate_hz:.1f} Hz "
                        + f"| wall={wall_line_rate_hz:.1f} Hz "
                        + f"| eff={100.0 * trigger_eff:.1f}%"
                        + (
                            f" | q~{pending_lines:.1f} lines"
                            if session is not None and np.isfinite(pending_lines)
                            else ""
                        )
                        + (
                            f" | {rate_title_prefix}~{pfi9_rate_hz:.1f} Hz"
                            if np.isfinite(pfi9_rate_hz)
                            else ""
                        )
                        + (
                            f" | chop_pairs={chop_pair_counter}"
                            if pump_chop_demod
                            else ""
                        )
                        + (
                            f" | phase={pda.chopper_input_term.split('/')[-1]}"
                            if (pump_chop_demod and actual_phase_source == "chopper_input")
                            else ""
                        )
                        + (
                            f" | tailflips={tail_guided_flip_counter} tail={tail_guided_last_mean:+.3g}"
                            if (pump_chop_demod and pump_chop_tail_heuristic_enable)
                            else ""
                        )
                        + (
                            f" | hit={capture_hit_rate_pct:.1f}%"
                            if capture_hit_rate_enable and np.isfinite(capture_hit_rate_pct)
                            else ""
                        )
                    )
                    if ax_demod is not None:
                        ax_demod.set_title(_format_demod_title())
                    if plot_update_counter == 1 or (
                        plot_update_counter % timing_text_update_every_n_lines == 0
                    ):
                        d = pda.get_timing_diagnostics(
                            trigger_frequency_hz=expected_trigger_hz
                        )
                        pfi9_status_line = _rate_status_line()
                        pfi9_med = (
                            float(np.median(pfi9_rate_hz_buffer))
                            if pfi9_rate_hz_buffer
                            else float("nan")
                        )
                        queue_med = (
                            float(np.median(pending_lines_buffer))
                            if (session is not None and pending_lines_buffer)
                            else float("nan")
                        )
                        timing_lines = [
                            f"Mode={mode_badge_label} Video={mode_key} N={line_buffer_len}",
                            (
                                f"Svc/Wall={read_service_rate_hz:.1f}/"
                                f"{wall_line_rate_hz:.1f} Hz  "
                                f"Eff={100.0 * trigger_eff:.1f}%"
                            ),
                            pfi9_status_line,
                            f"AUC main={integrated_auc_main:.6g} V*s",
                        ]
                        if np.isfinite(integrated_auc_ref):
                            timing_lines.append(
                                f"AUC ref/{derived_signal_label}="
                                f"{integrated_auc_ref:.6g}/"
                                f"{integrated_auc_diff:.6g} {derived_signal_units}*s"
                            )
                        timing_lines.append(
                            (
                                f"ST rise/fall={d['st_rise_s'] * 1e6:.1f}/"
                                f"{d['st_fall_s'] * 1e6:.1f} us  "
                                f"CLK start={d['clk_start_s'] * 1e6:.1f} us"
                            )
                        )
                        margin_us = (
                            d["timing_margin_s"] * 1e6
                            if d["timing_margin_s"] is not None
                            else float("nan")
                        )
                        timing_lines.append(
                            (
                                f"AI start={d['ai_start_event_s'] * 1e6:.1f} us  "
                                f"Read={d['samples_per_line_read']}  "
                                f"Margin={margin_us:.1f} us"
                            )
                        )
                        if np.isfinite(pfi9_med):
                            pfi9_line = f"{rate_med_label}={pfi9_med:.1f} Hz"
                            if rate_plot_is_pfi9 and expected_trigger_hz > 0:
                                pfi9_line += f" ({pfi9_med / expected_trigger_hz:.2f}x)"
                            timing_lines.append(pfi9_line)
                        if np.isfinite(queue_med):
                            timing_lines.append(
                                f"AI queue med/max={queue_med:.1f}/{max_pending_lines:.1f}"
                            )
                        if (
                            trigger_budget_margin_s is not None
                            and trigger_budget_limited_s is not None
                        ):
                            timing_lines.append(
                                f"Budget margin={trigger_budget_margin_s * 1e6:.1f} us"
                            )
                        if capture_hit_rate_enable:
                            if np.isfinite(capture_hit_rate_pct):
                                timing_lines.append(
                                    f"Hit={capture_hit_rate_pct:.1f}%  "
                                    f"Vpp={capture_last_score_vpp:.4g}/"
                                    f"{capture_last_threshold_vpp:.4g}"
                                )
                            else:
                                timing_lines.append(
                                    f"Hit warmup {capture_warmup_count}/"
                                    f"{capture_hit_warmup_lines}"
                                )
                        if pump_chop_demod:
                            chop_line = (
                                f"Chop {demod_mode_label} sign={pump_chop_sign:+.0f}"
                            )
                            if actual_phase_source == "chopper_input":
                                chop_line += (
                                    f" phase={pda.chopper_input_term.split('/')[-1]}"
                                )
                            if pump_chop_use_adjacent_pairs:
                                chop_line += f" pairs={pair_buffer_len}"
                                if pump_chop_sign_agnostic_preview:
                                    chop_line += f" flips={chop_preview_flip_counter}"
                            else:
                                chop_line += (
                                    f" ph0/ph1={phase0_buffer_len}/"
                                    f"{phase1_buffer_len}"
                            )
                            chop_line += f" resets={chop_parity_reset_counter}"
                            timing_lines.append(chop_line)
                            timing_lines.append(f"Dark={pump_chop_dark_status}")
                            if channel_dark_subtract:
                                timing_lines.append(
                                    f"{channel_dark_label}={channel_dark_status}"
                                )
                            if latest_chop_integrated_display is not None:
                                chop_rms, chop_pp = _trace_rms_pp(
                                    latest_chop_integrated_display
                                )
                                timing_lines.append(
                                    f"Chop RMS/PP={chop_rms:.4g}/{chop_pp:.4g} {chop_signal_units}"
                                )
                            timing_lines.append(
                                "Keys: d save diff-dark | l load | x clear | c save intensity/chan-dark | v load | n clear | s flip | +/- sign | q/esc stop"
                            )
                        timing_text.set_text("\n".join(timing_lines))

                    if ax_trig is not None and trig_rate_plot is not None and pfi9_rate_hz_buffer:
                        trig_arr = np.asarray(pfi9_rate_hz_buffer, dtype=float)
                        trig_rate_plot.set_xdata(np.arange(trig_arr.size))
                        trig_rate_plot.set_ydata(trig_arr)
                        ax_trig.set_xlim(0, max(1, trig_arr.size - 1))

                    if plot_update_counter % autoscale_every_n_plot_updates == 0:
                        ax.relim(visible_only=True)
                        ax.autoscale_view(scalex=False, scaley=True)
                        if derived_axis is not None and derived_axis is not ax:
                            derived_axis.relim(visible_only=True)
                            derived_axis.autoscale_view(scalex=False, scaley=True)
                        if ax_demod is not None:
                            ax_demod.relim(visible_only=True)
                            ax_demod.autoscale_view(scalex=False, scaley=True)
                        if (
                            ax_trig is not None
                            and trig_rate_plot is not None
                            and pfi9_rate_hz_buffer
                        ):
                            ax_trig.relim(visible_only=True)
                            ax_trig.autoscale_view(scalex=False, scaley=True)
                    fig.canvas.draw_idle()
                    plt.pause(0.001)
                    continue

                data = None
                lines_consumed = 1
                acq_elapsed_s = 0.0
                try:
                    if reader is not None:
                        packet, reader_exc, queue_depth_now, queue_depth_max = reader.pop_next(
                            wait_timeout_s=0.0
                        )
                        reader_queue_depth = float(queue_depth_now)
                        reader_queue_max_depth = float(queue_depth_max)
                        if reader_exc is not None:
                            raise reader_exc
                        if packet is None:
                            plot_rate_idle, _ = _read_plot_rate()
                            if np.isfinite(plot_rate_idle):
                                pfi9_rate_hz_buffer.append(float(plot_rate_idle))
                            _maybe_show_no_data_warning(
                                "Waiting for the next acquired line."
                            )
                            plt.pause(ui_idle_pause_s)
                            continue

                        data = packet["data"]
                        lines_consumed = 1
                        acq_elapsed_s = max(1e-9, float(packet["elapsed_s"]))
                    else:
                        acq_start = time.perf_counter()
                        if session is None:
                            data = _attach_phase_state(
                                pda.acquire_line(
                                    timeout=gui_poll_timeout_s,
                                    read_reference=read_reference,
                                )
                            )
                            lines_consumed = 1
                        else:
                            data = _attach_phase_state(
                                session.read_line(timeout=gui_poll_timeout_s)
                            )
                            lines_consumed = int(getattr(session, "last_lines_consumed", 1))
                        acq_elapsed_s = max(1e-9, time.perf_counter() - acq_start)
                except Exception as exc:
                    if _is_daq_timeout_error(exc):
                        plot_rate_idle, _ = _read_plot_rate()
                        if np.isfinite(plot_rate_idle):
                            pfi9_rate_hz_buffer.append(float(plot_rate_idle))
                        _maybe_show_no_data_warning(
                            "Timed out waiting for a new line."
                        )
                        plt.pause(ui_idle_pause_s)
                        continue
                    if session is not None and _is_buffer_overwrite_error(exc):
                        median_pending = (
                            float(np.median(pending_lines_buffer))
                            if pending_lines_buffer
                            else float("nan")
                        )
                        expected = float(expected_trigger_hz) if expected_trigger_hz > 0 else float("nan")
                        print(
                            "\nFast-mode diagnostics before fallback: "
                            f"service_rate~{read_service_rate_hz:.1f} Hz, "
                            f"wall_rate~{wall_line_rate_hz:.1f} Hz, "
                            f"expected_trigger~{expected:.1f} Hz, "
                            f"median_queue~{median_pending:.1f} lines, "
                            f"max_queue~{max_pending_lines:.1f} lines, "
                            f"buffer~{session.ai_buffer_lines} lines."
                        )
                        print(
                            "\nWarning: retriggered session hit buffer overwrite "
                            "(-200222). Falling back to safe per-line mode."
                        )
                        if reader is not None:
                            try:
                                reader.close()
                            except Exception:
                                pass
                            reader = None
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
                        _set_mode_badge("SAFE-FALLBACK", "#f7d9d9")
                        parity_reset_requested = True
                        parity_reset_reason = "fast-session fallback/possible line loss"
                        _schedule_fast_rebuild()
                        continue
                    raise
                lines_consumed = max(1, lines_consumed)
                _mark_data_activity()
                if session is not None and lines_consumed > 1:
                    latest_multiline_read_events += 1
                    latest_skipped_lines_total += (lines_consumed - 1)
                    if not latest_skip_warned:
                        print(
                            "Note: latest-only read consumed multiple queued lines "
                            f"(first seen: {lines_consumed}). Demod phase now advances "
                            "by skipped-line parity."
                        )
                        latest_skip_warned = True
                external_phase_state = None
                if isinstance(data, dict):
                    external_phase_state = data.get("_phase_state")
                if read_reference:
                    if not (isinstance(data, dict) and "main" in data and "reference" in data):
                        raise RuntimeError(
                            "Expected {'main','reference'} data when live_video_mode='both'."
                        )
                    line = np.asarray(data["main"], dtype=float)
                    ref_line = np.asarray(data["reference"], dtype=float)
                else:
                    if isinstance(data, dict) and "main" in data:
                        line = np.asarray(data["main"], dtype=float)
                        ref_line = (
                            None
                            if data.get("reference") is None
                            else np.asarray(data["reference"], dtype=float)
                        )
                    else:
                        line = np.asarray(data, dtype=float)
                        ref_line = None
                if acq_elapsed_s > 0:
                    line_rate_hz_buffer.append(lines_consumed / acq_elapsed_s)
                read_service_rate_hz = (
                    float(np.median(line_rate_hz_buffer)) if line_rate_hz_buffer else 0.0
                )
                now_line_wall_t = time.perf_counter()
                if wall_start_t is None:
                    wall_start_t = now_line_wall_t
                    wall_total_lines = 0
                wall_total_lines += lines_consumed
                wall_elapsed_s = max(1e-6, now_line_wall_t - wall_start_t)
                wall_line_rate_hz = wall_total_lines / wall_elapsed_s

                pfi9_rate_hz = float("nan")
                pfi9_count = None
                edge_delta_since_last_line = None
                pfi9_rate_hz, _ = _read_plot_rate()
                if np.isfinite(pfi9_rate_hz):
                    pfi9_rate_hz_buffer.append(pfi9_rate_hz)
                if (
                    pfi9_monitor is not None
                    and getattr(pfi9_monitor, "available", False)
                ):
                    _, pfi9_count = pfi9_monitor.read_rate()
                    if pfi9_count is not None:
                        if pfi9_count_prev is not None:
                            edge_delta_since_last_line = max(
                                0, int(pfi9_count - pfi9_count_prev)
                            )
                        pfi9_count_prev = int(pfi9_count)

                if pda.use_external_trigger:
                    if np.isfinite(pfi9_rate_hz) and pfi9_rate_hz > 0:
                        trigger_eff = min(1.0, wall_line_rate_hz / pfi9_rate_hz)
                    elif expected_trigger_hz > 0:
                        trigger_eff = min(1.0, wall_line_rate_hz / expected_trigger_hz)
                    else:
                        trigger_eff = 1.0
                else:
                    trigger_eff = 1.0

                if (
                    rate_plot_enable
                    and not (rate_plot_is_pfi9 or rate_plot_is_chopper_input)
                    and np.isfinite(wall_line_rate_hz)
                ):
                    pfi9_rate_hz_buffer.append(float(wall_line_rate_hz))

                if reader is None:
                    reader_queue_depth = float("nan")
                    reader_queue_max_depth = float("nan")

                if (
                    expected_trigger_hz > 0
                    and np.isfinite(pfi9_rate_hz)
                    and pfi9_rate_hz > (1.5 * expected_trigger_hz)
                    and not pfi9_rate_warned
                ):
                    print(
                        "Warning: PFI9 edge rate is much higher than expected "
                        f"({pfi9_rate_hz:.1f} vs {expected_trigger_hz:.1f} Hz). "
                        "This can cause parity flips and pump-chop cancellation in persistent mode."
                    )
                    pfi9_rate_warned = True

                line_counter += lines_consumed
                size_changed = False
                if line.size != x.size:
                    x = pda.output_sample_axis(line.size)
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
                    if derived_axis is not None and derived_axis is not ax:
                        derived_axis.set_xlim(float(x[0]), float(x[-1]))
                    if chop_raw_line_plot is not None:
                        chop_raw_line_plot.set_xdata(x)
                    if chop_integrated_line_plot is not None:
                        chop_integrated_line_plot.set_xdata(x)
                    if chop_integrated_pairwise_dark_corrected_plot is not None:
                        chop_integrated_pairwise_dark_corrected_plot.set_xdata(x)
                    if chop_main_diag_line_plot is not None:
                        chop_main_diag_line_plot.set_xdata(x)
                    if chop_ref_diag_line_plot is not None:
                        chop_ref_diag_line_plot.set_xdata(x)
                    ax.set_xlim(float(x[0]), float(x[-1]))
                    if ax_demod is not None:
                        ax_demod.set_xlim(float(x[0]), float(x[-1]))
                    line_buffer.clear()
                    integration_sum = np.zeros_like(x, dtype=float)
                    ref_line_buffer.clear()
                    ref_integration_sum = np.zeros_like(x, dtype=float)
                    diff_line_buffer.clear()
                    diff_integration_sum = np.zeros_like(x, dtype=float)
                    chop_prev_line = None
                    chop_phase = 0
                    chop_phase0_buffer.clear()
                    chop_phase1_buffer.clear()
                    chop_phase0_sum = np.zeros_like(x, dtype=float)
                    chop_phase1_sum = np.zeros_like(x, dtype=float)
                    chop_ref_phase0_buffer.clear()
                    chop_ref_phase1_buffer.clear()
                    chop_ref_phase0_sum = np.zeros_like(x, dtype=float)
                    chop_ref_phase1_sum = np.zeros_like(x, dtype=float)
                    chop_pair_buffer.clear()
                    chop_pair_sum = np.zeros_like(x, dtype=float)
                    _reset_pairwise_dark_corrected_local()
                    chop_pair_counter = 0
                    chop_preview_flip_counter = 0
                    chop_parity_reset_counter = 0
                    latest_chop_pair = None
                    latest_chop_integrated = None
                    latest_chop_integrated_pairwise_dark_corrected = None
                    latest_chop_main_diagnostic = None
                    latest_chop_ref_diagnostic = None
                    chop_preview_template = None
                    chop_prev_phase = None
                    demod_last_line_phase = None
                    capture_score_recent.clear()
                    capture_hit_flags.clear()
                    capture_hit_rate_pct = float("nan")
                    capture_last_score_vpp = float("nan")
                    capture_last_threshold_vpp = float("nan")
                    integrated_auc_main = float("nan")
                    integrated_auc_ref = float("nan")
                    integrated_auc_diff = float("nan")
                    if (
                        pump_chop_dark_offset is not None
                        and pump_chop_dark_offset.size != line.size
                    ):
                        pump_chop_dark_offset = None
                        _set_pump_dark_status(
                            f"size mismatch after resize ({line.size})"
                        )
                        print(
                            "Cleared pump-dark offset after sample-count change."
                        )
                    if (
                        channel_dark_main_offset is not None
                        and channel_dark_main_offset.size != line.size
                    ) or (
                        channel_dark_ref_offset is not None
                        and channel_dark_ref_offset.size != line.size
                    ):
                        channel_dark_main_offset = None
                        channel_dark_ref_offset = None
                        _set_channel_dark_status(
                            f"size mismatch after resize ({line.size})"
                        )
                        _sync_channel_dark_to_runtime()
                        print(
                            "Cleared channel-dark offsets after sample-count change."
                        )
                    parity_reset_requested = True
                    parity_reset_reason = "line size changed"
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
                    if reference_processing_mode == "ratio":
                        corrected_main = _apply_channel_dark_local(
                            line,
                            channel_dark_main_offset,
                        )
                        corrected_ref = _apply_channel_dark_local(
                            ref_line,
                            channel_dark_ref_offset,
                        )
                        corrected_integrated_main = _apply_channel_dark_local(
                            integrated_line,
                            channel_dark_main_offset,
                        )
                        corrected_integrated_ref = _apply_channel_dark_local(
                            ref_integrated_line,
                            channel_dark_ref_offset,
                        )
                        diff_line = _reference_ratio_local(
                            corrected_main,
                            corrected_ref,
                        )
                        diff_integrated_line = _reference_ratio_local(
                            corrected_integrated_main,
                            corrected_integrated_ref,
                        )
                    else:
                        diff_line = line - ref_line
                        diff_integrated_line = integrated_line - ref_integrated_line
                else:
                    diff_line = None
                    diff_integrated_line = None

                integrated_auc_main = float(np.trapz(integrated_line, x=x))
                if ref_integrated_line is not None:
                    integrated_auc_ref = float(np.trapz(ref_integrated_line, x=x))
                    integrated_auc_diff = float(np.trapz(diff_integrated_line, x=x))
                else:
                    integrated_auc_ref = float("nan")
                    integrated_auc_diff = float("nan")

                if capture_hit_rate_enable:
                    capture_score = float(np.ptp(line))
                    capture_last_score_vpp = capture_score
                    capture_score_recent.append(capture_score)
                    if len(capture_score_recent) >= capture_hit_warmup_lines:
                        score_arr = np.asarray(capture_score_recent, dtype=float)
                        score_lo = float(np.quantile(score_arr, 0.10))
                        score_hi = float(np.quantile(score_arr, 0.90))
                        score_span = max(1e-12, score_hi - score_lo)
                        capture_threshold = (
                            score_lo + capture_hit_threshold_fraction * score_span
                        )
                        capture_last_threshold_vpp = capture_threshold
                        capture_hit_flags.append(capture_score >= capture_threshold)
                        capture_hit_rate_pct = (
                            100.0 * float(np.mean(capture_hit_flags))
                            if capture_hit_flags
                            else float("nan")
                        )

                demod_line = _demod_source_line_local(line, diff_line)

                if pump_chop_demod:
                    parity_reset_needed = False
                    local_reset_reason = ""
                    if parity_reset_requested:
                        parity_reset_needed = True
                        local_reset_reason = parity_reset_reason
                        parity_reset_requested = False
                        parity_reset_reason = ""

                    # In safe mode, we can qualify each accepted line by PFI9 edge
                    # count delta since the previous line. In persistent mode this
                    # mapping is ambiguous, so qualification is disabled.
                    demod_qual_active = (
                        demod_qual_requested
                        and (session is None)
                        and (
                            pfi9_monitor is not None
                            and getattr(pfi9_monitor, "available", False)
                        )
                    )
                    if demod_qual_requested and not demod_qual_active:
                        if session is not None and not demod_qual_warned_persistent:
                            print(
                                "Note: demod trigger-qualified acceptance requested, "
                                "but disabled in persistent mode "
                                "(line/edge mapping is ambiguous)."
                            )
                            demod_qual_warned_persistent = True
                        elif (
                            session is None
                            and (
                                pfi9_monitor is None
                                or not getattr(pfi9_monitor, "available", False)
                            )
                            and not demod_qual_warned_no_monitor
                        ):
                            print(
                                "Warning: demod trigger-qualified acceptance requested, "
                                "but PFI9 monitor is unavailable."
                            )
                            demod_qual_warned_no_monitor = True

                    demod_line_accepted = True
                    demod_line_phase = None
                    if demod_qual_active:
                        # Robust qualified mode:
                        # - reject only impossible/missing edge cases
                        # - allow multi-edge gaps, but advance phase by edge parity
                        #   so dropped triggers do not poison demod parity.
                        if edge_delta_since_last_line is None:
                            if demod_last_line_phase is None:
                                demod_line_phase = 0
                            else:
                                demod_line_accepted = False
                                demod_reject_no_prev_count += 1
                                local_reset_reason = (
                                    "trigger-qualified reject (no previous edge sample)"
                                )
                        elif edge_delta_since_last_line <= 0:
                            demod_line_accepted = False
                            demod_reject_missing_count += 1
                            local_reset_reason = (
                                "trigger-qualified reject (missing edge)"
                            )
                        else:
                            step_edges = int(edge_delta_since_last_line)
                            if step_edges > 1:
                                demod_multi_edge_count += 1
                            if demod_last_line_phase is None:
                                demod_line_phase = 0
                            else:
                                demod_line_phase = int(
                                    demod_last_line_phase ^ (step_edges & 1)
                                )

                        if demod_line_accepted:
                            demod_accept_count += 1
                            demod_last_line_phase = int(
                                0 if demod_line_phase is None else demod_line_phase
                            )
                        else:
                            demod_reject_count += 1
                            parity_reset_needed = True
                            latest_chop_pair = None
                            latest_chop_integrated = None

                    if parity_reset_needed:
                        chop_phase = 0
                        chop_prev_line = None
                        chop_prev_phase = None
                        demod_last_line_phase = None
                        chop_phase0_buffer.clear()
                        chop_phase1_buffer.clear()
                        chop_phase0_sum = np.zeros_like(x, dtype=float)
                        chop_phase1_sum = np.zeros_like(x, dtype=float)
                        chop_ref_phase0_buffer.clear()
                        chop_ref_phase1_buffer.clear()
                        chop_ref_phase0_sum = np.zeros_like(x, dtype=float)
                        chop_ref_phase1_sum = np.zeros_like(x, dtype=float)
                        chop_pair_buffer.clear()
                        chop_pair_sum = np.zeros_like(x, dtype=float)
                        _reset_pairwise_dark_corrected_local()
                        latest_chop_pair = None
                        latest_chop_integrated = None
                        latest_chop_integrated_pairwise_dark_corrected = None
                        latest_chop_main_diagnostic = None
                        latest_chop_ref_diagnostic = None
                        chop_preview_template = None
                        chop_parity_reset_counter += 1
                        if not chop_parity_warned:
                            print(
                                "Warning: reset chop demod parity due to trigger/line discontinuity "
                                f"({local_reset_reason})."
                            )
                            chop_parity_warned = True

                    if not demod_line_accepted:
                        # Keep raw/integrated plotting running, but do not feed
                        # rejected lines into chop demod accumulators.
                        pass
                    elif pump_chop_use_adjacent_pairs:
                        if pump_chop_sign_agnostic_preview:
                            if chop_prev_line is None:
                                chop_prev_line = demod_line.copy()
                                latest_chop_pair = None
                                latest_chop_integrated = None
                            else:
                                if reference_processing_mode == "ratio" and ref_line is not None:
                                    chop_pair = _tail_guided_reference_delta_od(
                                        demod_line,
                                        chop_prev_line,
                                    )
                                else:
                                    chop_pair = _tail_guided_pair(
                                        demod_line - chop_prev_line
                                    )
                                if (
                                    chop_preview_template is not None
                                    and np.any(np.isfinite(chop_preview_template))
                                    and not pump_chop_tail_heuristic_enable
                                ):
                                    template_dot = float(
                                        np.dot(chop_pair, chop_preview_template)
                                    )
                                    if template_dot < 0.0:
                                        chop_pair = -chop_pair
                                        chop_preview_flip_counter += 1
                                chop_pair_counter += 1
                                latest_chop_pair = chop_pair
                                _update_pairwise_dark_corrected_referenced_local(chop_pair)
                                if len(chop_pair_buffer) == chop_pair_buffer.maxlen:
                                    chop_pair_sum -= chop_pair_buffer.popleft()
                                chop_pair_buffer.append(chop_pair.copy())
                                chop_pair_sum += chop_pair
                                latest_chop_integrated = chop_pair_sum / float(
                                    len(chop_pair_buffer)
                                )
                                chop_preview_template = latest_chop_integrated.copy()
                                chop_prev_line = demod_line.copy()
                        else:
                            if actual_phase_source == "chopper_input":
                                if external_phase_state is None:
                                    latest_chop_pair = None
                                    latest_chop_integrated = None
                                    continue
                                current_phase = int(bool(external_phase_state))
                            else:
                                if demod_qual_active:
                                    current_phase = int(
                                        0 if demod_line_phase is None else demod_line_phase
                                    )
                                else:
                                    current_phase = int(
                                        chop_phase ^ ((lines_consumed - 1) & 1)
                                    )
                                    chop_phase = int(chop_phase ^ (lines_consumed & 1))
                            if chop_prev_line is None:
                                chop_prev_line = demod_line.copy()
                                chop_prev_phase = current_phase
                                latest_chop_pair = None
                                latest_chop_integrated = None
                            else:
                                make_pair = (
                                    (chop_prev_phase is None)
                                    or (current_phase != chop_prev_phase)
                                )
                                if make_pair:
                                    if (
                                        reference_processing_mode == "ratio"
                                        and ref_line is not None
                                        and actual_phase_source != "chopper_input"
                                    ):
                                        chop_pair = _tail_guided_reference_delta_od(
                                            demod_line,
                                            chop_prev_line,
                                        )
                                    elif (
                                        reference_processing_mode == "ratio"
                                        and ref_line is not None
                                    ):
                                        pumped_phase = 1 if pump_chop_sign > 0 else 0
                                        if current_phase == pumped_phase:
                                            pumped_ratio = demod_line
                                            unpumped_ratio = chop_prev_line
                                        else:
                                            pumped_ratio = chop_prev_line
                                            unpumped_ratio = demod_line
                                        chop_pair = _compute_delta_od_local(
                                            pumped_ratio,
                                            unpumped_ratio,
                                        )
                                    else:
                                        chop_pair = _tail_guided_pair(
                                            demod_line - chop_prev_line
                                        )
                                    chop_pair_counter += 1
                                    latest_chop_pair = chop_pair
                                    _update_pairwise_dark_corrected_referenced_local(chop_pair)
                                    if len(chop_pair_buffer) == chop_pair_buffer.maxlen:
                                        chop_pair_sum -= chop_pair_buffer.popleft()
                                    chop_pair_buffer.append(chop_pair.copy())
                                    chop_pair_sum += chop_pair
                                    latest_chop_integrated = chop_pair_sum / float(
                                        len(chop_pair_buffer)
                                    )
                                else:
                                    latest_chop_pair = None
                                chop_prev_line = demod_line.copy()
                                chop_prev_phase = current_phase
                    else:
                        if actual_phase_source == "chopper_input":
                            if external_phase_state is None:
                                latest_chop_pair = None
                                latest_chop_integrated = None
                                continue
                            current_phase = int(bool(external_phase_state))
                        elif demod_qual_active:
                            current_phase = int(
                                0 if demod_line_phase is None else demod_line_phase
                            )
                        else:
                            current_phase = int(
                                chop_phase ^ ((lines_consumed - 1) & 1)
                            )
                            chop_phase = int(chop_phase ^ (lines_consumed & 1))

                        if current_phase == 0:
                            if len(chop_phase0_buffer) == chop_phase0_buffer.maxlen:
                                chop_phase0_sum -= chop_phase0_buffer.popleft()
                            chop_phase0_buffer.append(line.copy())
                            chop_phase0_sum += line
                            if reference_processing_mode == "ratio" and ref_line is not None:
                                if len(chop_ref_phase0_buffer) == chop_ref_phase0_buffer.maxlen:
                                    chop_ref_phase0_sum -= chop_ref_phase0_buffer.popleft()
                                chop_ref_phase0_buffer.append(ref_line.copy())
                                chop_ref_phase0_sum += ref_line
                        else:
                            if len(chop_phase1_buffer) == chop_phase1_buffer.maxlen:
                                chop_phase1_sum -= chop_phase1_buffer.popleft()
                            chop_phase1_buffer.append(line.copy())
                            chop_phase1_sum += line
                            if reference_processing_mode == "ratio" and ref_line is not None:
                                if len(chop_ref_phase1_buffer) == chop_ref_phase1_buffer.maxlen:
                                    chop_ref_phase1_sum -= chop_ref_phase1_buffer.popleft()
                                chop_ref_phase1_buffer.append(ref_line.copy())
                                chop_ref_phase1_sum += ref_line

                        if chop_prev_line is None:
                            chop_prev_line = demod_line.copy()
                            chop_prev_phase = current_phase
                            latest_chop_pair = None
                        else:
                            make_pair = (
                                (chop_prev_phase is None)
                                or (current_phase != chop_prev_phase)
                            )
                            if make_pair:
                                if (
                                    reference_processing_mode == "ratio"
                                    and ref_line is not None
                                    and pump_chop_tail_heuristic_enable
                                    and actual_phase_source != "chopper_input"
                                ):
                                    chop_pair = _tail_guided_reference_delta_od(
                                        demod_line,
                                        chop_prev_line,
                                    )
                                elif reference_processing_mode == "ratio" and ref_line is not None:
                                    pumped_phase = 1 if pump_chop_sign > 0 else 0
                                    if current_phase == pumped_phase:
                                        pumped_ratio = demod_line
                                        unpumped_ratio = chop_prev_line
                                    else:
                                        pumped_ratio = chop_prev_line
                                        unpumped_ratio = demod_line
                                    chop_pair = _compute_delta_od_local(
                                        pumped_ratio,
                                        unpumped_ratio,
                                    )
                                else:
                                    chop_pair = _tail_guided_pair(demod_line - chop_prev_line)
                                chop_pair_counter += 1
                                latest_chop_pair = chop_pair
                                _update_pairwise_dark_corrected_referenced_local(chop_pair)
                                if pump_chop_tail_heuristic_enable:
                                    if len(chop_pair_buffer) == chop_pair_buffer.maxlen:
                                        chop_pair_sum -= chop_pair_buffer.popleft()
                                    chop_pair_buffer.append(chop_pair.copy())
                                    chop_pair_sum += chop_pair
                            else:
                                latest_chop_pair = None
                            chop_prev_line = demod_line.copy()
                            chop_prev_phase = current_phase

                        referenced_phase_delta_od = _referenced_phase_diagnostics_local()

                        if pump_chop_tail_heuristic_enable and actual_phase_source != "chopper_input":
                            if len(chop_pair_buffer) > 0:
                                latest_chop_integrated = chop_pair_sum / float(
                                    len(chop_pair_buffer)
                                )
                            else:
                                latest_chop_integrated = None
                        elif referenced_phase_delta_od is not None:
                            latest_chop_integrated = referenced_phase_delta_od
                        elif len(chop_phase0_buffer) > 0 and len(chop_phase1_buffer) > 0:
                            phase0_mean = chop_phase0_sum / float(len(chop_phase0_buffer))
                            phase1_mean = chop_phase1_sum / float(len(chop_phase1_buffer))
                            latest_chop_integrated = pump_chop_sign * (
                                phase1_mean - phase0_mean
                            )
                        else:
                            latest_chop_integrated = None

                now_for_plot = time.perf_counter()
                should_update_plot = size_changed
                if not should_update_plot:
                    should_update_plot = (
                        (line_counter % plot_update_every_n_lines == 0)
                        and (now_for_plot >= next_plot_update_t)
                    )
                if not should_update_plot:
                    continue
                next_plot_update_t = now_for_plot + plot_update_interval_s

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
                latest_chop_pair_display = _prepare_chop_display(
                    latest_chop_pair,
                    integrated_line,
                    apply_dark=False,
                )
                if (
                    chop_raw_line_plot is not None
                    and latest_chop_pair_display is not None
                ):
                    chop_raw_line_plot.set_ydata(latest_chop_pair_display)
                latest_chop_integrated_display = _prepare_chop_display(
                    latest_chop_integrated,
                    integrated_line,
                    apply_dark=True,
                    dark_role="demod",
                )
                if (
                    chop_integrated_line_plot is not None
                    and latest_chop_integrated_display is not None
                ):
                    chop_integrated_line_plot.set_ydata(
                        latest_chop_integrated_display
                    )
                latest_chop_integrated_pairwise_dark_corrected_display = _prepare_chop_display(
                    latest_chop_integrated_pairwise_dark_corrected,
                    integrated_line,
                    apply_dark=False,
                )
                if (
                    chop_integrated_pairwise_dark_corrected_plot is not None
                    and latest_chop_integrated_pairwise_dark_corrected_display is not None
                ):
                    chop_integrated_pairwise_dark_corrected_plot.set_ydata(
                        latest_chop_integrated_pairwise_dark_corrected_display
                    )
                latest_chop_main_diag_display = _prepare_chop_display(
                    latest_chop_main_diagnostic,
                    integrated_line,
                    apply_dark=True,
                    dark_role="main_diagnostic",
                )
                latest_chop_ref_diag_display = _prepare_chop_display(
                    latest_chop_ref_diagnostic,
                    ref_integrated_line,
                    apply_dark=True,
                    dark_role="ref_diagnostic",
                )
                if (
                    chop_main_diag_line_plot is not None
                    and latest_chop_main_diag_display is not None
                ):
                    chop_main_diag_line_plot.set_ydata(
                        latest_chop_main_diag_display
                    )
                if (
                    chop_ref_diag_line_plot is not None
                    and latest_chop_ref_diag_display is not None
                ):
                    chop_ref_diag_line_plot.set_ydata(
                        latest_chop_ref_diag_display
                    )

                ax.set_title(
                    "CMOS Video (Simple Mode) "
                    + f"| mode={mode_key} "
                    + (
                        f"| derived={derived_signal_label} "
                        if read_reference
                        else ""
                    )
                    + f"| st_delay={pda.st_initial_delay * 1e6:.1f} us "
                    + f"| clk_delay={pda.clk_initial_delay * 1e6:.1f} us "
                    + f"| integrated N={len(line_buffer)} "
                    + f"| svc={read_service_rate_hz:.1f} Hz "
                    + f"| wall={wall_line_rate_hz:.1f} Hz "
                    + f"| eff={100.0 * trigger_eff:.1f}%"
                    + (
                        f" | q~{pending_lines:.1f} lines"
                        if session is not None and np.isfinite(pending_lines)
                        else ""
                    )
                    + (
                        f" | fifo~{reader_queue_depth:.0f}/{reader_queue_max_depth:.0f}"
                        if reader is not None and np.isfinite(reader_queue_depth)
                        else ""
                    )
                    + (
                        f" | {rate_title_prefix}~{pfi9_rate_hz:.1f} Hz"
                        if np.isfinite(pfi9_rate_hz)
                        else ""
                    )
                    + (
                        f" | chop_pairs={chop_pair_counter}"
                        if pump_chop_demod
                        else ""
                    )
                    + (
                        f" | phase={pda.chopper_input_term.split('/')[-1]}"
                        if (pump_chop_demod and actual_phase_source == "chopper_input")
                        else ""
                    )
                    + (
                        f" | tailflips={tail_guided_flip_counter} tail={tail_guided_last_mean:+.3g}"
                        if (pump_chop_demod and pump_chop_tail_heuristic_enable)
                        else ""
                    )
                    + (
                        f" | hit={capture_hit_rate_pct:.1f}%"
                        if capture_hit_rate_enable and np.isfinite(capture_hit_rate_pct)
                        else ""
                    )
                )
                if ax_demod is not None:
                    ax_demod.set_title(_format_demod_title())
                if line_counter == 1 or (
                    line_counter % timing_text_update_every_n_lines == 0
                ):
                    d = pda.get_timing_diagnostics(
                        trigger_frequency_hz=expected_trigger_hz
                    )
                    pfi9_status_line = _rate_status_line()
                    pfi9_med = (
                        float(np.median(pfi9_rate_hz_buffer))
                        if pfi9_rate_hz_buffer
                        else float("nan")
                    )
                    queue_med = (
                        float(np.median(pending_lines_buffer))
                        if (session is not None and pending_lines_buffer)
                        else float("nan")
                    )
                    timing_lines = [
                        f"Mode={mode_badge_label} Video={mode_key} N={len(line_buffer)}",
                        (
                            f"Svc/Wall={read_service_rate_hz:.1f}/"
                            f"{wall_line_rate_hz:.1f} Hz  "
                            f"Eff={100.0 * trigger_eff:.1f}%"
                        ),
                        pfi9_status_line,
                        f"AUC main={integrated_auc_main:.6g} V*s",
                    ]
                    if np.isfinite(integrated_auc_ref):
                            timing_lines.append(
                                f"AUC ref/{derived_signal_label}="
                                f"{integrated_auc_ref:.6g}/"
                                f"{integrated_auc_diff:.6g} {derived_signal_units}*s"
                            )
                    timing_lines.append(
                        (
                            f"ST rise/fall={d['st_rise_s'] * 1e6:.1f}/"
                            f"{d['st_fall_s'] * 1e6:.1f} us  "
                            f"CLK start={d['clk_start_s'] * 1e6:.1f} us"
                        )
                    )
                    margin_us = (
                        d["timing_margin_s"] * 1e6
                        if d["timing_margin_s"] is not None
                        else float("nan")
                    )
                    timing_lines.append(
                        (
                            f"AI start={d['ai_start_event_s'] * 1e6:.1f} us  "
                            f"Read={d['samples_per_line_read']}  "
                            f"Margin={margin_us:.1f} us"
                        )
                    )
                    if np.isfinite(pfi9_med):
                        pfi9_line = f"{rate_med_label}={pfi9_med:.1f} Hz"
                        if rate_plot_is_pfi9 and expected_trigger_hz > 0:
                            pfi9_line += (
                                f" ({pfi9_med / expected_trigger_hz:.2f}x)"
                            )
                        timing_lines.append(pfi9_line)
                    if np.isfinite(queue_med):
                        timing_lines.append(
                            f"AI queue med/max={queue_med:.1f}/{max_pending_lines:.1f}"
                        )
                    if reader is not None and np.isfinite(reader_queue_depth):
                        timing_lines.append(
                            f"Reader FIFO now/max={reader_queue_depth:.0f}/{reader_queue_max_depth:.0f}"
                        )
                    if use_fast_session and retrigger_latest_only_read:
                        timing_lines.append(
                            f"Skip ev/lines={latest_multiline_read_events}/"
                            f"{latest_skipped_lines_total}"
                        )
                    if (
                        trigger_budget_margin_s is not None
                        and trigger_budget_limited_s is not None
                    ):
                        timing_lines.append(
                            f"Budget margin={trigger_budget_margin_s * 1e6:.1f} us"
                        )
                    if capture_hit_rate_enable:
                        if np.isfinite(capture_hit_rate_pct):
                            timing_lines.append(
                                f"Hit={capture_hit_rate_pct:.1f}%  "
                                f"Vpp={capture_last_score_vpp:.4g}/"
                                f"{capture_last_threshold_vpp:.4g}"
                            )
                        else:
                            timing_lines.append(
                                f"Hit warmup {len(capture_score_recent)}/"
                                f"{capture_hit_warmup_lines}"
                            )
                    if pump_chop_demod:
                        chop_line = (
                            f"Chop {demod_mode_label} sign={pump_chop_sign:+.0f}"
                        )
                        if actual_phase_source == "chopper_input":
                            chop_line += (
                                f" phase={pda.chopper_input_term.split('/')[-1]}"
                            )
                        if pump_chop_use_adjacent_pairs:
                            chop_line += f" pairs={len(chop_pair_buffer)}"
                            if pump_chop_sign_agnostic_preview:
                                chop_line += f" flips={chop_preview_flip_counter}"
                        else:
                            chop_line += (
                                f" ph0/ph1={len(chop_phase0_buffer)}/"
                                f"{len(chop_phase1_buffer)}"
                            )
                        chop_line += f" resets={chop_parity_reset_counter}"
                        timing_lines.append(chop_line)
                        timing_lines.append(f"Dark={pump_chop_dark_status}")
                        if channel_dark_subtract:
                            timing_lines.append(
                                f"{channel_dark_label}={channel_dark_status}"
                            )
                        if latest_chop_integrated_display is not None:
                            chop_rms, chop_pp = _trace_rms_pp(
                                latest_chop_integrated_display
                            )
                            timing_lines.append(
                                f"Chop RMS/PP={chop_rms:.4g}/{chop_pp:.4g} {chop_signal_units}"
                            )
                        timing_lines.append(
                            "Keys: d save diff-dark | l load | x clear | c save intensity/chan-dark | v load | n clear | s flip | +/- sign | q/esc stop"
                        )
                    timing_text.set_text("\n".join(timing_lines))

                if ax_trig is not None and trig_rate_plot is not None and pfi9_rate_hz_buffer:
                    trig_arr = np.asarray(pfi9_rate_hz_buffer, dtype=float)
                    trig_rate_plot.set_xdata(np.arange(trig_arr.size))
                    trig_rate_plot.set_ydata(trig_arr)
                    ax_trig.set_xlim(0, max(1, trig_arr.size - 1))

                if plot_update_counter % autoscale_every_n_plot_updates == 0:
                    ax.relim(visible_only=True)
                    ax.autoscale_view(scalex=False, scaley=True)
                    if derived_axis is not None and derived_axis is not ax:
                        derived_axis.relim(visible_only=True)
                        derived_axis.autoscale_view(scalex=False, scaley=True)
                    if ax_demod is not None:
                        ax_demod.relim(visible_only=True)
                        ax_demod.autoscale_view(scalex=False, scaley=True)
                    if ax_trig is not None and trig_rate_plot is not None and pfi9_rate_hz_buffer:
                        ax_trig.relim(visible_only=True)
                        ax_trig.autoscale_view(scalex=False, scaley=True)
                fig.canvas.draw_idle()
                plt.pause(ui_idle_pause_s)

    except KeyboardInterrupt:
        stop_requested = True
        stop_reason = "keyboard interrupt"
        print("\nStopped continuous acquisition.")
    finally:
        if processor is not None:
            try:
                processor.close()
            except Exception:
                pass
        if reader is not None:
            try:
                reader.close()
            except Exception:
                pass
        if 'session' in locals() and session is not None:
            try:
                session.close()
            except Exception:
                pass
        pda.video_main = original_video_main
        plt.ioff()
        try:
            plt.close(fig)
        except Exception:
            pass


def _build_cli_parser():
    """Build command-line parser for live and sweep operations."""
    cli = argparse.ArgumentParser(description="Simple live PDA/CMOS DAQ runner.")
    cli.add_argument(
        "--operation",
        default="live",
        choices=("live", "sweep"),
        help="Run live plotting or automated persistent timing sweep.",
    )
    cli.add_argument(
        "--timing-profile",
        default=PDAControllerDAQSimple.DEFAULT_TIMING_PROFILE,
        choices=sorted(PDAControllerDAQSimple.TIMING_PROFILES.keys()),
        help="Select named timing profile.",
    )
    cli.add_argument(
        "--runtime-profile",
        default=RUNTIME_PROFILE_DEFAULT,
        choices=RUNTIME_PROFILE_CHOICES,
        help=(
            "Acquisition runtime profile. "
            "'persistent_robust_test' is the default retriggered mode."
        ),
    )
    cli.add_argument(
        "--preset",
        default=LIVE_PRESET_DEFAULT,
        choices=LIVE_PRESET_CHOICES,
        help=(
            "Quick UX preset. "
            "'signal_only' disables pump-chop demod; "
            "'trigger_debug' emphasizes trigger diagnostics."
        ),
    )
    cli.add_argument(
        "--list-timing-profiles",
        action="store_true",
        help="Print available timing profiles and exit.",
    )
    cli.add_argument(
        "--plot-fps",
        type=float,
        default=None,
        help="Live plot target FPS override (default from runtime profile).",
    )
    cli.add_argument(
        "--plot-every-lines",
        type=int,
        default=None,
        help="Update plot every N acquired lines (default from runtime profile).",
    )
    cli.add_argument(
        "--timing-text-every-lines",
        type=int,
        default=None,
        help="Refresh diagnostics text every N lines (default from runtime profile).",
    )
    cli.add_argument(
        "--autoscale-every-updates",
        type=int,
        default=None,
        help="Autoscale every N plot updates (default from runtime profile).",
    )
    cli.add_argument(
        "--reader-fifo-packets",
        type=int,
        default=None,
        help=(
            "Max packets buffered in the decoupled retrigger FIFO reader "
            "(default from runtime profile)."
        ),
    )
    cli.add_argument(
        "--sweep-evaluation-seconds",
        type=float,
        default=6.0,
        help="Per-point evaluation duration in seconds for sweep mode.",
    )
    cli.add_argument(
        "--sweep-phase-coarse-start-us",
        type=float,
        default=0.0,
        help="Coarse sweep start for trigger->ST phase shift (us).",
    )
    cli.add_argument(
        "--sweep-phase-coarse-stop-us",
        type=float,
        default=1000.0,
        help="Coarse sweep stop for trigger->ST phase shift (us).",
    )
    cli.add_argument(
        "--sweep-phase-coarse-step-us",
        type=float,
        default=25.0,
        help="Coarse sweep step for trigger->ST phase shift (us).",
    )
    cli.add_argument(
        "--sweep-phase-fine-half-width-us",
        type=float,
        default=50.0,
        help="Fine sweep half-width around best coarse phase (us).",
    )
    cli.add_argument(
        "--sweep-phase-fine-step-us",
        type=float,
        default=5.0,
        help="Fine sweep step for trigger->ST phase shift (us).",
    )
    cli.add_argument(
        "--sweep-st-delay-half-width-us",
        type=float,
        default=100.0,
        help="ST initial-delay fine sweep half-width (us).",
    )
    cli.add_argument(
        "--sweep-st-delay-step-us",
        type=float,
        default=10.0,
        help="ST initial-delay fine sweep step (us).",
    )
    cli.add_argument(
        "--sweep-ai-buffer-lines",
        type=int,
        default=2048,
        help="AI buffer lines used in persistent sweep evaluation.",
    )
    cli.add_argument(
        "--sweep-queue-abort-fraction",
        type=float,
        default=0.80,
        help="Abort a candidate if queue exceeds this fraction of buffer.",
    )
    cli.add_argument(
        "--sweep-then-live",
        action="store_true",
        help="After sweep, continue into live plot with best settings.",
    )
    return cli


def _resolve_runtime_profile_settings(profile_name):
    """
    Return acquisition/runtime defaults for the selected runtime profile.

    The returned mapping includes DAQ session mode, read mode, and live-plot
    cadence defaults.
    """
    profile = str(profile_name).strip().lower()
    if profile == RUNTIME_PROFILE_PERSISTENT_LATEST:
        return {
            "use_persistent_session": True,
            "retrigger_latest_only_read": True,
            "retrigger_overwrite_unread": True,
            "decouple_acquisition_from_plot": True,
            "reader_fifo_max_packets": 64,
            "demod_trigger_qualified_acceptance": False,
            "plot_update_every_n_lines": 25,
            "plot_target_fps": 10.0,
            "timing_text_update_every_n_lines": 200,
            "autoscale_every_n_plot_updates": 12,
            "persistent_ai_buffer_lines": 2048,
        }
    if profile == RUNTIME_PROFILE_PERSISTENT_ROBUST:
        return {
            "use_persistent_session": True,
            "retrigger_latest_only_read": False,
            "retrigger_overwrite_unread": False,
            "decouple_acquisition_from_plot": True,
            "reader_fifo_max_packets": 256,
            "demod_trigger_qualified_acceptance": False,
            "plot_update_every_n_lines": 2,
            "plot_target_fps": 25.0,
            "timing_text_update_every_n_lines": 40,
            "autoscale_every_n_plot_updates": 8,
            "persistent_ai_buffer_lines": 2048,
        }
    if profile == RUNTIME_PROFILE_SAFE:
        return {
            "use_persistent_session": False,
            "retrigger_latest_only_read": False,
            "retrigger_overwrite_unread": False,
            "decouple_acquisition_from_plot": False,
            "reader_fifo_max_packets": 64,
            "demod_trigger_qualified_acceptance": True,
            "plot_update_every_n_lines": 1,
            "plot_target_fps": 15.0,
            "timing_text_update_every_n_lines": 100,
            "autoscale_every_n_plot_updates": 8,
            "persistent_ai_buffer_lines": 256,
        }
    raise ValueError(
        "Unknown acquisition runtime profile. "
        f"Use one of: {', '.join(RUNTIME_PROFILE_CHOICES)}"
    )


def _apply_live_preset(live_cfg, preset_name):
    """
    Apply a lightweight usage preset to live configuration.

    Presets intentionally only touch a small subset of user-facing options.
    """
    preset = str(preset_name).strip().lower()
    if preset in ("", LIVE_PRESET_DEFAULT):
        return
    if preset == LIVE_PRESET_SIGNAL_ONLY:
        # Simplest viewing mode: raw/integrated main line only.
        live_cfg["live_video_mode"] = "main"
        live_cfg["pump_chop_demod"] = False
        live_cfg["plot_raw_line"] = True
        return
    if preset == LIVE_PRESET_TRIGGER_DEBUG:
        # Emphasize trigger diagnostics while keeping acquisition unchanged.
        live_cfg["pump_chop_demod"] = False
        live_cfg["plot_raw_line"] = True
        live_cfg["timing_text_update_every_n_lines"] = min(
            int(live_cfg["timing_text_update_every_n_lines"]), 20
        )
        live_cfg["trigger_plot_history"] = max(int(live_cfg["trigger_plot_history"]), 1200)
        return
    raise ValueError(
        f"Unknown live preset '{preset_name}'. Use one of: {', '.join(LIVE_PRESET_CHOICES)}"
    )


def main():
    """Entry point for live run and sweep workflows."""
    cli = _build_cli_parser()
    cli_args = cli.parse_args()
    if cli_args.list_timing_profiles:
        print("Available timing profiles:")
        for _name in sorted(PDAControllerDAQSimple.TIMING_PROFILES.keys()):
            _desc = PDAControllerDAQSimple.TIMING_PROFILES[_name].get("description", "")
            print(f"  {_name}: {_desc}")
        raise SystemExit(0)

    # -----------------------------
    # Controller and trigger setup
    # -----------------------------
    pda = PDAControllerDAQSimple(device="Dev1", num_pixels=1024)
    pda.enable_external_trigger(True)

    trigger_filter_enable = False
    trigger_filter_min_pulse_width_s = 0.2e-6
    trigger_sync_enable = True
    pda.set_trigger_filter(
        enable=trigger_filter_enable,
        min_pulse_width_s=trigger_filter_min_pulse_width_s,
    )
    pda.set_trigger_sync(trigger_sync_enable)
    pda.set_retrigger_initial_delay(True)
    pda.set_chopper_sync_output(
        enable=True,
        out_pfi="PFI3",
        source_terminal=pda.trig_in,
        initial_delay_ticks=0,
        high_ticks=2,
        low_ticks=2,
    )

    # Video timing: sample from first CLK edge after ST falls and keep 14 clocks
    # of pre-pixel region visible for timing diagnostics.
    pda.set_video_timing(
        dummy_clocks=14,
        pixel_clocks=pda.num_pixels,
        start_on_st_fall=True,
    )
    pda.set_output_cropping(False)

    timing_profile_name = cli_args.timing_profile
    pda.apply_timing_profile(timing_profile_name)
    print(f"Timing profile: {timing_profile_name}")
    trigger_to_st_phase_shift_us = 0.0
    pda.set_trigger_phase_shift(trigger_to_st_phase_shift_us * 1e-6)
    print(f"Trigger->ST phase shift: {trigger_to_st_phase_shift_us:.1f} us")

    expected_trigger_hz = 1000.0
    acquisition_timeout_s = 50.0
    acquisition_runtime_profile = str(cli_args.runtime_profile).strip().lower()
    runtime_settings = _resolve_runtime_profile_settings(acquisition_runtime_profile)

    # Consolidated live-run configuration. Keeping this in one mapping makes it
    # easier to reason about active behavior and pass through to run_live_plot().
    live_cfg = {
        "integration_line_count": 128,
        "plot_raw_line": True,
        "live_video_mode": "main",
        "pump_chop_demod": True,
        "pump_chop_sign": -1.0,
        # Default to the simplest ABAB assumption: odd/even accepted-line buckets.
        "pump_chop_use_adjacent_pairs": False,
        "pump_chop_sign_agnostic_preview": False,
        "expected_trigger_hz": expected_trigger_hz,
        "acquisition_timeout_s": acquisition_timeout_s,
        "monitor_pfi9": True,
        "pfi9_monitor_counter": "ctr2",
        "pfi9_rate_gate_s": 0.002,
        "trigger_plot_history": 1200,
        "capture_hit_rate_enable": True,
        "capture_hit_rate_window_lines": 256,
        "capture_hit_threshold_fraction": 0.45,
        "capture_hit_warmup_lines": 64,
        "ordered_read_batch_lines": 16,
        "tdms_log_enable": False,
        "tdms_group_name": "PDA",
        "tdms_logging_mode": LoggingMode.LOG_AND_READ,
        "tdms_logging_operation": LoggingOperation.OPEN_OR_CREATE,
    }
    live_cfg.update(runtime_settings)

    # Apply optional lightweight UX preset.
    _apply_live_preset(live_cfg, cli_args.preset)

    # Optional live plotting cadence overrides.
    if cli_args.plot_fps is not None:
        live_cfg["plot_target_fps"] = max(1.0, float(cli_args.plot_fps))
    if cli_args.plot_every_lines is not None:
        live_cfg["plot_update_every_n_lines"] = max(1, int(cli_args.plot_every_lines))
    if cli_args.timing_text_every_lines is not None:
        live_cfg["timing_text_update_every_n_lines"] = max(
            1, int(cli_args.timing_text_every_lines)
        )
    if cli_args.autoscale_every_updates is not None:
        live_cfg["autoscale_every_n_plot_updates"] = max(
            1, int(cli_args.autoscale_every_updates)
        )
    if cli_args.reader_fifo_packets is not None:
        live_cfg["reader_fifo_max_packets"] = max(
            8, int(cli_args.reader_fifo_packets)
        )

    tdms_log_dir = Path(__file__).resolve().parent / "acquisition_results"
    tdms_log_dir.mkdir(parents=True, exist_ok=True)
    live_cfg["tdms_file_path"] = str(
        tdms_log_dir / f"pda_retrigger_{time.strftime('%Y%m%d_%H%M%S')}.tdms"
    )

    # -----------------------------
    # Optional sweep path
    # -----------------------------
    if cli_args.operation == "sweep":
        print("\n=== Persistent Sweep Mode ===")
        print(
            "Sweep enforces robust persistent settings: "
            "external trigger on, main-only, demod off, "
            "latest_only=False, overwrite_unread=False."
        )
        live_cfg.update(
            {
                "live_video_mode": "main",
                "pump_chop_demod": False,
                "demod_trigger_qualified_acceptance": False,
                "use_persistent_session": True,
                "retrigger_latest_only_read": False,
                "retrigger_overwrite_unread": False,
                "decouple_acquisition_from_plot": True,
                "tdms_log_enable": False,
            }
        )

        run_persistent_timing_sweep(
            pda=pda,
            expected_trigger_hz=live_cfg["expected_trigger_hz"],
            evaluation_seconds=cli_args.sweep_evaluation_seconds,
            acquisition_timeout_s=live_cfg["acquisition_timeout_s"],
            monitor_pfi9=live_cfg["monitor_pfi9"],
            pfi9_monitor_counter=live_cfg["pfi9_monitor_counter"],
            pfi9_rate_gate_s=live_cfg["pfi9_rate_gate_s"],
            phase_coarse_start_us=cli_args.sweep_phase_coarse_start_us,
            phase_coarse_stop_us=cli_args.sweep_phase_coarse_stop_us,
            phase_coarse_step_us=cli_args.sweep_phase_coarse_step_us,
            phase_fine_half_width_us=cli_args.sweep_phase_fine_half_width_us,
            phase_fine_step_us=cli_args.sweep_phase_fine_step_us,
            st_delay_half_width_us=cli_args.sweep_st_delay_half_width_us,
            st_delay_step_us=cli_args.sweep_st_delay_step_us,
            ai_buffer_lines=cli_args.sweep_ai_buffer_lines,
            queue_abort_fraction=cli_args.sweep_queue_abort_fraction,
        )
        if not cli_args.sweep_then_live:
            raise SystemExit(0)
        print("\nSweep finished. Entering live mode with best settings...\n")

    run_live_plot(pda=pda, **live_cfg)


if __name__ == "__main__":
    main()
