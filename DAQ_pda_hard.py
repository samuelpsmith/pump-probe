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
    Level,
    TerminalConfiguration,
    LoggingMode,
    LoggingOperation,
    OverwriteMode,
    ReadRelativeTo,
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

    # Named profiles make it explicit which timing set is active.
    TIMING_PROFILES = {
        "main_1khz_safe": MAIN_1KHZ_SAFE_TIMING,
        "txt_1khz_rebased": TXT_1KHZ_REBASED_TIMING,
        "initial_guess": INITIAL_GUESS_TIMING,
        "improved_guess": IMPROVED_GUESS_TIMING,
        "improved_guess_2channel": IMPROVED_GUESS_TIMING_2CHANNEL,
        "toofast_guess": TOOFAST_GUESS_TIMING,
        "guard_10us": GUARD_10US_TIMING,
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
        # Keep profile/explicit ST delay and trigger phase shift separate.
        # Effective ST initial delay used by hardware is:
        #   base_st_initial_delay + trigger_phase_shift_s
        self.base_st_initial_delay = 0.0
        self.trigger_phase_shift_s = 0.0
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
        self.trigger_sync_enable = True
        self._trigger_sync_warned = False
        self.retrigger_enable_initial_delay = True
        self._retrigger_delay_warned = False

        # Video timing model:
        # - AI starts on ST falling edge by default.
        # - Sampling begins on first CLK rising edge after that edge.
        # - First `video_dummy_clocks` samples are pre-video clocks, then pixels.
        self.ai_start_trigger_edge = Edge.FALLING
        self.video_dummy_clocks = 14

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
          Must currently match num_pixels for simple cropping semantics.
        """
        dummy = int(dummy_clocks)
        pixels = int(self.num_pixels if pixel_clocks is None else pixel_clocks)
        if dummy < 0:
            raise ValueError("dummy_clocks must be >= 0.")
        if pixels <= 0:
            raise ValueError("pixel_clocks must be > 0.")
        if pixels != int(self.num_pixels):
            raise ValueError(
                "pixel_clocks must match num_pixels in this simple runner."
            )

        self.video_dummy_clocks = dummy
        self.ai_start_trigger_edge = (
            Edge.FALLING if bool(start_on_st_fall) else Edge.RISING
        )
        # Keep dummy clocks in front of the valid-pixel region so optional
        # cropping can return exactly num_pixels when enabled.
        self.set_capture_window_samples(
            total_samples=(self.num_pixels + dummy),
            ignored_samples=dummy,
        )

    @property
    def clk_rate(self):
        return 1.0 / (self.clk_high_time + self.clk_low_time)

    @property
    def st_period_s(self):
        return float(self.st_high_time + self.st_low_time)

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

    @property
    def pre_ai_clock_count(self):
        capture = self._compute_capture_timing()
        pre = (
            (capture["first_sample_s"] - capture["clk_start_s"])
            / capture["clk_period_s"]
        )
        return max(0, int(round(pre)))

    @property
    def clk_pulses_per_line(self):
        return int(self.ai_samples_per_line + self.pre_ai_clock_count)

    def _compute_capture_timing(self):
        clk_period_s = 1.0 / self.clk_rate
        st_rise_s = self.st_initial_delay
        st_fall_s = self.st_initial_delay + self.st_high_time
        clk_start_s = self.st_initial_delay + self.clk_initial_delay

        # AI starts from ST internal edge, then samples on the first CLK rising
        # edge that occurs after that AI trigger edge.
        ai_start_event_s = (
            st_fall_s
            if self.ai_start_trigger_edge == Edge.FALLING
            else st_rise_s
        )
        strict_after = self.ai_start_trigger_edge == Edge.FALLING
        if ai_start_event_s < clk_start_s:
            first_sample_s = clk_start_s
        else:
            rel = (ai_start_event_s - clk_start_s) / clk_period_s
            if strict_after:
                n_edges = int(np.floor(rel + 1e-12)) + 1
            else:
                n_edges = int(np.ceil(rel - 1e-12))
            first_sample_s = clk_start_s + max(0, n_edges) * clk_period_s

        capture_window_s = self.sample_window_s
        capture_end_s = first_sample_s + capture_window_s
        return {
            "clk_period_s": clk_period_s,
            "st_rise_s": st_rise_s,
            "st_fall_s": st_fall_s,
            "clk_start_s": clk_start_s,
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
            "st_high_s": self.st_high_time,
            "st_low_s": self.st_low_time,
            "st_period_s": st_period_s,
            "st_initial_delay_s": self.st_initial_delay,
            "clk_initial_delay_s": self.clk_initial_delay,
            "sample_window_s": sample_window_s,
            "total_trigger_to_end_s": total_trigger_to_end_s,
            "limiting_cycle_s": limiting_cycle_s,
            "pre_ai_clock_count": self.pre_ai_clock_count,
            "clk_pulses_per_line": self.clk_pulses_per_line,
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
        dummy_clock_span_s = float(self.video_dummy_clocks) * clk_period_s
        video_valid_start_s = capture_start_s + dummy_clock_span_s
        video_valid_end_s = (
            video_valid_start_s + max(0, self.num_pixels - 1) * clk_period_s
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
            "video_valid_start_s": video_valid_start_s,
            "video_valid_end_s": video_valid_end_s,
            "pre_ai_clock_count": int(self.pre_ai_clock_count),
            "clk_pulses_per_line": int(self.clk_pulses_per_line),
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
                f"AI start edge: {d['ai_start_edge_label']} "
                f"@ {d['ai_start_event_s'] * 1e6:.1f} us"
            ),
            (
                "Capture start (first CLK after AI edge): "
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
                f"(pre-AI clocks: {d['pre_ai_clock_count']})"
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
            samps_per_chan=self.clk_pulses_per_line,
        )
        return clk_task

    def _apply_start_trigger_chain(self, ai_task, st_task, clk_task):
        ai_task.triggers.start_trigger.cfg_dig_edge_start_trig(
            trigger_source=self.st_internal_output,
            trigger_edge=self.ai_start_trigger_edge,
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
        for task_obj, task_label in ((self.st_task, "ST"), (self.clk_task, "CLK")):
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

    def __init__(self, pda, counter="ctr2", rate_gate_s=0.05):
        self.pda = pda
        self.counter = f"{self.pda.device}/{str(counter).lstrip('/')}"
        # Allow short gates for higher-rate trigger diagnostics.
        # This is an edge-rate monitor (not a waveform monitor), so shorter gates
        # increase temporal sensitivity at the cost of noisier quantization.
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
            self.task = nidaqmx.Task("PDA_PFI9_MON")
            ch = self.task.ci_channels.add_ci_count_edges_chan(
                counter=self.counter,
                edge=Edge.RISING,
                initial_count=0,
            )
            self.task.ci_channels[0].ci_count_edges_term = self.pda.trig_in
            # PFI terminals are shared resources: if another task on this terminal
            # enabled a digital filter, NI-DAQmx requires matching filter settings.
            try:
                ch.ci_count_edges_dig_fltr_enable = bool(self.pda.trigger_filter_enable)
                if self.pda.trigger_filter_enable:
                    ch.ci_count_edges_dig_fltr_min_pulse_width = float(
                        self.pda.trigger_filter_min_pulse_width_s
                    )
            except Exception:
                # Non-fatal: if unsupported, continue without explicit CI filter setup.
                pass
            try:
                ch.ci_count_edges_dig_sync_enable = bool(self.pda.trigger_sync_enable)
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

        # Compute edge rate over a fixed gate interval to avoid inflated values
        # when the caller polls quickly (e.g., in persistent fast mode).
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


class _BackgroundLineReader:
    """
    Background DAQ line reader.

    Reads lines continuously in a worker thread and keeps the latest packet
    plus cumulative counters, so plotting can run at its own pace.
    """

    def __init__(self, read_fn):
        self.read_fn = read_fn
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._latest = None
        self._error = None
        self._seq = 0
        self._lines_total = 0
        self._acq_total_s = 0.0

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
                with self._lock:
                    self._error = exc
                return

            elapsed_s = max(0.0, time.perf_counter() - t0)
            consumed = max(1, int(lines_consumed))
            with self._lock:
                self._seq += 1
                self._lines_total += consumed
                self._acq_total_s += elapsed_s
                self._latest = {
                    "seq": int(self._seq),
                    "data": data,
                    "lines_total": int(self._lines_total),
                    "acq_total_s": float(self._acq_total_s),
                }

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
    code = getattr(exc, "error_code", None)
    if code == -200222:
        return True
    text = str(exc).lower()
    return ("-200222" in text) or ("input buffer overwrite" in text)


def _metric_score(auc_median, auc_std, trigger_eff, valid):
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
    pump_chop_demod=False,
    pump_chop_sign=1.0,
    pump_chop_use_adjacent_pairs=False,
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
    capture_hit_rate_enable=True,
    capture_hit_rate_window_lines=256,
    capture_hit_threshold_fraction=0.45,
    capture_hit_warmup_lines=64,
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
    pump_chop_demod = bool(pump_chop_demod)
    pump_chop_sign = float(pump_chop_sign)
    pump_chop_use_adjacent_pairs = bool(pump_chop_use_adjacent_pairs)
    demod_trigger_qualified_acceptance = bool(demod_trigger_qualified_acceptance)
    retrigger_latest_only_read = bool(retrigger_latest_only_read)
    retrigger_overwrite_unread = bool(retrigger_overwrite_unread)
    tdms_log_enable = bool(tdms_log_enable)
    decouple_acquisition_from_plot = bool(decouple_acquisition_from_plot)
    persistent_ai_buffer_lines = max(64, int(persistent_ai_buffer_lines))
    plot_target_fps = max(1.0, float(plot_target_fps))
    capture_hit_rate_enable = bool(capture_hit_rate_enable)
    capture_hit_rate_window_lines = max(16, int(capture_hit_rate_window_lines))
    capture_hit_warmup_lines = max(8, int(capture_hit_warmup_lines))
    capture_hit_threshold_fraction = float(capture_hit_threshold_fraction)
    capture_hit_threshold_fraction = min(
        1.0, max(0.0, capture_hit_threshold_fraction)
    )
    demod_mode_label = (
        "adjacent pairs"
        if pump_chop_use_adjacent_pairs
        else "phase buckets"
    )
    if pump_chop_sign == 0:
        raise ValueError("pump_chop_sign must be non-zero.")
    read_reference = mode_key == "both"
    ref_only = mode_key == "ref"
    pda._validate_scan_rate(num_ai_channels=(2 if read_reference else 1))

    x = np.arange(pda.output_samples_per_line)

    plt.ion()
    if pump_chop_demod and monitor_pfi9:
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
    elif monitor_pfi9:
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
    if ax_demod is not None:
        chop_raw_line_plot, = ax_demod.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.0,
            alpha=0.45,
            color="tab:purple",
            label="Chop pair raw (adjacent)",
        )
        chop_integrated_line_plot, = ax_demod.plot(
            x,
            np.zeros_like(x, dtype=float),
            linewidth=1.9,
            color="tab:purple",
            label=f"Chop integrated ({integration_line_count} lines/phase)",
        )
        ax_demod.axhline(
            0.0,
            color="black",
            linewidth=0.8,
            alpha=0.6,
        )
        if not plot_raw_line:
            chop_raw_line_plot.set_visible(False)
        ax_demod.set_title(
            f"Pump-Chop Difference ({demod_mode_label}, sign {pump_chop_sign:+.0f})"
        )
        ax_demod.set_ylabel("Delta V (V)")
        ax_demod.set_xlabel("Sample Index")
        ax_demod.set_xlim(0, max(1, x.size - 1))
        ax_demod.grid(True, alpha=0.3)
        ax_demod.legend(loc="upper right")
    else:
        chop_raw_line_plot = None
        chop_integrated_line_plot = None
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
    print(
        "Pump chop demod: "
        f"{pump_chop_demod} "
        + (
            f"(adjacent mode: {pump_chop_sign:+.0f} * (line[n]-line[n-1]))"
            if pump_chop_use_adjacent_pairs
            else f"(phase mode: {pump_chop_sign:+.0f} * (phase1-phase0))"
        )
    )
    print(
        "Demod trigger-qualified acceptance: "
        f"{demod_trigger_qualified_acceptance}"
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
    )
    print(
        f"Live plot target FPS: {plot_target_fps:.1f} "
        f"(line-gate every {max(1, int(plot_update_every_n_lines))} lines)"
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
        "Video timing: "
        f"AI edge={('ST falling' if pda.ai_start_trigger_edge == Edge.FALLING else 'ST rising')}, "
        f"dummy_clocks={pda.video_dummy_clocks}, "
        f"pixel_clocks={pda.num_pixels}"
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
    chop_prev_line = None
    chop_phase = 0
    chop_phase0_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_phase1_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_phase0_sum = np.zeros_like(x, dtype=float)
    chop_phase1_sum = np.zeros_like(x, dtype=float)
    chop_pair_buffer = deque(maxlen=max(1, int(integration_line_count)))
    chop_pair_sum = np.zeros_like(x, dtype=float)
    chop_pair_counter = 0
    chop_parity_reset_counter = 0
    latest_chop_pair = None
    latest_chop_integrated = None
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

    original_video_main = pda.video_main
    reader = None
    if ref_only:
        # Repoint single-channel reads to the configured reference input.
        pda.video_main = pda.video_ref

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
        monitor_context = (
            _PFI9EdgeMonitorSession(
                pda,
                counter=pfi9_monitor_counter,
                rate_gate_s=pfi9_rate_gate_s,
            )
            if monitor_pfi9
            else nullcontext(None)
        )
        with session_context as session, monitor_context as pfi9_monitor:
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
            reader = None
            reader_seq_last = 0
            reader_lines_total_last = 0
            reader_acq_total_last_s = 0.0
            if session is not None and decouple_acquisition_from_plot:
                def _reader_pull():
                    fast_data = session.read_line(timeout=acquisition_timeout_s)
                    fast_lines = int(getattr(session, "last_lines_consumed", 1))
                    return fast_data, max(1, fast_lines)

                reader = _BackgroundLineReader(_reader_pull).start()
                print(
                    "Background reader enabled: DAQ acquisition decoupled "
                    "from plotting loop."
                )
            while True:
                pending_lines = float("nan")
                if session is not None and reader is None:
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

                data = None
                lines_consumed = 1
                acq_elapsed_s = 0.0
                try:
                    if reader is not None:
                        packet, reader_exc = reader.snapshot()
                        if reader_exc is not None:
                            raise reader_exc
                        if packet is None or int(packet["seq"]) <= int(reader_seq_last):
                            if (
                                pfi9_monitor is not None
                                and getattr(pfi9_monitor, "available", False)
                            ):
                                pfi9_rate_idle, _ = pfi9_monitor.read_rate()
                                if np.isfinite(pfi9_rate_idle):
                                    pfi9_rate_hz_buffer.append(float(pfi9_rate_idle))
                            plt.pause(0.001)
                            continue

                        reader_seq_last = int(packet["seq"])
                        data = packet["data"]
                        lines_total = int(packet["lines_total"])
                        acq_total_s = float(packet["acq_total_s"])
                        lines_consumed = max(1, lines_total - reader_lines_total_last)
                        acq_elapsed_s = max(1e-9, acq_total_s - reader_acq_total_last_s)
                        reader_lines_total_last = lines_total
                        reader_acq_total_last_s = acq_total_s
                    else:
                        acq_start = time.perf_counter()
                        if session is None:
                            data = pda.acquire_line(
                                timeout=acquisition_timeout_s,
                                read_reference=read_reference,
                            )
                            lines_consumed = 1
                        else:
                            data = session.read_line(timeout=acquisition_timeout_s)
                            lines_consumed = int(getattr(session, "last_lines_consumed", 1))
                        acq_elapsed_s = max(1e-9, time.perf_counter() - acq_start)
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
                        mode_badge_label = "SAFE-FALLBACK"
                        mode_badge_color = "#f7d9d9"
                        mode_text.set_text(mode_badge_label)
                        mode_text.set_bbox(
                            {"facecolor": mode_badge_color, "alpha": 0.85, "edgecolor": "none"}
                        )
                        parity_reset_requested = True
                        parity_reset_reason = "fast-session fallback/possible line loss"
                        continue
                    raise
                lines_consumed = max(1, lines_consumed)
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
                if pfi9_monitor is not None and getattr(pfi9_monitor, "available", False):
                    pfi9_rate_hz, pfi9_count = pfi9_monitor.read_rate()
                    if np.isfinite(pfi9_rate_hz):
                        pfi9_rate_hz_buffer.append(pfi9_rate_hz)
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
                    if chop_raw_line_plot is not None:
                        chop_raw_line_plot.set_xdata(x)
                    if chop_integrated_line_plot is not None:
                        chop_integrated_line_plot.set_xdata(x)
                    ax.set_xlim(0, max(1, line.size - 1))
                    if ax_demod is not None:
                        ax_demod.set_xlim(0, max(1, line.size - 1))
                    line_buffer.clear()
                    integration_sum = np.zeros_like(x, dtype=float)
                    ref_line_buffer.clear()
                    ref_integration_sum = np.zeros_like(x, dtype=float)
                    chop_prev_line = None
                    chop_phase = 0
                    chop_phase0_buffer.clear()
                    chop_phase1_buffer.clear()
                    chop_phase0_sum = np.zeros_like(x, dtype=float)
                    chop_phase1_sum = np.zeros_like(x, dtype=float)
                    chop_pair_buffer.clear()
                    chop_pair_sum = np.zeros_like(x, dtype=float)
                    chop_pair_counter = 0
                    chop_parity_reset_counter = 0
                    latest_chop_pair = None
                    latest_chop_integrated = None
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
                        chop_pair_buffer.clear()
                        chop_pair_sum = np.zeros_like(x, dtype=float)
                        latest_chop_pair = None
                        latest_chop_integrated = None
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
                            chop_prev_line = line.copy()
                            chop_prev_phase = current_phase
                            latest_chop_pair = None
                            latest_chop_integrated = None
                        else:
                            make_pair = (
                                (chop_prev_phase is None)
                                or (current_phase != chop_prev_phase)
                            )
                            if make_pair:
                                chop_pair = pump_chop_sign * (line - chop_prev_line)
                                chop_pair_counter += 1
                                latest_chop_pair = chop_pair
                                if len(chop_pair_buffer) == chop_pair_buffer.maxlen:
                                    chop_pair_sum -= chop_pair_buffer.popleft()
                                chop_pair_buffer.append(chop_pair.copy())
                                chop_pair_sum += chop_pair
                                latest_chop_integrated = chop_pair_sum / float(
                                    len(chop_pair_buffer)
                                )
                            else:
                                latest_chop_pair = None
                            chop_prev_line = line.copy()
                            chop_prev_phase = current_phase
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

                        if current_phase == 0:
                            if len(chop_phase0_buffer) == chop_phase0_buffer.maxlen:
                                chop_phase0_sum -= chop_phase0_buffer.popleft()
                            chop_phase0_buffer.append(line.copy())
                            chop_phase0_sum += line
                        else:
                            if len(chop_phase1_buffer) == chop_phase1_buffer.maxlen:
                                chop_phase1_sum -= chop_phase1_buffer.popleft()
                            chop_phase1_buffer.append(line.copy())
                            chop_phase1_sum += line

                        if chop_prev_line is None:
                            chop_prev_line = line.copy()
                            chop_prev_phase = current_phase
                            latest_chop_pair = None
                        else:
                            make_pair = (
                                (chop_prev_phase is None)
                                or (current_phase != chop_prev_phase)
                            )
                            if make_pair:
                                chop_pair = pump_chop_sign * (line - chop_prev_line)
                                chop_pair_counter += 1
                                latest_chop_pair = chop_pair
                            else:
                                latest_chop_pair = None
                            chop_prev_line = line.copy()
                            chop_prev_phase = current_phase

                        if len(chop_phase0_buffer) > 0 and len(chop_phase1_buffer) > 0:
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
                if chop_raw_line_plot is not None and latest_chop_pair is not None:
                    chop_raw_line_plot.set_ydata(latest_chop_pair)
                if (
                    chop_integrated_line_plot is not None
                    and latest_chop_integrated is not None
                ):
                    chop_integrated_line_plot.set_ydata(latest_chop_integrated)

                ax.set_title(
                    "CMOS Video (Simple Mode) "
                    f"| mode={mode_key} "
                    f"| st_delay={pda.st_initial_delay * 1e6:.1f} us "
                    f"| clk_delay={pda.clk_initial_delay * 1e6:.1f} us "
                    f"| integrated N={len(line_buffer)} "
                    f"| svc={read_service_rate_hz:.1f} Hz "
                    f"| wall={wall_line_rate_hz:.1f} Hz "
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
                    + (
                        f" | chop_pairs={chop_pair_counter}"
                        if pump_chop_demod
                        else ""
                    )
                    + (
                        f" | hit={capture_hit_rate_pct:.1f}%"
                        if capture_hit_rate_enable and np.isfinite(capture_hit_rate_pct)
                        else ""
                    )
                )
                if line_counter == 1 or (
                    line_counter % timing_text_update_every_n_lines == 0
                ):
                    d = pda.get_timing_diagnostics(
                        trigger_frequency_hz=expected_trigger_hz
                    )
                    if pfi9_monitor is None:
                        pfi9_status_line = "PFI9 monitor: disabled"
                    elif getattr(pfi9_monitor, "available", False):
                        pfi9_status_line = (
                            "PFI9 monitor: active "
                            f"({pfi9_monitor.counter}, gate={pfi9_monitor.rate_gate_s * 1e3:.1f} ms)"
                        )
                    else:
                        pfi9_err = str(getattr(pfi9_monitor, "error_text", "")).strip()
                        if len(pfi9_err) > 90:
                            pfi9_err = pfi9_err[:87] + "..."
                        pfi9_status_line = (
                            "PFI9 monitor: unavailable"
                            + (f" ({pfi9_err})" if pfi9_err else "")
                        )
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
                            f"AUC ref/diff={integrated_auc_ref:.6g}/"
                            f"{integrated_auc_diff:.6g} V*s"
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
                        pfi9_line = f"PFI9 med={pfi9_med:.1f} Hz"
                        if expected_trigger_hz > 0:
                            pfi9_line += (
                                f" ({pfi9_med / expected_trigger_hz:.2f}x)"
                            )
                        timing_lines.append(pfi9_line)
                    if np.isfinite(queue_med):
                        timing_lines.append(
                            f"Queue med/max={queue_med:.1f}/{max_pending_lines:.1f}"
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
                        chop_line = f"Chop {demod_mode_label}"
                        if pump_chop_use_adjacent_pairs:
                            chop_line += f" pairs={len(chop_pair_buffer)}"
                        else:
                            chop_line += (
                                f" ph0/ph1={len(chop_phase0_buffer)}/"
                                f"{len(chop_phase1_buffer)}"
                            )
                        chop_line += f" resets={chop_parity_reset_counter}"
                        timing_lines.append(chop_line)
                        if latest_chop_integrated is not None:
                            chop_rms = float(np.std(latest_chop_integrated))
                            chop_pp = float(np.ptp(latest_chop_integrated))
                            timing_lines.append(
                                f"Chop RMS/PP={chop_rms:.4g}/{chop_pp:.4g} V"
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
                    if ax_demod is not None:
                        ax_demod.relim(visible_only=True)
                        ax_demod.autoscale_view(scalex=False, scaley=True)
                    if ax_trig is not None and trig_rate_plot is not None and pfi9_rate_hz_buffer:
                        ax_trig.relim(visible_only=True)
                        ax_trig.autoscale_view(scalex=False, scaley=True)
                fig.canvas.draw_idle()
                plt.pause(0.001)

    except KeyboardInterrupt:
        print("\nStopped continuous acquisition.")
    finally:
        if reader is not None:
            try:
                reader.close()
            except Exception:
                pass
        pda.video_main = original_video_main
        plt.ioff()
        plt.show()


if __name__ == "__main__":
    cli = argparse.ArgumentParser(
        description="Simple live PDA/CMOS DAQ runner."
    )
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
    cli_args = cli.parse_args()
    if cli_args.list_timing_profiles:
        print("Available timing profiles:")
        for _name in sorted(PDAControllerDAQSimple.TIMING_PROFILES.keys()):
            _desc = PDAControllerDAQSimple.TIMING_PROFILES[_name].get("description", "")
            print(f"  {_name}: {_desc}")
        raise SystemExit(0)

    # -----------------------------
    # Simple runtime config
    # -----------------------------
    pda = PDAControllerDAQSimple(device="Dev1", num_pixels=1024)

    # Toggle this to compare free-run vs external trigger gated operation.
    pda.enable_external_trigger(True)
    # External trigger digital filtering (helps reject PFI9 glitches/ringing).
    # Start disabled. If needed, enable with a small width such as 0.1-0.5 us.
    trigger_filter_enable = False
    trigger_filter_min_pulse_width_s = 0.2e-6
    trigger_sync_enable = True
    pda.set_trigger_filter(
        enable=trigger_filter_enable,
        min_pulse_width_s=trigger_filter_min_pulse_width_s,
    )
    pda.set_trigger_sync(trigger_sync_enable)
    # Critical for retriggered counter timing: re-apply initial_delay on every
    # retriggered pulse train, not just the first trigger.
    pda.set_retrigger_initial_delay(True)

    # Video timing control:
    # - start_on_st_fall=True: begin sampling at first CLK rising edge after ST low.
    # - dummy_clocks=14: retain 14 pre-pixel clocks before the 1024 valid pixels.
    # This sets total acquired clocks to 14 + 1024 = 1038.
    video_dummy_clocks = 14
    video_start_on_st_fall = True
    pda.set_video_timing(
        dummy_clocks=video_dummy_clocks,
        pixel_clocks=pda.num_pixels,
        start_on_st_fall=video_start_on_st_fall,
    )
    # Keep uncropped output so timing/debug view shows the dummy-clock region.
    pda.set_output_cropping(False)

    # Live plot settings
    # Timing profile (can also be passed from CLI via --timing-profile).
    # Available: main_1khz_safe, txt_1khz_rebased, initial_guess, improved_guess,
    #            improved_guess_2channel, toofast_guess
    timing_profile_name = cli_args.timing_profile
    pda.apply_timing_profile(timing_profile_name)
    print(f"Timing profile: {timing_profile_name}")
    # Deterministic phase shift from trigger edge to ST rise (shifts ST+CLK together).
    # Default phase shift.
    trigger_to_st_phase_shift_us = 0.0
    pda.set_trigger_phase_shift(trigger_to_st_phase_shift_us * 1e-6)
    print(f"Trigger->ST phase shift: {trigger_to_st_phase_shift_us:.1f} us")

    integration_line_count = 128
    plot_raw_line = True
    # "main", "ref", or "both"
    live_video_mode = "main"
    # Alternating-line demod for 500 Hz pump chopper on a 1000 Hz trigger.
    pump_chop_demod = True
    # If the sign is inverted, set to -1.0.
    pump_chop_sign = 1.0
    # Troubleshooting toggle:
    # False -> robust phase-bucket demod (phase1-phase0),
    # True  -> original adjacent odd-even pair demod (line[n]-line[n-1]).
    pump_chop_use_adjacent_pairs = True
    # In safe mode, only accept demod lines with exactly one PFI9 edge between
    # consecutive accepted reads. Helps avoid parity poisoning from missed/extra
    # trigger edges.
    demod_trigger_qualified_acceptance = True
    expected_trigger_hz = 1000.0
    acquisition_timeout_s = 50.0
    persistent_ai_buffer_lines = 256
    # Runtime profile:
    # - "persistent_latest_test": arm once, read latest line, drop backlog.
    # - "persistent_robust_test": arm once, decoupled sequential reads, no overwrite.
    # - "safe": rebuild/start tasks per line.
    acquisition_runtime_profile = "persistent_robust_test"
    # Stable retriggered architecture:
    # 1) optionally log all acquired lines to TDMS (LOG_AND_READ),
    # 2) live plot reads latest complete line only, so GUI lag does not
    #    chase backlog.
    tdms_log_enable = False
    tdms_log_dir = Path(__file__).resolve().parent / "acquisition_results"
    tdms_log_dir.mkdir(parents=True, exist_ok=True)
    tdms_file_path = str(
        tdms_log_dir / f"pda_retrigger_{time.strftime('%Y%m%d_%H%M%S')}.tdms"
    )
    tdms_group_name = "PDA"
    tdms_logging_mode = LoggingMode.LOG_AND_READ
    tdms_logging_operation = LoggingOperation.OPEN_OR_CREATE

    if acquisition_runtime_profile == "persistent_latest_test":
        # Persistent retriggered + latest-only read path.
        use_persistent_session = True
        retrigger_latest_only_read = True
        retrigger_overwrite_unread = True
        decouple_acquisition_from_plot = True
        # Trigger-qualified acceptance is exact in safe mode only; disable it
        # here to avoid ambiguity in persistent mode.
        demod_trigger_qualified_acceptance = False
        # Conservative UI update pacing so plotting does not dominate runtime.
        plot_update_every_n_lines = 25
        plot_target_fps = 10.0
        timing_text_update_every_n_lines = 200
        autoscale_every_n_plot_updates = 12
        persistent_ai_buffer_lines = 256
    elif acquisition_runtime_profile == "persistent_robust_test":
        # Persistent retriggered + decoupled acquisition, but keep line integrity:
        # - read sequentially (no latest-only slicing)
        # - never overwrite unread samples (fallback to safe on overflow)
        use_persistent_session = True
        retrigger_latest_only_read = False
        retrigger_overwrite_unread = False
        decouple_acquisition_from_plot = True
        demod_trigger_qualified_acceptance = False
        # Default live cadence tuned for responsive plotting.
        plot_update_every_n_lines = 2
        plot_target_fps = 25.0
        timing_text_update_every_n_lines = 40
        autoscale_every_n_plot_updates = 8
        persistent_ai_buffer_lines = 256
    elif acquisition_runtime_profile == "safe":
        # Per-line task build/start.
        use_persistent_session = False
        retrigger_latest_only_read = False
        retrigger_overwrite_unread = False
        decouple_acquisition_from_plot = False
        demod_trigger_qualified_acceptance = True
        plot_update_every_n_lines = 1
        plot_target_fps = 15.0
        timing_text_update_every_n_lines = 100
        autoscale_every_n_plot_updates = 8
        persistent_ai_buffer_lines = 256
    else:
        raise ValueError(
            "Unknown acquisition_runtime_profile. "
            "Use 'persistent_latest_test', 'persistent_robust_test', or 'safe'."
        )

    # Optional live plotting cadence overrides.
    if cli_args.plot_fps is not None:
        plot_target_fps = max(1.0, float(cli_args.plot_fps))
    if cli_args.plot_every_lines is not None:
        plot_update_every_n_lines = max(1, int(cli_args.plot_every_lines))
    if cli_args.timing_text_every_lines is not None:
        timing_text_update_every_n_lines = max(1, int(cli_args.timing_text_every_lines))
    if cli_args.autoscale_every_updates is not None:
        autoscale_every_n_plot_updates = max(1, int(cli_args.autoscale_every_updates))

    monitor_pfi9 = True
    pfi9_monitor_counter = "ctr2"
    # Robust trigger-rate estimate gate. Larger gate = smoother/slower response.
    pfi9_rate_gate_s = 0.002
    trigger_plot_history = 1200
    # Lightweight capture hit-rate diagnostic:
    # score = Vpp(main line), threshold adapts from recent score distribution.
    capture_hit_rate_enable = True
    capture_hit_rate_window_lines = 256
    capture_hit_threshold_fraction = 0.45
    capture_hit_warmup_lines = 64

    if cli_args.operation == "sweep":
        print("\n=== Persistent Sweep Mode ===")
        print(
            "Sweep enforces robust persistent settings: "
            "external trigger on, main-only, demod off, "
            "latest_only=False, overwrite_unread=False."
        )
        live_video_mode = "main"
        pump_chop_demod = False
        demod_trigger_qualified_acceptance = False
        use_persistent_session = True
        retrigger_latest_only_read = False
        retrigger_overwrite_unread = False
        decouple_acquisition_from_plot = True
        tdms_log_enable = False

        run_persistent_timing_sweep(
            pda=pda,
            expected_trigger_hz=expected_trigger_hz,
            evaluation_seconds=cli_args.sweep_evaluation_seconds,
            acquisition_timeout_s=acquisition_timeout_s,
            monitor_pfi9=monitor_pfi9,
            pfi9_monitor_counter=pfi9_monitor_counter,
            pfi9_rate_gate_s=pfi9_rate_gate_s,
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

    run_live_plot(
        pda=pda,
        integration_line_count=integration_line_count,
        plot_raw_line=plot_raw_line,
        live_video_mode=live_video_mode,
        pump_chop_demod=pump_chop_demod,
        pump_chop_sign=pump_chop_sign,
        pump_chop_use_adjacent_pairs=pump_chop_use_adjacent_pairs,
        demod_trigger_qualified_acceptance=demod_trigger_qualified_acceptance,
        expected_trigger_hz=expected_trigger_hz,
        acquisition_timeout_s=acquisition_timeout_s,
        use_persistent_session=use_persistent_session,
        plot_update_every_n_lines=plot_update_every_n_lines,
        plot_target_fps=plot_target_fps,
        timing_text_update_every_n_lines=timing_text_update_every_n_lines,
        autoscale_every_n_plot_updates=autoscale_every_n_plot_updates,
        monitor_pfi9=monitor_pfi9,
        pfi9_monitor_counter=pfi9_monitor_counter,
        pfi9_rate_gate_s=pfi9_rate_gate_s,
        trigger_plot_history=trigger_plot_history,
        retrigger_latest_only_read=retrigger_latest_only_read,
        retrigger_overwrite_unread=retrigger_overwrite_unread,
        tdms_log_enable=tdms_log_enable,
        tdms_file_path=tdms_file_path,
        tdms_group_name=tdms_group_name,
        tdms_logging_mode=tdms_logging_mode,
        tdms_logging_operation=tdms_logging_operation,
        decouple_acquisition_from_plot=decouple_acquisition_from_plot,
        persistent_ai_buffer_lines=persistent_ai_buffer_lines,
        capture_hit_rate_enable=capture_hit_rate_enable,
        capture_hit_rate_window_lines=capture_hit_rate_window_lines,
        capture_hit_threshold_fraction=capture_hit_threshold_fraction,
        capture_hit_warmup_lines=capture_hit_warmup_lines,
    )
