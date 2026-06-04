"""Shared timing profile data structures and built-in profile definitions."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TimingProfile:
    """Timing profile for ST/CLK/video behavior."""

    name: str
    description: str
    st_high_time_s: float
    st_low_time_s: float
    st_initial_delay_s: float
    clk_high_time_s: float
    clk_low_time_s: float
    clk_initial_delay_s: float
    dummy_clocks: int = 14
    pixel_clocks: int | None = None
    start_on_st_fall: bool = True
    ai_sample_clock_divisor: int = 1


def _profile(
    name: str,
    description: str,
    st_high: float,
    st_low: float,
    st_delay: float,
    clk_high: float,
    clk_low: float,
    clk_delay: float,
    dummy_clocks: int = 14,
    pixel_clocks: int | None = None,
    start_on_st_fall: bool = True,
    ai_sample_clock_divisor: int = 1,
) -> TimingProfile:
    return TimingProfile(
        name=name,
        description=description,
        st_high_time_s=st_high,
        st_low_time_s=st_low,
        st_initial_delay_s=st_delay,
        clk_high_time_s=clk_high,
        clk_low_time_s=clk_low,
        clk_initial_delay_s=clk_delay,
        dummy_clocks=dummy_clocks,
        pixel_clocks=pixel_clocks,
        start_on_st_fall=start_on_st_fall,
        ai_sample_clock_divisor=ai_sample_clock_divisor,
    )


TIMING_PROFILES: dict[str, TimingProfile] = {
    "main_1khz_safe": _profile(
        "main_1khz_safe",
        "1 kHz-safe main-channel default with positive trigger-period margin.",
        4e-6,
        950e-6,
        0.0,
        250e-9,
        250e-9,
        0.0,
    ),
    "txt_1khz_rebased": _profile(
        "txt_1khz_rebased",
        "Rebased from legacy text configuration with ~474 us trigger-to-ST behavior.",
        50e-6,
        474e-6,
        0.0,
        250e-9,
        250e-9,
        0.0,
    ),
    "initial_guess": _profile(
        "initial_guess",
        "Initial first-pass timing with 2.0 MHz-compatible clocking.",
        50e-6,
        2.5e-3,
        0.0,
        250e-9,
        250e-9,
        0.0,
    ),
    "improved_guess": _profile(
        "improved_guess",
        "Fast profile tuned around 1 kHz operation.",
        4e-6,
        1000e-6,
        0.0,
        250e-9,
        250e-9,
        0.0,
    ),
    "improved_guess_2channel": _profile(
        "improved_guess_2channel",
        "Two-channel variant with a 1 MHz clock period.",
        4e-6,
        1000e-6,
        0.0,
        500e-9,
        500e-9,
        0.0,
    ),
    "toofast_guess": _profile(
        "toofast_guess",
        "Aggressive profile with tighter ST low-period margin.",
        4e-6,
        600e-6,
        0.0,
        250e-9,
        250e-9,
        0.0,
    ),
    "guard_10us": _profile(
        "guard_10us",
        "Near-1 kHz guard profile with about 10 us budget margin.",
        4e-6,
        986e-6,
        0.0,
        250e-9,
        250e-9,
        0.0,
    ),
    "short_line_single_channel": _profile(
        "short_line_single_channel",
        "Guard-derived short-line profile with 519 total samples.",
        4e-6,
        986e-6,
        0.0,
        250e-9,
        250e-9,
        0.0,
        dummy_clocks=14,
        pixel_clocks=505,
        start_on_st_fall=True,
    ),
    "decimated_2channel": _profile(
        "decimated_2channel",
        "Full-span two-channel profile: 2 MHz detector CLK, 1 MHz AI sample clock.",
        4e-6,
        986e-6,
        0.0,
        250e-9,
        250e-9,
        0.0,
        dummy_clocks=14,
        pixel_clocks=1024,
        start_on_st_fall=True,
        ai_sample_clock_divisor=2,
    ),
    "almost_full_2channel": _profile(
        "almost_full_2channel",
        "Near-full two-channel profile: 1 MHz CLK/AI and 972 valid pixel clocks.",
        4e-6,
        986e-6,
        0.0,
        500e-9,
        500e-9,
        0.0,
        dummy_clocks=14,
        pixel_clocks=972,
        start_on_st_fall=True,
        ai_sample_clock_divisor=1,
    ),
    "almost_full_single_channel": _profile(
        "almost_full_single_channel",
        "Single-channel diagnostic equivalent of almost_full_2channel.",
        4e-6,
        986e-6,
        0.0,
        500e-9,
        500e-9,
        0.0,
        dummy_clocks=14,
        pixel_clocks=972,
        start_on_st_fall=True,
        ai_sample_clock_divisor=1,
    ),
}

DEFAULT_TIMING_PROFILE_NAME = "guard_10us"


def get_timing_profile(name: str) -> TimingProfile:
    """Return a built-in timing profile by name."""
    return TIMING_PROFILES[str(name)]
