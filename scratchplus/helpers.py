"""Misc helpers — stubs to be filled in as phases add needs."""
import time


def now_ms() -> int:
    return int(time.monotonic() * 1000)
