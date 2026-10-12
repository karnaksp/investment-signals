#!/usr/bin/env python3
"""Compatibility wrapper for the configured liquid-futures refresh."""

from tinvest_signal_engine.services.futures_universe_refresh import main


if __name__ == "__main__":
    raise SystemExit(main())
