from pathlib import Path

from tinvest_signal_engine.config import load_detector_config


ROOT = Path(__file__).resolve().parents[1]


def test_repository_detector_configuration_is_parseable() -> None:
    loaded = load_detector_config(
        ROOT / "conf" / "detectors.yaml",
        ROOT / "conf" / "detectors.overrides.yaml",
    )

    assert loaded.default.baseline_points >= loaded.default.min_baseline_points
    assert loaded.default.sample_every_seconds > 0
    assert all(key.strip() for key in loaded.per_instrument)
