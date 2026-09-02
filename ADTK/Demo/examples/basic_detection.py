from pathlib import Path

import matplotlib

# Select a non-interactive backend before importing pyplot so the example can
# save a chart without opening a desktop window.
matplotlib.use("Agg")

import numpy as np
import pandas as pd
from adtk.data import to_events, validate_series
from adtk.detector import InterQuartileRangeAD
from adtk.visualization import plot
from matplotlib import pyplot as plt


# ADTK 0.6.2 requests the old name for this bundled Matplotlib style.
if "seaborn-whitegrid" not in plt.style.library:
    plt.style.library["seaborn-whitegrid"] = plt.style.library[
        "seaborn-v0_8-whitegrid"
    ]


def build_sample_series() -> tuple[pd.Series, pd.DatetimeIndex]:
    """Create a regular hourly series with two known outliers."""
    # A fixed seed makes the generated data and detection result reproducible.
    random_generator = np.random.default_rng(seed=42)
    index = pd.date_range("2026-01-01", periods=24 * 14, freq="h")
    hours = np.arange(len(index))

    # Combine a daily cycle with small random noise to model a normal signal.
    values = (
        20
        + 2 * np.sin(2 * np.pi * hours / 24)
        + random_generator.normal(0, 0.35, len(index))
    )

    # Inject one high and one low outlier at known positions for verification.
    anomaly_positions = [24 * 4 + 6, 24 * 10 + 18]
    values[anomaly_positions] += [12, -12]

    series = pd.Series(values, index=index, name="metric")

    # ADTK expects an ordered DatetimeIndex with a valid, regular frequency.
    return validate_series(series), index[anomaly_positions]


def main() -> None:
    series, injected_anomalies = build_sample_series()

    # Fit IQR boundaries on the series and return one anomaly flag per point.
    detector = InterQuartileRangeAD(c=1.5)
    anomalies = detector.fit_detect(series)

    # Treat any undefined flags as normal before using the result as an index.
    detected_anomalies = anomalies.fillna(False).astype(bool)
    detected_times = series.index[detected_anomalies]

    # Fail loudly if the detector misses either deliberately injected outlier.
    missed_times = injected_anomalies.difference(detected_times)
    if not missed_times.empty:
        raise RuntimeError(f"Known anomalies were not detected: {list(missed_times)}")

    print(f"Detected {len(detected_times)} anomalous points:")
    for timestamp in detected_times:
        print(f"- {timestamp}: {series.loc[timestamp]:.2f}")

    # Convert point-wise flags into time intervals suitable for alert records.
    print(f"Anomaly events: {to_events(anomalies)}")

    # Keep generated files inside this demo, regardless of the terminal's CWD.
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "artifacts" / "basic_detection.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot(
        series,
        anomaly=anomalies,
        anomaly_color="red",
        anomaly_tag="marker",
    )
    plt.title("ADTK interquartile-range anomaly detection")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close("all")
    print(f"Chart saved to {output_path.resolve()}")


if __name__ == "__main__":
    main()
