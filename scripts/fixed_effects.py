from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt


@dataclass(frozen=True)
class FEModelSpec:
    y_col: str = "mean_delay_s"
    station_col: str = "station_code"
    hour_col: str = "hour"
    weather_cols: Tuple[str, str, str] = ("precipitation", "temperature_2m", "wind_speed_10m")
    # Centering reference values (interpretation convenience)
    ref_precip_mm: float = 0.0
    ref_temp_c: float = 7.0
    ref_wind_kmh: float = 17.0
    # Drop early-night hours (optional; set to None to keep all)
    drop_hours_leq: Optional[int] = 2


@dataclass(frozen=True)
class FEOutputs:
    r2: float
    params: pd.Series
    cov: pd.DataFrame
    grand_mean: float
    station_fe: pd.DataFrame   # station_code, fe_seconds
    hour_fe: pd.DataFrame      # hour, fe_seconds


def _sum_to_zero_from_drop_first(
    params: pd.Series,
    baseline_station: str,
    baseline_hour: int,
    station_prefix: str = "station_",
    hour_prefix: str = "hour_",
) -> Tuple[float, pd.DataFrame, pd.DataFrame]:
    """
    Convert a drop-first dummy parameterization into a sum-to-zero representation.

    Returns:
      grand_mean (float), station_fe (df), hour_fe (df)
    """
    if "const" not in params.index:
        raise ValueError("Model params missing intercept 'const'.")

    # Stations
    station_terms = params.filter(like=station_prefix)
    station_map: Dict[str, float] = {k.replace(station_prefix, ""): float(v) for k, v in station_terms.items()}
    station_map[baseline_station] = 0.0

    avg_station = float(np.mean(list(station_map.values())))
    station_centered = {k: v - avg_station for k, v in station_map.items()}

    # Hours
    hour_terms = params.filter(like=hour_prefix)
    hour_map: Dict[int, float] = {int(k.replace(hour_prefix, "")): float(v) for k, v in hour_terms.items()}
    hour_map[int(baseline_hour)] = 0.0

    avg_hour = float(np.mean(list(hour_map.values())))
    hour_centered = {k: v - avg_hour for k, v in hour_map.items()}

    grand_mean = float(params["const"]) + avg_station + avg_hour

    station_df = (
        pd.DataFrame({"station_code": list(station_centered.keys()), "fe_seconds": list(station_centered.values())})
          .sort_values("fe_seconds", ascending=False)
          .reset_index(drop=True)
    )
    hour_df = (
        pd.DataFrame({"hour": list(hour_centered.keys()), "fe_seconds": list(hour_centered.values())})
          .sort_values("hour")
          .reset_index(drop=True)
    )
    return grand_mean, station_df, hour_df


def fit_station_hour_fe(df: pd.DataFrame, spec: FEModelSpec) -> FEOutputs:
    required = [spec.y_col, spec.station_col, spec.hour_col, *spec.weather_cols]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work = df[required].dropna().copy()

    # Normalize hour to int if needed
    work[spec.hour_col] = work[spec.hour_col].astype(int)

    if spec.drop_hours_leq is not None:
        work = work.loc[work[spec.hour_col] > int(spec.drop_hours_leq)].copy()

    # Center weather variables for interpretability
    precip, temp, wind = spec.weather_cols
    work["precip_centered"] = work[precip] - float(spec.ref_precip_mm)
    work["temp_centered"] = work[temp] - float(spec.ref_temp_c)
    work["wind_centered"] = work[wind] - float(spec.ref_wind_kmh)

    y = work[spec.y_col].astype(float)

    # Fixed effects dummies (drop-first)
    station_d = pd.get_dummies(work[spec.station_col], prefix="station", drop_first=True).astype(float)
    hour_d = pd.get_dummies(work[spec.hour_col], prefix="hour", drop_first=True).astype(float)

    baseline_station = sorted(work[spec.station_col].unique())[0]
    baseline_hour = sorted(work[spec.hour_col].unique())[0]

    x = pd.concat(
        [
            work[["precip_centered", "temp_centered", "wind_centered"]].astype(float),
            station_d,
            hour_d,
        ],
        axis=1,
    )
    x = sm.add_constant(x)

    model = sm.OLS(y, x)
    res = model.fit(cov_type="HC1")

    grand_mean, station_fe, hour_fe = _sum_to_zero_from_drop_first(
        res.params,
        baseline_station=baseline_station,
        baseline_hour=int(baseline_hour),
    )

    return FEOutputs(
        r2=float(res.rsquared),
        params=res.params,
        cov=res.cov_params(),
        grand_mean=grand_mean,
        station_fe=station_fe,
        hour_fe=hour_fe,
    )


def save_fe_outputs(out: FEOutputs, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Weather coefficients (centered)
    weather = pd.DataFrame(
        {
            "term": ["precip_centered", "temp_centered", "wind_centered"],
            "coef": [out.params.get("precip_centered", np.nan),
                     out.params.get("temp_centered", np.nan),
                     out.params.get("wind_centered", np.nan)],
        }
    )
    weather.to_csv(out_dir / "weather_coefficients.csv", index=False)

    out.station_fe.to_csv(out_dir / "station_fixed_effects.csv", index=False)
    out.hour_fe.to_csv(out_dir / "hour_fixed_effects.csv", index=False)

    summary = pd.DataFrame([{"r2": out.r2, "grand_mean_delay_s": out.grand_mean}])
    summary.to_csv(out_dir / "model_summary.csv", index=False)


def plot_fe_summaries(out: FEOutputs, out_dir: Path, top_n: int = 15) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Slowest stations
    slow = out.station_fe.head(int(top_n)).iloc[::-1]
    plt.figure(figsize=(9, 6))
    plt.barh(slow["station_code"], slow["fe_seconds"])
    plt.xlabel("Fixed effect (seconds, deviation from network mean)")
    plt.title(f"Top {top_n} stations with highest fixed effects")
    plt.tight_layout()
    plt.savefig(out_dir / "station_fe_top.png", dpi=200)
    plt.close()

    # Fastest stations
    fast = out.station_fe.tail(int(top_n)).iloc[::-1]
    plt.figure(figsize=(9, 6))
    plt.barh(fast["station_code"], fast["fe_seconds"])
    plt.xlabel("Fixed effect (seconds, deviation from network mean)")
    plt.title(f"Top {top_n} stations with lowest fixed effects")
    plt.tight_layout()
    plt.savefig(out_dir / "station_fe_bottom.png", dpi=200)
    plt.close()

    # Hour profile
    plt.figure(figsize=(9, 4))
    plt.plot(out.hour_fe["hour"], out.hour_fe["fe_seconds"], marker="o")
    plt.xlabel("Hour of day (Europe/Paris)")
    plt.ylabel("Fixed effect (seconds)")
    plt.title("Hourly fixed effects (deviation from network mean)")
    plt.tight_layout()
    plt.savefig(out_dir / "hour_fe_profile.png", dpi=200)
    plt.close()


def main() -> None:
    panel_path = Path("data/derived/hourly_panel.csv")
    if not panel_path.exists():
        raise FileNotFoundError(f"Hourly panel not found: {panel_path}")

    df = pd.read_csv(panel_path)
    spec = FEModelSpec()

    out = fit_station_hour_fe(df, spec)
    out_dir = Path("results/weather_fe")

    save_fe_outputs(out, out_dir)
    plot_fe_summaries(out, out_dir, top_n=15)


if __name__ == "__main__":
    main()
