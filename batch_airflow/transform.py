"""Pure dataframe transformations used by the EV batch pipeline."""

import pandas as pd

REQUIRED_COLUMNS = ("event_time", "session_id", "station_id", "city")
COLUMN_ALIASES = {
    "timestamp": "event_time", "time": "event_time",
    "stationid": "station_id", "sessionid": "session_id",
    "energy": "energy_kwh", "duration": "duration_min",
    "price": "price_eur", "type": "event_type",
}
NUMERIC_COLUMNS = ("energy_kwh", "duration_min", "price_eur")


def clean_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize source columns, coerce types, and discard invalid event keys."""
    if frame.empty:
        raise ValueError("Input data contains no rows")
    result = frame.copy()
    result.columns = [str(column).strip().lower() for column in result.columns]
    result = result.rename(columns=COLUMN_ALIASES)
    missing = [column for column in REQUIRED_COLUMNS if column not in result.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    result["event_time"] = pd.to_datetime(result["event_time"], errors="coerce", utc=True)
    for column in NUMERIC_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
        result[column] = pd.to_numeric(result[column], errors="coerce")

    result = result.dropna(subset=list(REQUIRED_COLUMNS)).copy()
    result["event_date"] = result["event_time"].dt.strftime("%Y-%m-%d")
    return result.reset_index(drop=True)


def aggregate_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cleaned events by date, city, and charging station."""
    required = ("event_date", "city", "station_id", "session_id", *NUMERIC_COLUMNS)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing aggregation columns: {', '.join(missing)}")
    return (
        frame.groupby(["event_date", "city", "station_id"], dropna=False)
        .agg(
            unique_sessions=("session_id", "nunique"),
            avg_energy_kwh=("energy_kwh", "mean"),
            avg_duration_min=("duration_min", "mean"),
            total_revenue_eur=("price_eur", "sum"),
        )
        .reset_index()
    )
