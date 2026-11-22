import pandas as pd
import pytest

from batch_airflow.transform import aggregate_dataframe, clean_dataframe


def test_clean_dataframe_normalizes_aliases_and_coerces_types():
    source = pd.DataFrame({
        " Timestamp ": ["2026-02-01T10:00:00Z", "not-a-date"],
        "SessionID": ["s-1", "s-2"], "StationID": ["st-1", "st-2"],
        "City": ["Hamburg", "Berlin"], "Energy": ["12.5", "bad"],
        "Duration": ["30", "20"], "Price": ["4.5", "3.0"],
    })
    result = clean_dataframe(source)
    assert len(result) == 1
    assert result.loc[0, "event_date"] == "2026-02-01"
    assert result.loc[0, "energy_kwh"] == 12.5


def test_clean_dataframe_rejects_missing_required_columns():
    with pytest.raises(ValueError, match="Missing required columns: city"):
        clean_dataframe(pd.DataFrame({"event_time": ["2026-01-01"], "session_id": ["s"], "station_id": ["st"]}))


def test_clean_dataframe_handles_missing_optional_metrics():
    result = clean_dataframe(pd.DataFrame({
        "event_time": ["2026-01-01"], "session_id": ["s"],
        "station_id": ["st"], "city": ["Hamburg"],
    }))
    assert pd.isna(result.loc[0, "energy_kwh"])
    assert pd.isna(result.loc[0, "duration_min"])
    assert pd.isna(result.loc[0, "price_eur"])


def test_aggregate_dataframe_calculates_session_and_metric_summaries():
    frame = pd.DataFrame({
        "event_date": ["2026-01-01", "2026-01-01"], "city": ["Hamburg", "Hamburg"],
        "station_id": ["st-1", "st-1"], "session_id": ["s-1", "s-1"],
        "energy_kwh": [10.0, 20.0], "duration_min": [20.0, 40.0],
        "price_eur": [3.0, 5.0],
    })
    result = aggregate_dataframe(frame)
    assert result.loc[0, "unique_sessions"] == 1
    assert result.loc[0, "avg_energy_kwh"] == 15.0
    assert result.loc[0, "avg_duration_min"] == 30.0
    assert result.loc[0, "total_revenue_eur"] == 8.0
