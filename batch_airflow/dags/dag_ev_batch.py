from __future__ import annotations

from pathlib import Path
import pandas as pd
import pendulum

from airflow.decorators import dag, task


PROJECT_DIR = Path(__file__).resolve().parents[2]  # ev-charging-sessions/
AIRFLOW_HOME_DIR = Path(__file__).resolve().parents[1]  # batch_airflow/

RAW_DIR = PROJECT_DIR / "raw"
OUT_DIR = AIRFLOW_HOME_DIR / "batch_output"
TMP_DIR = AIRFLOW_HOME_DIR / "_tmp"


@dag(
    dag_id="ev_batch_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"owner": "durga", "retries": 1},
    tags=["ev", "batch"],
)
def ev_batch_pipeline():
    @task
    def read_data() -> str:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        TMP_DIR.mkdir(parents=True, exist_ok=True)

        files = list(RAW_DIR.glob("*.csv")) + list(RAW_DIR.glob("*.json")) + list(RAW_DIR.glob("*.jsonl"))
        if not files:
            raise FileNotFoundError(f"No raw files found in {RAW_DIR}")

        src = max(files, key=lambda p: p.stat().st_mtime)

        if src.suffix == ".csv":
            df = pd.read_csv(src)
        elif src.suffix == ".jsonl":
            df = pd.read_json(src, lines=True)
        else:
            df = pd.read_json(src)

        path = TMP_DIR / "raw.parquet"
        df.to_parquet(path, index=False)
        return str(path)

    @task
    def clean_data(raw_path: str) -> str:
        df = pd.read_parquet(raw_path)

        df.columns = [c.strip().lower() for c in df.columns]

        rename = {
            "timestamp": "event_time",
            "time": "event_time",
            "stationid": "station_id",
            "sessionid": "session_id",
            "energy": "energy_kwh",
            "duration": "duration_min",
            "price": "price_eur",
            "type": "event_type",
        }
        df = df.rename(columns=rename)

        required = ["event_time", "session_id", "station_id", "city"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing column {col}")

        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
        df["energy_kwh"] = pd.to_numeric(df.get("energy_kwh"), errors="coerce")
        df["duration_min"] = pd.to_numeric(df.get("duration_min"), errors="coerce")
        df["price_eur"] = pd.to_numeric(df.get("price_eur"), errors="coerce")

        df = df.dropna(subset=["event_time", "session_id", "station_id", "city"])
        df["event_date"] = df["event_time"].dt.date.astype(str)

        path = TMP_DIR / "clean.parquet"
        df.to_parquet(path, index=False)
        return str(path)

    @task
    def aggregate_data(clean_path: str) -> str:
        df = pd.read_parquet(clean_path)

        grouped = (
            df.groupby(["event_date", "city", "station_id"])
            .agg(
                unique_sessions=("session_id", "nunique"),
                avg_energy_kwh=("energy_kwh", "mean"),
                avg_duration_min=("duration_min", "mean"),
                total_revenue_eur=("price_eur", "sum"),
            )
            .reset_index()
        )

        path = TMP_DIR / "aggregated.csv"
        grouped.to_csv(path, index=False)
        return str(path)

    @task
    def save_output(agg_path: str) -> str:
        df = pd.read_csv(agg_path)
        out = OUT_DIR / "ev_daily_aggregates.csv"
        df.to_csv(out, index=False)
        return str(out)

    raw = read_data()
    clean = clean_data(raw)
    agg = aggregate_data(clean)
    save_output(agg)


ev_batch_pipeline()
