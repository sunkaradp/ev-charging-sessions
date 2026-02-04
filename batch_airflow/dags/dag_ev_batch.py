from __future__ import annotations

from pathlib import Path
import pandas as pd
import pendulum

from airflow.decorators import dag, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "raw"
OUT_DIR = PROJECT_ROOT / "batch_output"
TMP_DIR = PROJECT_ROOT / "batch_airflow" / "_tmp"


@dag(
    dag_id="ev_batch_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "durga", "retries": 1},
    tags=["batch", "ev-charging"],
)
def ev_batch_dag():
    @task
    def read_data() -> str:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        TMP_DIR.mkdir(parents=True, exist_ok=True)

        candidates = list(RAW_DIR.glob("*.csv")) + list(RAW_DIR.glob("*.jsonl")) + list(RAW_DIR.glob("*.json"))
        if not candidates:
            raise FileNotFoundError(f"No input files found in: {RAW_DIR}")

        src = max(candidates, key=lambda p: p.stat().st_mtime)

        if src.suffix.lower() == ".csv":
            df = pd.read_csv(src)
        elif src.suffix.lower() == ".jsonl":
            df = pd.read_json(src, lines=True)
        elif src.suffix.lower() == ".json":
            df = pd.read_json(src)
        else:
            raise ValueError(f"Unsupported input format: {src.name}")

        tmp_path = TMP_DIR / "raw.parquet"
        df.to_parquet(tmp_path, index=False)
        return str(tmp_path)

    @task
    def clean_data(raw_path: str) -> str:
        df = pd.read_parquet(raw_path)

        rename_map = {}
        for c in df.columns:
            lc = c.strip().lower()
            if lc in {"event_time", "event_timestamp", "timestamp", "time"}:
                rename_map[c] = "event_time"
            elif lc in {"session_id", "sessionid"}:
                rename_map[c] = "session_id"
            elif lc in {"station_id", "stationid"}:
                rename_map[c] = "station_id"
            elif lc in {"city"}:
                rename_map[c] = "city"
            elif lc in {"energy_kwh", "energy", "kwh"}:
                rename_map[c] = "energy_kwh"
            elif lc in {"duration_min", "duration", "duration_minutes"}:
                rename_map[c] = "duration_min"
            elif lc in {"price_eur", "price", "eur", "amount"}:
                rename_map[c] = "price_eur"
            elif lc in {"event_type", "type"}:
                rename_map[c] = "event_type"

        if rename_map:
            df = df.rename(columns=rename_map)

        required = ["event_time", "session_id", "station_id", "city"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)

        for col in ["energy_kwh", "duration_min", "price_eur"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["city"] = df["city"].astype(str).str.strip()
        df["station_id"] = df["station_id"].astype(str).str.strip()
        df["session_id"] = df["session_id"].astype(str).str.strip()

        df = df.dropna(subset=["event_time", "session_id", "station_id", "city"])
        df = df[df["city"].str.len() > 0]
        df = df[df["station_id"].str.len() > 0]
        df = df[df["session_id"].str.len() > 0]

        if "duration_min" in df.columns:
            df = df[df["duration_min"].isna() | (df["duration_min"] >= 0)]
        if "energy_kwh" in df.columns:
            df = df[df["energy_kwh"].isna() | (df["energy_kwh"] >= 0)]
        if "price_eur" in df.columns:
            df = df[df["price_eur"].isna() | (df["price_eur"] >= 0)]

        if "event_type" in df.columns:
            df = df[df["event_type"].isin(["start", "update", "end"]) | df["event_type"].isna()]

        df["event_date"] = df["event_time"].dt.date.astype(str)

        tmp_path = TMP_DIR / "clean.parquet"
        df.to_parquet(tmp_path, index=False)
        return str(tmp_path)

    @task
    def aggregate_data(clean_path: str) -> str:
        df = pd.read_parquet(clean_path)

        group_cols = ["event_date", "city", "station_id"]

        agg_map = {
            "session_id": pd.Series.nunique,
        }

        if "energy_kwh" in df.columns:
            agg_map["energy_kwh"] = "mean"
        if "duration_min" in df.columns:
            agg_map["duration_min"] = "mean"
        if "price_eur" in df.columns:
            agg_map["price_eur"] = "sum"

        grouped = df.groupby(group_cols, dropna=False).agg(agg_map).reset_index()

        rename_out = {
            "session_id": "unique_sessions",
            "energy_kwh": "avg_energy_kwh",
            "duration_min": "avg_duration_min",
            "price_eur": "total_revenue_eur",
        }
        grouped = grouped.rename(columns={k: v for k, v in rename_out.items() if k in grouped.columns})

        tmp_path = TMP_DIR / "agg.csv"
        grouped.to_csv(tmp_path, index=False)
        return str(tmp_path)

    @task
    def save_output(agg_path: str) -> str:
        df = pd.read_csv(agg_path)
        out_path = OUT_DIR / "ev_batch_aggregates.csv"
        df.to_csv(out_path, index=False)
        return str(out_path)

    raw_path = read_data()
    clean_path = clean_data(raw_path)
    agg_path = aggregate_data(clean_path)
    save_output(agg_path)


ev_batch_dag()
