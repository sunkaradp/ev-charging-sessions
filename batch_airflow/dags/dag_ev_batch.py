from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd
import pendulum

from airflow.decorators import dag, task


PROJECT_DIR = Path(__file__).resolve().parents[2]  # ev-charging-sessions/
sys.path.insert(0, str(PROJECT_DIR))
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
        from batch_airflow.transform import clean_dataframe

        df = clean_dataframe(pd.read_parquet(raw_path))
        path = TMP_DIR / "clean.parquet"
        df.to_parquet(path, index=False)
        return str(path)

    @task
    def aggregate_data(clean_path: str) -> str:
        from batch_airflow.transform import aggregate_dataframe

        df = aggregate_dataframe(pd.read_parquet(clean_path))
        path = TMP_DIR / "aggregated.csv"
        df.to_csv(path, index=False)
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
