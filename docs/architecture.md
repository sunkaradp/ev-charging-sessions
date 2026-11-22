# Architecture

This page distinguishes the implemented local pipeline from broader cloud/analytics concepts described in project materials.

## Implemented repository flow

~~~mermaid
flowchart LR
  GEN[Synthetic event generator] -->|Kafka producer| K[(Kafka topic)]
  K --> C[Streaming consumer]
  C --> W[In-memory 60-second window]
  W --> S[Console summaries and simple alerts]
  RAW[CSV / JSON / JSONL files] --> DAG[Airflow DAG]
  DAG --> R[Read latest input file]
  R --> CL[Normalize and validate fields]
  CL --> AG[Aggregate by date, city and station]
  AG --> OUT[CSV daily aggregates]
~~~

## Implemented components

- data_gen/producer.py: generates synthetic charging-session events and publishes to Kafka; acknowledged messages are appended to a local JSONL file.
- streaming/consumer.py: consumes Kafka events and prints rolling-window summaries and simple threshold alerts.
- batch_airflow/dags/dag_ev_batch.py: orchestrates read, clean, aggregate and CSV output tasks.
- batch_airflow/transform.py: normalizes columns, coerces types, drops invalid event keys and aggregates by date, city and station.
- tests/ and .github/workflows/python-ci.yml: automated checks for producer configuration/event generation and batch transformations.

## Documented extensions

The repository materials also discuss Pub/Sub, BigQuery, BigQuery ML and dashboards. Treat those as target/design material unless a corresponding deployed integration, reproducible run and output artifact is available. This diagram intentionally does not present those extensions as verified runtime components.
