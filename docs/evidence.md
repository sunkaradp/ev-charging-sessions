# Verification and evidence

## Automated checks in CI

The Python CI workflow uses Python 3.11 and performs:

1. Python bytecode compilation for batch_airflow, data_gen and tests.
2. pytest -q with the repository on PYTHONPATH.

The two test modules currently contain eight test functions:

| Test module | Test functions | Coverage focus |
|---|---:|---|
| tests/test_kafka_producer.py | 4 | Required configuration, whitespace normalization, unique session IDs, UTC timestamps and deterministic synthetic edge cases |
| tests/test_batch_transform.py | 4 | Column aliases/type coercion, required columns, missing optional metrics and grouped aggregation |

Source: .github/workflows/python-ci.yml.

## Implemented behavior visible in source

- Kafka producer configuration validation and reliable producer settings (acks=all, retries and idempotence).
- Local JSONL persistence after Kafka acknowledgement.
- Streaming consumer rolling-window summaries and simple rule-based alerts.
- Airflow batch DAG for file ingestion, cleaning, aggregation and CSV output.
- Pure transformation functions with unit tests.

## Metrics not currently evidenced

The README previously included throughput, end-to-end latency, batch duration and model-quality statements without a reproducible benchmark artifact in the repository. Those values are not treated as verified results here. No production throughput, latency, BigQuery ML accuracy or dashboard refresh guarantee is claimed.

## Reproduce the current tests

~~~bash
pip install -r requirements-test.txt
PYTHONPATH=. pytest -q
python -m compileall -q batch_airflow data_gen tests
~~~

## Next evidence to collect

1. Run the Kafka producer and consumer against a disposable local broker.
2. Record event counts, producer errors, consumer lag and processing timestamps.
3. Run the Airflow DAG against a versioned sample input and retain the output CSV.
4. Record input/output row counts and data-quality exclusions.
5. Add integration tests and sanitized query/model evaluation outputs before publishing cloud performance claims.
