EV Charging Analytics Platform
Project Overview

This project implements a complete end-to-end data engineering and analytics pipeline for EV charging session data. The objective was to design a scalable system that handles both real-time streaming and batch processing, and integrates machine learning for revenue prediction.

The platform simulates EV charging session events, processes them through streaming and orchestration layers, stores structured data in Google BigQuery, and trains a regression model to forecast total revenue. The system combines local infrastructure tools with cloud-based services to reflect a real-world data engineering architecture.

Problem Statement

EV charging networks generate continuous session data including energy consumption, charging duration, station usage, and pricing. To support operational decision-making, businesses require:

Real-time ingestion of charging events

Daily aggregated analytics

Revenue forecasting

Centralized cloud-based storage

System monitoring and reliability

This project addresses these requirements through an integrated streaming, batch, and machine learning pipeline.

Architecture Summary

The system is divided into four main layers:

1. Streaming Layer

Real-time EV charging events are simulated using Python. Each event contains:

Session ID

Station ID

City

Energy consumed

Charging duration

Price

For local testing, Apache Kafka was deployed using Docker. A producer publishes events to a Kafka topic, and a consumer validates message ingestion.

For cloud streaming, Google Pub/Sub was implemented. A publisher sends real-time events to a Pub/Sub topic, and a subscriber confirms successful message delivery. Authentication was handled using a service account and IAM roles.

2. Batch Processing Layer

Apache Airflow was used to orchestrate batch processing.

A DAG named ev_batch_pipeline was designed to:

Read raw session data

Clean and validate fields

Aggregate daily metrics

Output structured results

The aggregation computes:

Unique sessions

Average energy consumption

Average charging duration

Total revenue

This output becomes the analytical dataset used for reporting and model training.

3. Data Warehouse Layer

All processed data is stored in Google BigQuery under the dataset ev_analytics.

Structured tables include:

batch_daily_aggregates

ml_train_data

SQL transformations were used to create a clean training dataset from aggregated metrics.

BigQuery serves as the centralized analytical layer of the system.

4. Machine Learning Layer

A Multiple Linear Regression model was trained using BigQuery ML to predict total revenue.

Features used:

Unique sessions

Average energy consumption

Average charging duration

Target variable:

Total revenue

The regression model follows:

Y = β₀ + β₁X₁ + β₂X₂ + β₃X₃ + ε

Model performance was evaluated using:

Mean Absolute Error

Mean Squared Error

R² Score

Predictions were generated using ML.PREDICT and visualized in a unified dashboard.

Dashboard & Visualization

A dashboard was created to combine analytics and machine learning results into a single view. It displays:

Actual vs Predicted Revenue

Revenue trends

Session impact analysis

The dashboard connects directly to the BigQuery dataset and updates dynamically.

Monitoring

System reliability was ensured through monitoring at multiple levels:

Docker container health for Kafka

Airflow DAG execution logs

Pub/Sub message validation

BigQuery job history and query monitoring

This provided visibility across streaming, batch, and analytics layers.

Challenges Faced

During development, several technical challenges were encountered:

Pub/Sub authentication and IAM configuration

Docker container networking issues

Airflow scheduler initialization problems

Schema mismatches in BigQuery

Synchronization between streaming components

Each issue was resolved through configuration corrections, IAM role updates, and environment adjustments.

Conclusion

This project successfully demonstrates a complete cloud-based data engineering pipeline integrating:

Real-time streaming

Batch processing

Cloud data warehousing

Machine learning

Monitoring

The architecture is modular, scalable, and aligned with real-world EV charging analytics use cases. It showcases how streaming systems and machine learning can be integrated to enable revenue forecasting and data-driven decision-making.
<img width="1536" height="1024" alt="photo" src="https://github.com/user-attachments/assets/f17bdd5a-af2d-487a-8e53-010c13af4b43" />
