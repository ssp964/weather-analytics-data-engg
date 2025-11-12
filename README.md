# Weather Analytics Data Engineering

A data engineering pipeline for ingesting and transforming weather data using Airflow, Airbyte, MinIO, Snowflake, and dbt.

## Architecture

```
Airbyte → MinIO (S3) → Snowflake → dbt Transformations → Analytics
```

## Components

- **Airflow**: Orchestrates data ingestion pipeline (DAGs in `airflow/dags/`)
- **Airbyte**: Extracts weather data from APIs
- **MinIO**: S3-compatible storage for raw data
- **Snowflake**: Data warehouse
- **dbt**: Data transformations

### Airflow DAGs

- `weather_ingestion_pipeline`: Updates city metadata, drives Airbyte syncs, and writes raw files to MinIO.
- `minio_to_snowflake_loader`: Polls MinIO on a short interval and streams newly written JSON files into `WEATHERANALYTICS.WEATHER_ANALYTICS.raw_history_json` without waiting for the main ingestion DAG to finish.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Snowflake account
- OpenWeather API key

### Setup

1. **Configure Environment Variables**

   ```bash
   cp airflow/.env.example airflow/.env
   # Edit airflow/.env with your credentials
   ```

2. **Start Services**

   ```bash
   # Start MinIO
   docker compose -f docker-compose-minio.yml up -d

   # Start Airflow
   docker compose -f docker-compose-airflow.yml up -d

   # Start dbt (optional)
   docker compose -f docker-compose-dbt.yml up -d
   ```

3. **Access Services**

   - Airflow UI: http://localhost:8080 (admin/admin)
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

## Project Structure

```
├── airflow/          # Airflow DAGs and configuration
├── dbt/              # dbt models and transformations
├── sql/              # SQL scripts
├── docker-compose-*.yml  # Docker Compose configurations
└── raw_data/         # Raw data storage (MinIO)
```

## Documentation

- [Airflow Setup](airflow/README.md)
- [dbt Setup](dbt/README.md)
- [MinIO Setup](MINIO_SETUP.md)

## Configuration

All credentials are stored in `airflow/.env` (not committed to git). See `airflow/.env.example` for required variables.
