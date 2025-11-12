# Airflow Quick Start

This folder holds the DAGs, logs, plugins, and env file for the local Airflow instance that runs via `docker-compose-airflow.yml`.

## Services

- Webserver: http://localhost:8080
- Scheduler: runs automatically when compose is up
- Backend DB: PostgreSQL container (`airflow-postgres`)

All services share the same Docker networks as Airbyte and MinIO (`weather-analytics-data-engg_airbyte-network`, `kind`).

## Default Credentials

The `airflow-init` service creates the admin user:

- Username: `admin`
- Password: `admin`

Log in the first time and change the password via **Profile → Edit**.

## Common Commands

From the project root:

```
docker compose -f docker-compose-airflow.yml up -d airflow-webserver airflow-scheduler
docker compose -f docker-compose-airflow.yml logs -f airflow-webserver
docker compose -f docker-compose-airflow.yml down
```

## Folder Layout

- `dags/` – place DAG Python files here
- `logs/` – populated by Airflow automatically
- `plugins/` – custom operators/sensors/hooks
- `.env` – stores `AIRFLOW_UID` so container permissions match the host user

## Networking Notes

- Airflow containers can reach Airbyte, MinIO, and dbt over the shared Docker networks.
- Use service names (e.g., `weather-dbt`, `airbyte-minio-server`) when configuring connections inside DAGs.


